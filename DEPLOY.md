# Деплой Flipper на сервер

## Требования

- Linux VPS (Ubuntu 22.04+ / Debian 12+)
- Docker Engine 24+ и Docker Compose Plugin v2
- Git
- Минимум 2 GB RAM, 20 GB disk

---

## 1. Подготовка сервера

```bash
# Установить Docker (если ещё нет)
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Перелогиниться чтобы группа docker подхватилась
```

---

## 2. Клонирование и настройка

```bash
cd /opt  # или любой каталог
git clone <repo-url> flipper
cd flipper
```

### 2.1 Файл окружения

```bash
cp .env.example .env
nano .env
```

Обязательные переменные:

| Переменная | Описание |
|---|---|
| `FIRECRAWL_API_KEY` | API-ключ Firecrawl |
| `SPREADSHEET_ID` | ID Google Sheets документа |
| `TG_BOT_TOKEN` | Telegram бот токен |
| `TG_CHAT_ID` | ID чата для уведомлений |
| `POSTGRES_PASSWORD` | Пароль PostgreSQL (по умолчанию `flipper_secret`) |

Опциональные:

| Переменная | Описание |
|---|---|
| `PARSER_FIRECRAWL_BASE_URL` | URL Firecrawl если в той же Docker-сети |
| `NOVNC_PUBLIC_URL` | Публичный URL NoVNC для Telegram |
| `PARSER_CONCURRENCY` | Кол-во воркеров парсера (default: 20) |

### 2.2 Google Credentials

```bash
# Скопировать файл сервисного аккаунта Google
scp credentials.json user@server:/opt/flipper/credentials.json
```

### 2.3 Прокси (опционально)

```bash
mkdir -p data
# Формат: host:port:user:pass — по одному на строку
nano data/proxies.txt
```

---

## 3. Создание внешней сети Firecrawl

```bash
# Если Firecrawl запущен в отдельном compose и сеть ещё не создана:
docker network create firecrawl_backend 2>/dev/null || true
```

---

## 4. Сборка и запуск

```bash
# Собрать все образы
docker compose build

# Запустить инфраструктуру (postgres, redis, cookie_manager, html_to_markdown, scheduler)
docker compose up -d
```

Проверить что всё поднялось:

```bash
docker compose ps
```

Ожидаемый вывод — все сервисы `running (healthy)`:

```
app_postgres      running (healthy)
app_redis         running (healthy)
cookie_manager    running (healthy)
html_to_markdown  running (healthy)
flipper_scheduler running
```

> `parser_cian` и `category_counter` имеют `profiles: [manual]` — они **не** запускаются в `docker compose up`. Их запускает `scheduler` автоматически по расписанию.

---

## 5. Миграция данных из SQLite в PostgreSQL

> **Этот шаг выполняется один раз при переходе с SQLite.**
> Если деплоите с нуля (без старой БД) — пропустите этот раздел.

### 5.1 Копирование файла БД на сервер

```bash
# С локальной машины
scp data/parser_cian.db user@server:/opt/flipper/data/parser_cian.db
```

### 5.2 Убедиться что PostgreSQL запущен

```bash
docker compose up -d app_postgres
# Подождать healthcheck
docker compose exec app_postgres pg_isready -U flipper
```

### 5.3 Запуск миграции

```bash
docker compose run --rm parser_cian \
    python scripts/migrate_sqlite_to_postgres.py \
    --sqlite data/parser_cian.db \
    --pg "postgresql+asyncpg://flipper:${POSTGRES_PASSWORD:-flipper_secret}@app_postgres:5432/flipper"
```

Скрипт:
- Читает все таблицы из SQLite (`cian_filters`, `cian_active_ads`, `cian_sold_ads`)
- Создаёт таблицы в PostgreSQL (если не существуют)
- Вставляет данные с `ON CONFLICT DO NOTHING` (безопасен при повторном запуске)
- Сбрасывает sequences на `MAX(id)`

### 5.4 Проверка миграции

```bash
docker compose exec app_postgres psql -U flipper -c "
  SELECT 'filters', COUNT(*) FROM cian_filters
  UNION ALL SELECT 'active_ads', COUNT(*) FROM cian_active_ads
  UNION ALL SELECT 'sold_ads', COUNT(*) FROM cian_sold_ads;
"
```

### 5.5 Удаление старого файла (опционально)

```bash
# После успешной проверки
rm data/parser_cian.db
```

---

## 6. Расписание (автоматическое)

Scheduler уже запущен в шаге 4. Он работает по расписанию:

| Время (MSK) | Задача |
|---|---|
| 09:00 | `category_counter` |
| 10:00 | `parser_cian --mode avans`, затем `--mode offers` |
| 18:00 | `parser_cian --mode avans`, затем `--mode offers` |

Scheduler автоматически:
- Запускает контейнеры через `docker compose run --rm`
- Повторяет при ошибке (до 3 попыток с exponential backoff)
- Отправляет Telegram-алерты при сбоях
- Использует lock чтобы задачи не пересекались

---

## 7. Ручной запуск парсера

```bash
# Парсинг offers (поиск + парсинг)
docker compose run --rm parser_cian --mode offers

# Парсинг авансов
docker compose run --rm parser_cian --mode avans

# Только непропарсенные (без сбора ссылок)
docker compose run --rm parser_cian --mode offers --skip-links --unparsed-links

# Category counter
docker compose run --rm category_counter
```

---

## 8. Мониторинг

### Логи

```bash
# Все сервисы
docker compose logs -f

# Только scheduler
docker compose logs -f scheduler

# Последний запуск parser_cian
docker compose logs parser_cian

# PostgreSQL
docker compose logs app_postgres
```

### Состояние БД

```bash
docker compose exec app_postgres psql -U flipper -c "
  SELECT 'active_avans', COUNT(*) FROM cian_active_ads WHERE source='avans'
  UNION ALL SELECT 'active_offers', COUNT(*) FROM cian_active_ads WHERE source='offers'
  UNION ALL SELECT 'sold_total', COUNT(*) FROM cian_sold_ads;
"
```

### Telegram-уведомления

Бот отправляет 4 типа уведомлений:

1. **Signal** — объявление добавлено в Signals_Parser (снижение цены)
2. **Signal удалён** — объявление убрано из Signals (критерии больше не выполняются)
3. **Продано / Аванс внесён** — объявление снято с публикации
4. **Куки слетели** — требуется обновить cookies через NoVNC

---

## 9. Обновление

```bash
cd /opt/flipper
git pull

# Пересобрать и перезапустить
docker compose build
docker compose up -d

# Если изменились зависимости parser_cian/scheduler — пересобрать конкретный сервис:
docker compose build parser_cian scheduler
docker compose up -d scheduler
```

---

## 10. Бэкапы PostgreSQL

```bash
# Создать дамп
docker compose exec app_postgres pg_dump -U flipper flipper > backup_$(date +%Y%m%d_%H%M%S).sql

# Восстановить из дампа
cat backup_20260415.sql | docker compose exec -T app_postgres psql -U flipper flipper
```

Для автоматических бэкапов — добавить в crontab хоста:

```bash
# Каждый день в 3:00 — бэкап PostgreSQL
0 3 * * * cd /opt/flipper && docker compose exec -T app_postgres pg_dump -U flipper flipper | gzip > /opt/flipper/backups/flipper_$(date +\%Y\%m\%d).sql.gz
```

```bash
mkdir -p /opt/flipper/backups
```

---

## 11. Структура сервисов

```
┌─────────────────────────────────────────────────────┐
│  Docker Compose                                     │
│                                                     │
│  ┌──────────────┐   cron    ┌──────────────────┐   │
│  │  scheduler   │──────────▶│   parser_cian    │   │
│  │  (always on) │           │   (on-demand)    │   │
│  │              │──────┐    └────────┬─────────┘   │
│  └──────────────┘      │             │              │
│                        │             ▼              │
│                        │    ┌──────────────────┐   │
│                        └───▶│ category_counter │   │
│                             │   (on-demand)    │   │
│                             └──────────────────┘   │
│                                                     │
│  ┌──────────────┐    ┌──────────────────┐          │
│  │ app_postgres │    │  cookie_manager  │          │
│  │ (PostgreSQL) │    │ (FastAPI+NoVNC)  │          │
│  └──────────────┘    └──────────────────┘          │
│                                                     │
│  ┌──────────────┐    ┌──────────────────┐          │
│  │  app_redis   │    │ html_to_markdown │          │
│  │   (cache)    │    │   (Go service)   │          │
│  └──────────────┘    └──────────────────┘          │
└─────────────────────────────────────────────────────┘
```

---

## Troubleshooting

### PostgreSQL не стартует

```bash
docker compose logs app_postgres
# Проверить что volume pgdata доступен
docker volume ls | grep pgdata
```

### parser_cian не может подключиться к PostgreSQL

```bash
# Проверить сеть
docker compose exec parser_cian ping -c 1 app_postgres

# Проверить DATABASE_URL
docker compose run --rm parser_cian env | grep DATABASE_URL
```

### Scheduler не запускает задачи

```bash
docker compose logs -f scheduler | grep -E "START|END|FAILED"

# Проверить что docker.sock примонтирован
docker compose exec flipper_scheduler docker ps
```

### Cookie error

1. Открыть NoVNC: `http://<server-ip>:8080/vnc.html`
2. Вручную пройти капчу / залогиниться на cian.ru
3. Cookies обновятся автоматически

---

## Порты

| Порт | Сервис |
|---|---|
| 5432 | PostgreSQL |
| 6379 | Redis |
| 8000 | Cookie Manager API |
| 8080 | NoVNC |
| 8090 | HTML to Markdown |
