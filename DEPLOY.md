# Деплой Flipper на сервер

## Требования

- Linux VPS (Ubuntu 22.04+ / Debian 12+), рекомендуется Timeweb Cloud
- Docker Engine 24+ и Docker Compose Plugin v2
- Git
- Минимум 4 GB RAM, 40 GB disk (Firecrawl + Flipper + PostgreSQL)

---

## 1. Подготовка сервера

```bash
# Установить Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Перелогиниться чтобы группа docker подхватилась
```

---

## 2. Flippercrawl (self-hosted Firecrawl)

Firecrawl деплоится **отдельным** docker-compose. Flipper подключается к нему через общую Docker-сеть `firecrawl_backend`.

### 2.1 Клонировать и настроить Firecrawl

```bash
cd /opt
git clone https://github.com/mendableai/firecrawl.git flippercrawl
cd flippercrawl
```

Скопировать и отредактировать `.env`:

```bash
cp apps/api/.env.example apps/api/.env
nano apps/api/.env
```

Ключевые переменные:

```env
# Можно оставить пустым для self-hosted без auth
FIRECRAWL_API_KEY=local

# LLM для AI-экстракции (используется parser_cian)
# OpenRouter / любой OpenAI-совместимый endpoint
OPENAI_API_KEY=sk-or-v1-...
OPENAI_BASE_URL=https://openrouter.ai/api/v1
MODEL_NAME=glm-4-9b-chat
```

### 2.2 Запустить Flippercrawl

```bash
cd /opt/flippercrawl
docker compose up -d
```

Проверить:

```bash
# API должен отвечать на порту 3002
curl http://localhost:3002/
```

### 2.3 Убедиться что сеть создана

Firecrawl автоматически создаёт сеть. Flipper подключается к ней как `external`. Проверить:

```bash
docker network ls | grep firecrawl_backend
```

Если сети нет (Firecrawl ещё не поднят или использует другое имя):

```bash
docker network create firecrawl_backend
```

> **Важно:** имя сети в `docker-compose.yml` Flipper — `firecrawl_backend`. Если в вашем Firecrawl она называется иначе, поменяйте в `docker-compose.yml` Flipper секцию `networks`.

---

## 3. Flipper — клонирование и настройка

```bash
cd /opt
git clone <repo-url> flipper
cd flipper
```

### 3.1 Файл окружения

```bash
cp .env.example .env
nano .env
```

Обязательные переменные:

| Переменная | Описание |
|---|---|
| `FIRECRAWL_API_KEY` | `local` (self-hosted без auth) |
| `FIRECRAWL_BASE_URL` | URL Firecrawl API (см. ниже) |
| `SPREADSHEET_ID` | ID Google Sheets документа |
| `TG_BOT_TOKEN` | Telegram бот токен |
| `TG_CHAT_ID` | ID чата для уведомлений |
| `POSTGRES_PASSWORD` | Пароль PostgreSQL |
| `NOVNC_PUBLIC_URL` | `http://<IP сервера>:8080/vnc.html` |

#### Firecrawl URL — какой указать?

В `docker-compose.yml` parser_cian подключён к сети `firecrawl_backend`, поэтому может обращаться к Firecrawl по имени контейнера:

```env
# Имя контейнера Firecrawl API (обычно <project>-api-1)
# Проверить: docker ps --format '{{.Names}}' | grep api
FIRECRAWL_BASE_URL=http://flippercrawl-api-1:3002
```

Если DNS по имени контейнера не работает, используется fallback через хост:

```env
# Fallback — через host.docker.internal (настроено в docker-compose.yml)
# Работает если Firecrawl публикует порт 3002 на хосте
FIRECRAWL_BASE_URL=http://host.docker.internal:3002
```

> Проверить имя контейнера: `docker ps --format '{{.Names}}' | grep api`

### 3.2 Google Credentials

```bash
# С локальной машины
scp credentials.json root@<server-ip>:/opt/flipper/credentials.json
```

### 3.3 Прокси (резидентские)

```bash
mkdir -p data
# Формат: host:port:user:pass — по одному на строку
nano data/proxies.txt
```

Или скопировать готовый файл:

```bash
scp data/proxies.txt root@<server-ip>:/opt/flipper/data/proxies.txt
```

---

## 4. Сборка и запуск Flipper

```bash
cd /opt/flipper

# Собрать все образы
docker compose build

# Запустить инфраструктуру
docker compose up -d
```

Проверить:

```bash
docker compose ps
```

Ожидаемый результат:

```
app_postgres      running (healthy)
app_redis         running (healthy)
cookie_manager    running (healthy)
html_to_markdown  running (healthy)
flipper_scheduler running
```

> `parser_cian` и `category_counter` имеют `profiles: [manual]` — они **не** запускаются в `docker compose up`. Их запускает `scheduler` автоматически по расписанию.

### Проверить связность с Firecrawl

```bash
docker compose run --rm parser_cian python -c "
import httpx, os
url = os.environ.get('FIRECRAWL_BASE_URL', 'http://host.docker.internal:3002')
r = httpx.get(url, timeout=5)
print(f'{url} -> {r.status_code}')
"
```

---

## 5. Миграция данных из SQLite в PostgreSQL

> **Выполняется один раз при переходе с SQLite.**
> Если деплоите с нуля — пропустите этот раздел.

### 5.1 Копирование файла БД на сервер

```bash
scp data/parser_cian.db root@<server-ip>:/opt/flipper/data/parser_cian.db
```

### 5.2 Убедиться что PostgreSQL запущен

```bash
docker compose up -d app_postgres
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

### 5.5 Удаление старого файла

```bash
rm data/parser_cian.db
```

---

## 6. Расписание (автоматическое)

Scheduler запущен в шаге 4. Расписание:

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
# Все сервисы Flipper
docker compose logs -f

# Scheduler
docker compose logs -f scheduler

# Последний запуск parser_cian
docker compose logs parser_cian

# Firecrawl (из другого каталога)
cd /opt/flippercrawl && docker compose logs -f api
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

### Flipper

```bash
cd /opt/flipper
git pull
docker compose build
docker compose up -d
```

### Flippercrawl

```bash
cd /opt/flippercrawl
git pull
docker compose up -d --build
```

---

## 10. Бэкапы PostgreSQL

```bash
# Создать дамп
docker compose exec app_postgres pg_dump -U flipper flipper > backup_$(date +%Y%m%d_%H%M%S).sql

# Восстановить из дампа
cat backup_20260415.sql | docker compose exec -T app_postgres psql -U flipper flipper
```

Автоматические бэкапы — crontab хоста:

```bash
mkdir -p /opt/flipper/backups

# Добавить в crontab -e:
# Каждый день в 3:00
0 3 * * * cd /opt/flipper && docker compose exec -T app_postgres pg_dump -U flipper flipper | gzip > /opt/flipper/backups/flipper_$(date +\%Y\%m\%d).sql.gz
```

---

## 11. Структура сервисов

```
┌────────────────────────────────────────────────────────────────────┐
│  /opt/flippercrawl (отдельный docker-compose)                     │
│                                                                    │
│  ┌─────────────┐  ┌───────────┐  ┌───────────┐  ┌─────────────┐  │
│  │ firecrawl   │  │ playwright│  │  redis     │  │  rabbitmq   │  │
│  │ API :3002   │  │  service  │  │           │  │             │  │
│  └──────┬──────┘  └───────────┘  └───────────┘  └─────────────┘  │
│         │ сеть: firecrawl_backend                                  │
└─────────┼──────────────────────────────────────────────────────────┘
          │
          │ docker network (firecrawl_backend)
          │
┌─────────┼──────────────────────────────────────────────────────────┐
│  /opt/flipper (docker-compose)                                     │
│         │                                                          │
│         ▼                                                          │
│  ┌──────────────┐   cron    ┌──────────────────┐                  │
│  │  scheduler   │──────────▶│   parser_cian    │──── Firecrawl    │
│  │  (always on) │           │   (on-demand)    │                  │
│  │              │──────┐    └────────┬─────────┘                  │
│  └──────────────┘      │             │                             │
│                        │             ▼                             │
│                        │    ┌──────────────────┐                  │
│                        └───▶│ category_counter │                  │
│                             │   (on-demand)    │                  │
│                             └──────────────────┘                  │
│                                                                    │
│  ┌──────────────┐    ┌──────────────────┐                         │
│  │ app_postgres │    │  cookie_manager  │                         │
│  │ (PostgreSQL) │    │ (FastAPI+NoVNC)  │                         │
│  └──────────────┘    └──────────────────┘                         │
│                                                                    │
│  ┌──────────────┐    ┌──────────────────┐                         │
│  │  app_redis   │    │ html_to_markdown │                         │
│  └──────────────┘    └──────────────────┘                         │
└────────────────────────────────────────────────────────────────────┘
```

---

## Troubleshooting

### Firecrawl не отвечает из parser_cian

```bash
# Проверить что Firecrawl запущен
cd /opt/flippercrawl && docker compose ps

# Проверить имя контейнера API
docker ps --format '{{.Names}}' | grep api

# Проверить что сеть firecrawl_backend существует и оба compose подключены
docker network inspect firecrawl_backend --format '{{range .Containers}}{{.Name}} {{end}}'

# Тест из контейнера parser_cian
docker compose run --rm parser_cian python -c "
import httpx, os
url = os.environ.get('FIRECRAWL_BASE_URL', 'http://host.docker.internal:3002')
print(httpx.get(url, timeout=5).status_code)
"
```

### PostgreSQL не стартует

```bash
docker compose logs app_postgres
docker volume ls | grep pgdata
```

### parser_cian не подключается к PostgreSQL

```bash
docker compose run --rm parser_cian env | grep DATABASE_URL
docker compose exec app_postgres pg_isready -U flipper
```

### Scheduler не запускает задачи

```bash
docker compose logs -f scheduler | grep -E "START|END|FAILED"
docker compose exec flipper_scheduler docker ps
```

### Cookie error

1. Открыть NoVNC: `http://<server-ip>:8080/vnc.html`
2. Вручную пройти капчу / залогиниться на cian.ru
3. Cookies обновятся автоматически

---

## Порты

| Порт | Сервис | Проект |
|---|---|---|
| 3002 | Firecrawl API | flippercrawl |
| 5432 | PostgreSQL | flipper |
| 6379 | Redis | flipper |
| 8000 | Cookie Manager API | flipper |
| 8080 | NoVNC | flipper |
| 8090 | HTML to Markdown | flipper |
