# Переход от тестирования к Production

## 🧪 Текущий этап: Разработка и тестирование

Вы успешно протестировали парсер локально! Теперь можно перейти к реальному использованию.

---

## 📋 Варианты запуска

### Вариант 1: Локальный запуск с реальными Google Sheets

**Когда использовать:** Для тестирования с реальными данными, но без Docker

**Команда:**
```bash
python test_queue_real.py
```

**Что произойдет:**
- ✅ Парсинг через Firecrawl API
- ✅ Реальная запись в Google Sheets (таб RESULTS)
- ✅ Детальные логи в консоли
- ✅ 2 параллельных воркера

**Требования:**
- Cookie Manager должен быть запущен: `docker-compose up -d cookie_manager`
- Файл `credentials.json` в корне проекта
- `SPREADSHEET_ID` в `.env`

---

### Вариант 2: Production запуск через Docker

**Когда использовать:** Для постоянной работы парсера в фоне

#### Шаг 1: Раскомментируйте сервис в docker-compose.yml

Откройте `docker-compose.yml` и раскомментируйте блок:

```yaml
  parser_cian:
    build:
      context: .  # Контекст - корень проекта
      dockerfile: ./services/parser_cian/Dockerfile
    container_name: parser_cian
    restart: unless-stopped
    env_file:
      - .env
    volumes:
      - ./credentials.json:/app/credentials.json
    depends_on:
      cookie_manager:
        condition: service_healthy
    networks:
      - flipper_network
```

#### Шаг 2: Соберите образ

```bash
docker-compose build parser_cian
```

#### Шаг 3: Запустите парсер

**Одноразовый запуск:**
```bash
docker-compose run --rm parser_cian python -m services.parser_cian.main
```

**Постоянная работа в фоне:**
```bash
docker-compose up -d parser_cian
```

#### Шаг 4: Мониторинг логов

```bash
# Следить за логами в реальном времени
docker-compose logs -f parser_cian

# Последние 100 строк
docker-compose logs --tail=100 parser_cian

# Только ошибки
docker-compose logs parser_cian 2>&1 | grep -E "(ERROR|❌)"
```

---

## 🎯 Рекомендуемый workflow

### Этап 1: Тестирование (сейчас)
```bash
# 1. Запустите Cookie Manager
docker-compose up -d cookie_manager

# 2. Проверьте куки
curl http://localhost:8000/cookies

# 3. Тестируйте парсинг
python test_parser.py          # Одно объявление
python test_queue.py           # Несколько объявлений (Mock)
python test_queue_real.py      # Несколько объявлений (Real Sheets)
```

### Этап 2: Переход к Production
```bash
# 1. Раскомментируйте parser_cian в docker-compose.yml

# 2. Соберите образ
docker-compose build parser_cian

# 3. Запустите все сервисы
docker-compose up -d

# 4. Проверьте статус
docker-compose ps

# 5. Следите за логами
docker-compose logs -f parser_cian
```

---

## ⚙️ Настройка производительности

### Количество воркеров

В `.env` файле:

```bash
# Консервативно (безопасно для rate limits)
PARSER_CONCURRENCY=2

# Средняя скорость
PARSER_CONCURRENCY=3

# Быстро (риск блокировки)
PARSER_CONCURRENCY=5
```

**Рекомендация:** Начните с 2, постепенно увеличивайте до 5, если нет проблем с rate limits.

### Уровень логирования

```bash
# Детальные логи (для отладки)
LOG_LEVEL=DEBUG

# Стандартные логи (рекомендуется)
LOG_LEVEL=INFO

# Только ошибки
LOG_LEVEL=ERROR
```

---

## 📊 Мониторинг работы

### Проверка здоровья сервисов

```bash
# Статус всех контейнеров
docker-compose ps

# Здоровье Cookie Manager
curl http://localhost:8000/health

# Статус кук
curl http://localhost:8000/status | jq
```

### Просмотр логов

```bash
# Все логи парсера
docker-compose logs parser_cian

# Последние 50 строк
docker-compose logs --tail=50 parser_cian

# Следить в реальном времени
docker-compose logs -f parser_cian

# Только успешные парсинги
docker-compose logs parser_cian | grep "✅"

# Только ошибки
docker-compose logs parser_cian | grep "❌"
```

### Статистика в логах

Ищите строки с эмодзи 📊:
```
📊 Статистика: Всего=100 | Успешно=95 | Ошибок=5 | Успех=95.0%
```

---

## 🔧 Troubleshooting

### Проблема: Парсер не видит куки

**Решение:**
```bash
# Проверьте Cookie Manager
curl http://localhost:8000/cookies

# Если пусто, запустите восстановление
curl -X POST http://localhost:8000/refresh

# Или откройте NoVNC и авторизуйтесь вручную
open http://localhost:8080/vnc.html
```

### Проблема: Ошибки при записи в Google Sheets

**Решение:**
```bash
# Проверьте credentials.json
ls -la credentials.json

# Проверьте SPREADSHEET_ID
grep SPREADSHEET_ID .env

# Проверьте права доступа Service Account
# Должен иметь доступ к таблице как Editor
```

### Проблема: Rate limit от Firecrawl

**Решение:**
```bash
# Уменьшите количество воркеров
PARSER_CONCURRENCY=1

# Или добавьте задержку между запросами (в parser.py)
await asyncio.sleep(1)  # После каждого запроса
```

### Проблема: Слишком много логов

**Решение:**
```bash
# Измените уровень логирования
LOG_LEVEL=WARNING

# Или фильтруйте логи
docker-compose logs parser_cian | grep -v "DEBUG"
```

---

## 📈 Оптимизация для больших объемов

### Для парсинга 100+ объявлений

1. **Увеличьте воркеров:**
   ```bash
   PARSER_CONCURRENCY=5
   ```

2. **Используйте батч-запись в Sheets:**
   В `queue_manager.py` можно накапливать строки и писать пачками по 10-20 штук.

3. **Мониторьте прогресс:**
   ```bash
   docker-compose logs -f parser_cian | grep "📊"
   ```

### Для парсинга 1000+ объявлений

1. **Разбейте на батчи:**
   Парсите по 100-200 URLs за раз

2. **Используйте очередь задач:**
   Рассмотрите использование Celery + Redis для управления задачами

3. **Добавьте retry логику:**
   Автоматический повтор при ошибках

---

## 🎓 Полезные команды

```bash
# Перезапустить парсер
docker-compose restart parser_cian

# Остановить все сервисы
docker-compose down

# Пересобрать и запустить
docker-compose up -d --build

# Очистить все (включая volumes)
docker-compose down -v

# Посмотреть использование ресурсов
docker stats

# Зайти внутрь контейнера
docker-compose exec parser_cian bash
```

---

## 📚 Дополнительная документация

- **[LOGGING_GUIDE.md](./LOGGING_GUIDE.md)** - Детальное описание системы логирования
- **[TESTING_GUIDE.md](./TESTING_GUIDE.md)** - Руководство по тестированию
- **[STRUCTURE.md](./STRUCTURE.md)** - Архитектура системы
- **[README.md](./README.md)** - Основная документация

---

## ✅ Чеклист перед Production

- [ ] Cookie Manager работает и возвращает валидные куки
- [ ] `credentials.json` настроен и имеет доступ к таблице
- [ ] `SPREADSHEET_ID` правильно указан в `.env`
- [ ] `FIRECRAWL_API_KEY` активен и имеет достаточный лимит
- [ ] Протестировано на 3-5 объявлениях через `test_queue_real.py`
- [ ] Настроен `PARSER_CONCURRENCY` (рекомендуется 2-3)
- [ ] Настроен `LOG_LEVEL=INFO`
- [ ] Проверены логи на наличие ошибок
- [ ] Настроены Telegram уведомления (опционально)

---

**Готовы к запуску?** 🚀

Начните с `python test_queue_real.py` для проверки с реальными данными!
