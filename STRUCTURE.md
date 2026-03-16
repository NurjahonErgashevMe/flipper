# Flipper - Architecture & System Design

## Overview
**Flipper** — микросервисная система для парсинга объявлений недвижимости с cian.ru с управлением сессиями, кэшированием куков и загрузкой данных в Google Sheets.

**Стек**: Python 3.11+, FastAPI, Playwright, Firecrawl API, Docker Compose, Google Sheets API

---

## Directory Structure

```
flipper/
├── flipping_cian/           # Основной парсер и оркестратор
│   ├── main.py              # Entry point: запуск полного цикла
│   ├── parser.py            # AdParser - интеграция с Firecrawl + Cookie Manager
│   ├── sheets.py            # SheetsManager - интеграция с Google Sheets API
│   ├── queue_manager.py     # QueueManager - асинхронное управление кюй (concurrency 2)
│   ├── models.py            # Pydantic модели для данных объявлений
│   ├── config.py            # Конфигурация и переменные окружения
│   ├── credentials.json     # Google Service Account credentials
│   ├── cianparser/          # Внешняя библиотека для Cian парсинга (git submodule)
│   └── __pycache__/
│
├── cookie_manager/          # Микросервис управления куками (Docker)
│   ├── main.py              # FastAPI приложение с эндпоинтами
│   ├── browser.py           # Playwright: проверка сессии, восстановление
│   ├── Dockerfile           # Docker образ с Playwright + NoVNC
│   ├── entrypoint.sh        # Точка входа контейнера
│   ├── requirements.txt      # Зависимости
│   ├── cookies.json         # Хранилище куков
│   └── __pycache__/
│
├── docker-compose.yml       # Орхестрация контейнеров
├── .env                     # Переменные окружения (не в repo)
├── test_parser.py           # Тесты и проверка функциональности
├── data.json                # Временные данные
├── hasProfile.html          # HTML шаблон (валидная сессия)
├── hasNOTProfile.html       # HTML шаблон (невалидная сессия)
└── technical/               # Документация и заметки
```

---

## Component Architecture

### 1. **flipping_cian** — Main Application Layer

#### `main.py` (Orchestrator)
**Что делает:**
- Инициализирует все компоненты (SheetsManager, AdParser, QueueManager)
- Читает поисковые URLs из Google Sheets (табличка "FILTERS")
- Передает URLs в парсер через QueueManager
- Логирует и управляет ошибками

**Поток:**
```
Google Sheets (URLs) 
    ↓
[QueueManager: concurrency=2]
    ↓
[AdParser] → [Cookie Manager API] → [Firecrawl API]
    ↓
[Parsed Data] → Google Sheets (Results)
```

**Запуск:**
```bash
cd flipping_cian
python main.py
```

---

#### `parser.py` (AdParser - Core Parser)
**Что делает:**
- Получает куки из Cookie Manager (`http://localhost:8000/cookies`)
- Отправляет запрос на Firecrawl API v2 с куками в headers
- Экстрактирует данные по JSON Schema (цена, адрес, площадь, ID и т.д)
- Возвращает ParsedAdData объект

**Ключевые методы:**
- `_get_cookies()` — Асинхронный запрос куков к Cookie Manager
- `_get_schema()` — Возвращает JSON Schema для структурированной экстракции
- `parse_async(url)` — Основной метод парсинга одного объявления

**Зависимости:**
- `httpx` — асинхронные HTTP запросы
- `FIRECRAWL_API_KEY` — env переменная (обязательна)
- Cookie Manager API (порт 8000)

**Ошибки обработки:**
- Если Cookie Manager недоступен → логирует, продолжает без куков
- Если Firecrawl возвращает ошибку → выбрасывает исключение

---

#### `sheets.py` (SheetsManager)
**Что делает:**
- Аутентификация через Google Service Account (`credentials.json`)
- Чтение URLs из табл. "FILTERS" колонка A
- Запись распарсенных данных в табл. "RESULTS" (новая строка за раз)

**Ключевые методы:**
- `get_urls()` — Читает список поисковых URLs
- `write_parsed_row(row)` — Записывает распарсенное объявление (1 строка = 1 объявление)

**Google Sheets интеграция:**
- Требует `credentials.json` от Google Cloud Service Account
- Scope: `https://www.googleapis.com/auth/spreadsheets`

---

#### `queue_manager.py` (QueueManager)
**Что делает:**
- Управляет асинхронной очередью с ограничением concurrency
- Запускает N воркеров (по умолчанию 2)
- Каждый воркер: берет URL → парсит → пишет в Sheets

**Архитектура:**
```
Queue [URL1, URL2, URL3, ...]
  ↓
[Worker 1] ↓ [Worker 2]
  ↓ parse_async(url) → write to Sheets
  ↓ queue.task_done()
```

**Параметры:**
- `concurrency=2` — макс 2 одновременных запроса (защита от rate limits)

---

#### `models.py` (Data Models - Pydantic)
**Основные модели:**

```python
ParsedAdData:
  - price: int (рубли)
  - title: str
  - address: AddressInfo
  - description: str
  - price_per_m2: int
  - area: float
  - rooms: int
  - floor: FloorInfo
  - cian_id: str

AddressInfo:
  - full: str
  - district: str (район)
  - metro_station: str
  - okrug: str (ЦАО, САО, и т.д)

FloorInfo:
  - current: int (этаж)
  - all: int (всего этажей)
```

**Методы:**
- `to_row()` — Конвертирует модель в список для Google Sheets

---

#### `config.py` (Settings & Environment)
**Что делает:**
- Загружает переменные из `.env` файла
- Валидирует required параметры
- Предоставляет функцию `change_ip()` для смены IP прокси

**Переменные окружения:**
```
FIRECRAWL_API_KEY       # API ключ Firecrawl (обязательно)
SPREADSHEET_ID          # ID Google Sheets документа (обязательно)
CREDENTIALS_PATH        # Путь к credentials.json
PROXY_USERNAME          # Username для мобильного прокси
PROXY_PASSWORD          # Password для мобильного прокси
CHANGE_IP_URL           # URL для смены IP (например: http://proxy.local/changeip)
HTTP_PROXY              # Прокси URL (http://user:pass@host:port)
```

**Функция `change_ip()`:**
- Отправляет GET запрос на CHANGE_IP_URL
- Ждет 5 сек после смены IP
- Логирует результат

---

### 2. **cookie_manager** — Session Management Microservice (Docker)

**Назначение:** Управление куками Cian, проверка валидности сессии, восстановление при необходимости

**Порты:**
- `8000` — FastAPI API (для парсера)
- `8080` — NoVNC веб-интерфейс (для мониторинга)

---

#### `main.py` (FastAPI Server)
**API Endpoints:**

| Эндпоинт | Метод | Описание |
|----------|-------|---------|
| `/cookies` | GET | Возвращает массив cookie объектов |
| `/health` | GET | Проверка здоровья сервиса |
| `/recover` | POST | Запустить восстановление сессии (background) |

**Главная логика:**
1. Загружает куки из `cookies.json` при старте
2. Каждые 30 минут проверяет валидность сессии
3. Если 3 раза подряд проверка false → запускает восстановление
4. В фоне восстанавливает сессию через Playwright

**Состояние:**
```python
_consecutive_failures   # Счетчик ошибок валидации подряд
_recovery_in_progress   # Флаг восстановления
_last_check_result      # Результат последней проверки
```

---

#### `browser.py` (Playwright Session Management)
**Ключевые функции:**

```python
check_session_validity(cookie_str) -> bool
  # Проверяет, валидна ли сессия
  # Делает GET запрос на https://my.cian.ru/profile
  # Ищет '"isAuthenticated":true' в HTML
  # 3 попытки если что-то пошло не так
  # Возвращает: True (валидна) / False (невалидна)

start_recovery_session() -> bool
  # Запускает Playwright браузер
  # Логирует в аккаунт Cian
  # Извлекает куки и сохраняет в cookies.json
  # Отправляет уведомление в Telegram при успехе/ошибке
  # Возвращает: True (успех) / False (провал)
```

**Интеграция:**
- Телеграм уведомления (если TG_BOT_TOKEN и TG_CHAT_ID установлены)
- NoVNC для визуального мониторинга браузера
- User-Agent и Headers имитируют обычный браузер

---

#### `Dockerfile` + `entrypoint.sh`
**Образ содержит:**
- Python 3.11
- Playwright (Chromium)
- NoVNC сервер
- FastAPI

**Запуск контейнера:**
```bash
docker-compose up cookie_manager
```

Внутри контейнера:
1. Запускается NoVNC на порт 8080
2. Запускается FastAPI на порт 8000
3. Загружаются куки из `cookies.json`
4. Запускается background таск проверки валидности

---

### 3. **docker-compose.yml** (Orchestration)

**Сервисы:**

```yaml
cookie_manager:
  - Build: ./cookie_manager/Dockerfile
  - Ports: 8000 (API), 8080 (NoVNC)
  - Volumes: ./cookies.json, ./cookie_manager
  - Depends on: app_redis

app_redis:
  - Image: redis:alpine
  - Назначение: кэш и лимиты для rate limiting
```

**Запуск всего:**
```bash
docker-compose up -d
```

---

## Data Flow Diagram

```
┌─────────────────────┐
│  Google Sheets      │
│  (FILTERS tab)      │
└──────────┬──────────┘
           │ get_urls()
           ↓
┌─────────────────────────────────┐
│   flipping_cian/main.py          │
│   (Orchestrator)                 │
└──────────┬──────────────────────┘
           │ initialize
           ↓
┌─────────────────────────────────┐
│   QueueManager                   │
│   (concurrency: 2)              │
│   ┌─────────────────────┐       │
│   │ Worker 1 │ Worker 2 │       │
│   └────┬──────┴────┬────┘       │
└────────┼───────────┼────────────┘
         │           │
         ↓           ↓
    ┌────────────────────┐
    │   AdParser         │
    │ parse_async(url)   │
    └────────┬───────────┘
             │
    ┌────────▼──────────┐
    │ Cookie Manager    │
    │ API (Port 8000)   │  ← GET /cookies
    │  (Docker)         │
    └────────┬──────────┘
             │ cookies in headers
    ┌────────▼──────────────────────┐
    │   Firecrawl API v2             │
    │ https://api.firecrawl.dev      │
    │ (JSON Schema extraction)       │
    └────────┬──────────────────────┘
             │ ParsedAdData
    ┌────────▼──────────┐
    │   SheetsManager   │
    │ write_parsed_row()│
    └────────┬──────────┘
             │
    ┌────────▼──────────────┐
    │  Google Sheets        │
    │  (RESULTS tab)        │
    └───────────────────────┘
```

---

## Running the System

### Prerequisites
1. **Google Cloud Setup:**
   - Создать Service Account в Google Cloud Console
   - Скачать credentials.json
   - Разместить в `flipping_cian/credentials.json`
   - Предоставить доступ к Google Sheets документу

2. **Firecrawl API:**
   - Получить API key от https://firecrawl.dev
   - Установить в `.env`: `FIRECRAWL_API_KEY=...`

3. **Окружение:**
   - Создать `.env` файл в корне проекта
   - Установить все required переменные

4. **Docker:**
   - Docker и Docker Compose установлены

---

### Step-by-Step Startup

**1. Запустить инфраструктуру:**
```bash
docker-compose up -d
```
Это запустит:
- Cookie Manager (API + NoVNC)
- Redis для кэша

Проверка здоровья:
```bash
curl http://localhost:8000/health
```

**2. Проверить куки (если их еще нет):**
- Откройте http://localhost:8080/vnc.html
- Вручную залогиньтесь в Cian
- FastAPI автоматически извлечет куки в cookies.json

ИЛИ вызовите восстановление:
```bash
curl -X POST http://localhost:8000/recover
```

**3. Запустить парсер:**
```bash
cd flipping_cian
python main.py
```

**4. Мониторить процесс:**
- Логи парсера в консоль
- Результаты пишутся в Google Sheets (RESULTS tab)
- При ошибках куков → автоматическое восстановление

---

## Testing

**Тестирование одного объявления:**
```bash
python test_parser.py
```

Это тестирует:
- Подключение к Cookie Manager
- Получение куков
- Парсинг одного URL через Firecrawl
- Валидацию Pydantic модели

**Ожидаемый output:**
```
[*] Тестируем v2.0 SDK для: https://www.cian.ru/sale/flat/312533860/
[✓] Успешно!
Цена: 20000000 руб.
ID: 312533860
```

---

## Environment Variables (.env)

```bash
# === Firecrawl ===
FIRECRAWL_API_KEY=fc-xxx...

# === Google Sheets ===
SPREADSHEET_ID=1abc123def456...

# === Proxy (Mobile IP Rotation) ===
PROXY_USERNAME=user123
PROXY_PASSWORD=pass456
CHANGE_IP_URL=http://proxy.example.com:8080/changeip
HTTP_PROXY=http://user123:pass456@proxy.example.com:8080

# === Telegram Notifications (optional) ===
TG_BOT_TOKEN=123456:ABCdefGHIjklmnOP...
TG_CHAT_ID=987654321

# === Cookie Manager ===
COOKIES_FILE=/app/cookies.json
COOKIE_CHECK_INTERVAL=1800  # 30 минут
```

---

## Performance Characteristics

| Параметр | Значение | Примечание |
|----------|----------|-----------|
| **QueueManager Concurrency** | 2 | Защита от rate limits Cian |
| **Cookie Validation Check** | Каждые 30 мин | Автоматическое восстановление при ошибке |
| **Max Consecutive Failures** | 3 | Лимит перед восстановлением сессии |
| **Parse Time (per ad)** | ~3-5 сек | Зависит от Firecrawl API |
| **Rate Limit Protection** | Мобильный прокси | Change IP между запросами (настраивается) |

---

## Error Handling & Recovery

### Cookie Manager
1. **Валидация fails 1x** → Логирует, продолжает с old cookies
2. **Валидация fails 3x подряд** → Запускает восстановление в фоне
3. **Восстановление fails** → Отправляет alert в Telegram
4. **Восстановление success** → Обновляет cookies.json, продолжает работу

### AdParser
1. **Cookie Manager недоступен** → Логирует, запрашивает без куков
2. **Firecrawl API ошибка** → Выбрасывает exception, Worker логирует и продолжает
3. **Невалидные данные** → Pydantic validation error, логирует, пропускает

### Main App
1. **Sheets error** → Логирует, но очередь продолжает работу
2. **No URLs found** → Graceful shutdown с логом

---

## Development Notes

- **Async-first:** Весь код использует `asyncio` для параллельной обработки
- **Type hints:** Полная типизация через Pydantic
- **Logging:** Структурированное логирование с timestamps
- **Modular:** Компоненты легко заменяются (например, другой источник URLs)
- **Scalable:** QueueManager легко масштабируется изменением `concurrency`

---

## Deployment Checklist

- [ ] `.env` файл создан с всеми required переменными
- [ ] Google Service Account credentials установлены
- [ ] Firecrawl API key валиден
- [ ] Docker и Docker Compose установлены
- [ ] Google Sheets документ имеет табл. "FILTERS" (URLs) и "RESULTS" (output)
- [ ] Мобильный прокси настроен (если используется)
- [ ] Telegram credentials установлены (если нужны уведомления)
- [ ] `docker-compose up -d` успешно запустился
- [ ] `curl http://localhost:8000/health` возвращает 200
- [ ] Куки получены (вручную или через восстановление)
- [ ] `python test_parser.py` прошел успешно
- [ ] `python main.py` запущен и логирует обработку URLs

---

## Contact & Support

**Проект:** Flipper Real Estate Parser  
**Язык:** Python 3.11+  
**Лицензия:** MIT  
**Repository:** flipper (GitHub)
