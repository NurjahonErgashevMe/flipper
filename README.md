# Flipper - Monorepo Architecture

Полнофункциональная микросервисная система для парсинга объявлений недвижимости с Cian.ru на основе **Docker Compose + Python Monorepo**.

## Quick Start

### 1. Setup
```bash
# Клонируем проект
git clone https://github.com/NurjahonErgashevMe/flipper
cd flipper

# Создаем .env файл
cp .env.example .env
# Заполняем переменные:
# - FIRECRAWL_API_KEY
# - SPREADSHEET_ID
# - PROXY_USERNAME, PROXY_PASSWORD
# - TG_BOT_TOKEN, TG_CHAT_ID (опционально)

# Копируем credentials.json от Google в корень проекта
```

### 2. Build & Run

```bash
# Запускаем все микросервисы
docker-compose up -d

# Проверяем здоровье
docker-compose ps
curl http://localhost:8000/health  # Cookie Manager
```

### 3. Manual Session Setup (если куки пусты)

```bash
# Откроем NoVNC в браузере
open http://localhost:8080/vnc.html

# Или вручную запустим восстановление
curl -X POST http://localhost:8000/recover

# Авторизуемся на Cian.ru в браузере
# Кликаем зеленую кнопку ГОТОВО
# Куки будут автоматически сохранены
```

### 4. Run Parser

```bash
# Запускаем парсер один раз (читает URLs из Google Sheets)
docker-compose run --rm parser_cian python -m services.parser_cian.main

# Или в фоне с логами
docker-compose up parser_cian
```

---

## Architecture

```
flipper/ (Monorepo Root)
│
├── packages/                          # 📦 Shared Libraries
│   └── flipper_core/
│       ├── sheets.py                  # Generic Google Sheets API wrapper
│       ├── utils.py                   # Common utilities (change_ip, etc)
│       └── __init__.py
│
├── services/                          # 🔧 Microservices
│   ├── cookie_manager/                # Service 1: Session Management
│   │   ├── main.py                    # FastAPI app + health checks
│   │   ├── browser.py                 # Playwright + NoVNC integration
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── entrypoint.sh
│   │
│   └── parser_cian/                   # Service 2: Firecrawl Parser
│       ├── main.py                    # Orchestrator entry point
│       ├── parser.py                  # AdParser (Firecrawl API)
│       ├── queue_manager.py           # Async queue + workers
│       ├── models.py                  # Pydantic data models
│       ├── config.py                  # Settings from .env
│       ├── Dockerfile                 # Monorepo-aware build
│       └── requirements.txt
│
├── docker-compose.yml                 # 🐳 Orchestration
├── .env                               # 🔐 Environment variables
├── credentials.json                   # 🔑 Google Service Account
├── STRUCTURE.md                       # 📋 System design doc
└── README.md                          # 📖 This file
```

---

## How It Works

### Data Flow

```
┌──────────────────────┐
│  Google Sheets       │  FILTERS tab with search URLs
└──────────┬───────────┘
           │
           ↓
┌─────────────────────────────────┐
│  parser_cian (Docker)           │
│  - main.py: Orchestrator        │
│  - Reads URLs from Sheets       │
└────────────┬────────────────────┘
             │
             ↓
┌─────────────────────────────────┐
│  QueueManager                   │  concurrency=2
│  [Worker 1] | [Worker 2]        │  (rate limit protection)
└────────────┬────────────────────┘
             │ parse_async(url)
             ↓
┌─────────────────────────────────┐
│  AdParser + Firecrawl API       │
│  (with cookies from Manager)    │
└────────────┬────────────────────┘
             │ structured data
             ↓
┌─────────────────────────────────┐
│  SheetsManager.write_row()      │ RESULTS tab
│  Google Sheets API              │
└─────────────────────────────────┘
```

### Component Roles

#### 1. **cookie_manager** (Port 8000 + 8080)
- **Responsibility:** Maintains valid Cian.ru session cookies
- **Tech:** FastAPI + Playwright + NoVNC + Redis
- **Features:**
  - Automatic cookie validation every 30 minutes
  - NoVNC browser for manual login if needed
  - Telegram alerts for session issues
  - RESTful API (`/cookies`, `/health`, `/status`)

#### 2. **parser_cian** (Stateless)
- **Responsibility:** Parse individual ads through Firecrawl API
- **Tech:** Python 3.11 + AsyncIO + Pydantic
- **Features:**
  - Reads search URLs from Google Sheets
  - Async queue with configurable concurrency (default: 2)
  - Structured data extraction via JSON Schema
  - Batch writing to Google Sheets
  - Graceful error handling

#### 3. **packages/flipper_core** (Reusable)
- **SheetsManager:** Generic Google Sheets API (tab-agnostic)
- **utils.py:** Shared functions (change_ip, retry logic, logging)

---

## Configuration

### Environment Variables (.env)

```bash
# ═══ REQUIRED ═══════════════════════════════════════════════════════════════
FIRECRAWL_API_KEY=fc-xxx...                    # API key from firecrawl.dev
SPREADSHEET_ID=1abc123...                      # Google Sheets document ID

# ═══ Google Sheets ═══════════════════════════════════════════════════════════
CREDENTIALS_PATH=/app/credentials.json         # Service account JSON

# ═══ Proxy & Rate Limiting ═══════════════════════════════════════════════════
PROXY_USERNAME=user123
PROXY_PASSWORD=pass456
CHANGE_IP_URL=http://proxy.example.com:8080/changeip
HTTP_PROXY=http://user123:pass456@proxy.example.com:8080

# ═══ Cookie Manager ═════════════════════════════════════════════════════════
COOKIE_CHECK_INTERVAL=1800                     # Seconds (30 min)
COOKIES_FILE=/app/cookies.json

# ═══ Parser Settings ════════════════════════════════════════════════════════
PARSER_CONCURRENCY=2                           # Simultaneous workers

# ═══ Notifications (Optional) ══════════════════════════════════════════════
TG_BOT_TOKEN=123456:ABCdef...
TG_CHAT_ID=987654321
```

---

## Docker Compose Commands

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f cookie_manager
docker-compose logs -f parser_cian

# Run parser once (one-shot)
docker-compose run --rm parser_cian python -m services.parser_cian.main

# Stop all services
docker-compose down

# Rebuild services
docker-compose build --no-cache

# Clean everything
docker-compose down -v  # Also removes volumes
```

---

## API Reference

### Cookie Manager (Port 8000)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/cookies` | GET | Returns cookies as JSON array |
| `/status` | GET | Current status (validation, failures, etc) |
| `/check` | POST | Force immediate cookie validation |
| `/refresh` | POST | Trigger manual recovery session |
| `/cookies` | POST | Upload cookies manually (debug) |

### Example Usage

```bash
# Get cookies
curl http://localhost:8000/cookies | jq

# Get status
curl http://localhost:8000/status | jq

# Trigger recovery
curl -X POST http://localhost:8000/refresh

# Manual cookie validation
curl -X POST http://localhost:8000/check | jq
```

---

## Adding a New Microservice

To add a new parser (e.g., Avito):

```bash
# 1. Create service directory
mkdir -p services/parser_avito

# 2. Create structure
touch services/parser_avito/{__init__.py,main.py,parser.py,models.py,config.py,requirements.txt,Dockerfile}

# 3. Update Dockerfile (same pattern as parser_cian)
#    - Monorepo context: context: .
#    - Copy packages: COPY packages/ ./packages/
#    - Copy service: COPY services/parser_avito/ ./services/parser_avito/

# 4. Import from common: from packages.flipper_core import SheetsManager

# 5. Add to docker-compose.yml
```

---

## Deployment

### Local Development
```bash
docker-compose -f docker-compose.yml up
```

### Production (AWS/GCP/Heroku)

```bash
# Build and push to Docker registry
docker build -f services/parser_cian/Dockerfile -t my-registry/parser_cian:latest .
docker push my-registry/parser_cian:latest

# Deploy docker-compose or Kubernetes
```

---

## Troubleshooting

### Cookie Manager Issues

```bash
# Check logs
docker-compose logs cookie_manager

# Manually check cookies
curl http://localhost:8000/cookies

# Trigger recovery
curl -X POST http://localhost:8000/refresh

# Check status
curl http://localhost:8000/status | jq
```

### Parser Issues

```bash
# Check logs
docker-compose logs parser_cian

# Test single URL (local development)
python test_parser.py

# Test with multiple workers (local development)
python test_queue.py

# Check connection to Cookie Manager
curl http://localhost:8000/health

# See detailed logging guide
cat LOGGING_GUIDE.md
```

### Google Sheets Errors

```bash
# Verify credentials.json exists
ls -la credentials.json

# Verify SPREADSHEET_ID in .env
grep SPREADSHEET_ID .env

# Test connection (inside parser container)
docker-compose exec parser_cian python -c \
  "from packages.flipper_core.sheets import SheetsManager; ..."
```

---

## Performance Tuning

### Parser Concurrency
```bash
# Increase workers (faster but higher rate-limit risk)
PARSER_CONCURRENCY=4

# Decrease workers (slower but safer)
PARSER_CONCURRENCY=1
```

### Cookie Validation Interval
```bash
# Check more frequently (seconds)
COOKIE_CHECK_INTERVAL=600  # 10 min instead of 30

# Check less frequently
COOKIE_CHECK_INTERVAL=3600  # 1 hour
```

---

## 📚 Documentation

Проект включает подробную документацию по архитектуре и интеграции:

| Документ | Содержание |
|----------|-----------|
| **[STRUCTURE.md](./STRUCTURE.md)** | System Design: архитектура, компоненты, потоки данных |
| **[MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)** | Процесс миграции из monolith → Monorepo |
| **[DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)** | Production deployment checklist |
| **[PRODUCTION_GUIDE.md](./PRODUCTION_GUIDE.md)** | Переход от тестирования к production |
| **[ARCHITECTURE.md](./ARCHITECTURE.md)** | Подробное описание каждого микросервиса |
| **[TESTING_GUIDE.md](./TESTING_GUIDE.md)** | Локальное тестирование и отладка |
| **[LOGGING_GUIDE.md](./LOGGING_GUIDE.md)** | Система логирования и мониторинг парсинга |
| **[COOKIE_MANAGER_LOGIC.md](./COOKIE_MANAGER_LOGIC.md)** | Логика работы Cookie Manager и проверки кук |
| **[THIRD_PARTY.md](./THIRD_PARTY.md)** | Встроенные библиотеки (cianparser, etc) |
| **[INTEGRATION_REPORT.md](./INTEGRATION_REPORT.md)** | Отчет об интеграции cianparser ✅ |

### Быстрые ссылки

- **Для новичков:** Начните с [STRUCTURE.md](./STRUCTURE.md)
- **Для деплоя:** [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)
- **Для разработки:** [TESTING_GUIDE.md](./TESTING_GUIDE.md)
- **Для production:** [PRODUCTION_GUIDE.md](./PRODUCTION_GUIDE.md)
- **Для мониторинга:** [LOGGING_GUIDE.md](./LOGGING_GUIDE.md)
- **Для внешних lib:** [THIRD_PARTY.md](./THIRD_PARTY.md)

---

## Development

### Adding Logging
```python
import logging

logger = logging.getLogger(__name__)
logger.info("Processing...")
logger.error("Error!", exc_info=True)
```

### Async Patterns
```python
# All I/O bound operations should be async
async def parse_async(self, url: str):
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    return response
```

### Type Hints
```python
from typing import List, Optional, Dict, Any
from pydantic import BaseModel

class MyData(BaseModel):
    name: str
    value: Optional[int] = None
```

---

## Contributing

1. Create a feature branch: `git checkout -b feature/my-feature`
2. Make changes and test locally
3. Push and create Pull Request
4. Ensure all services build: `docker-compose build`

---

## License

MIT

---

## Support

For issues or questions:
- Check logs: `docker-compose logs [service]`
- Review STRUCTURE.md for system design
- Open an issue on GitHub

---

**Last Updated:** 2024-03-17  
**Maintainer:** @NurjahonErgashevMe  
**Status:** ✅ Production Ready
