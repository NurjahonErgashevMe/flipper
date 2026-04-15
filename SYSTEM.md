# Flipper — Система мониторинга недвижимости Cian

## Архитектура

Система состоит из нескольких Docker-контейнеров, управляемых через `docker-compose.yml`:

| Сервис | Назначение |
|--------|-----------|
| `parser_cian` | Основной парсер объявлений (два режима: `offers` и `avans`) |
| `category_counter` | Подсчёт количества объявлений по категориям → вкладка `Balans` |
| `cookie_manager` | Микросервис управления cookies для Firecrawl (Chromium + FastAPI) |
| `html_to_markdown` | Go-сервис конвертации HTML → Markdown |
| `app_redis` | Redis для cookie_manager |

Внешняя зависимость: **self-hosted Firecrawl** (отдельный docker-compose, сеть `firecrawl_backend`).

---

## Вкладки Google Sheets

| Вкладка | Источник | Критерий попадания |
|---------|----------|--------------------|
| **FILTERS** | Ввод вручную | URL-фильтры поисковых страниц Cian для режима `offers` |
| **Аванс** | `parser_cian --mode avans` | Активные объявления без аванса. Парсятся повторно каждый запуск. Удаляются при снятии или внесении аванса |
| **Аванс_Продано** | `parser_cian --mode avans` | В объявлении найден смысл «внесён аванс/задаток» и с момента публикации прошло **не более недели** (days_in_exposition ≤ 7) → добавляется в «Аванс_Продано», при этом строка удаляется из «Аванс», а ссылка удаляется из БД (перестаём отслеживать) |
| **Продано** | `parser_cian --mode offers` | Снятые с публикации **в течение недели** (days_in_exposition ≤ 7) |
| **Offers_Parser** | `parser_cian --mode offers` | **Все** объявления из FILTERS; объявления с **≥ 200 уникальных просмотров за сегодня** подсвечиваются цветом |
| **Signals_Parser** | `parser_cian --mode offers` | Объявления, удовлетворяющие хотя бы одному из: **снижение цены ≥ 5%** или **≥ 3 снижений цены за 30 дней** |
| **Balans** | `category_counter` | Количество объявлений по категориям (Вторичка/Первичка × Москва/МО) с формулой суммы и точкой равновесия |

---

## Сервис `parser_cian`

### Режимы работы

Запускается с флагом `--mode`:

```
python -m services.parser_cian.main --mode offers
python -m services.parser_cian.main --mode avans

# Alias (для совместимости со старым режимом)
python -m services.parser_cian.main --mode regular   # == offers
```

Дополнительные флаги:
- `--skip-links` — пропустить сбор ссылок, парсить только те URL, что уже есть в БД
- `--only-links` — только собрать ссылки с поисковых страниц в БД, без парсинга карточек

### Пайплайн выполнения

```
Step 1: Валидация конфигурации (.env)
Step 2: Инициализация (SQLite, Google Sheets, Firecrawl)
Step 3: Получение поисковых URL
         offers  → из вкладки FILTERS (Google Sheets)
         avans   → статичный URL из config (avans_search_url)
Step 4: Извлечение ссылок объявлений из поисковых страниц
         (cianparser + ротация прокси из data/proxies.txt)
Step 5: Парсинг каждого объявления через Firecrawl
         (N параллельных воркеров, QueueManager)
Step 6: (Опционально) очистка устаревших активных объявлений из БД (> ad_max_age_days)
Step 7: Итоговый отчёт
```

### Парсинг одного объявления (AdParser)

Данные собираются из **трёх источников**:

1. **Firecrawl AI-экстракция** — JSON-схема через LLM (цена, адрес, описание, история цен, ремонт, площадь, этажи и т.д.)
2. **rawHtml** — `creationDate` из встроенных JSON-LD скриптов страницы (точная дата публикации)
3. **Cian Statistics API** — `days_in_exposition`, `total_views`, `unique_views` (через API `/api/analytics/`)

Результат: модель `ParsedAdData` (Pydantic).

### Логика Worker (`QueueManager.worker`)

#### Режим `avans`

1. Парсим объявление → `ParsedAdData`
2. AI определяет `has_avans_deposit` (внесён ли аванс/задаток)
3. **Если AI нашла аванс**:
  - Если AI определила аванс/задаток и `days_in_exposition ≤ 7` → записать в **«Аванс_Продано»**, удалить строку из **«Аванс»** и удалить из БД (перестать отслеживать)
4. **Если снято с публикации** (`is_active = False`):
   - Удалить из **«Аванс»** + из БД
  - «Продано» фиксируется только для `mode=offers` и только если `days_in_exposition ≤ 7`
5. **Иначе** (активное, аванс не внесён):
   - Записать/обновить в **«Аванс»** (без цвета), обновить БД
   - Объявление будет спарсено повторно при следующем запуске

#### Режим `offers`

1. Парсим объявление → `ParsedAdData`
2. Вычисляем `signal_reason` (check_signals: снижение цены ≥ 5% или ≥ 3 снижений за 30 дней)
3. **Если снято с публикации** (`is_active = False`):
   - Удаляем из `cian_active_ads` в БД → больше не парсим
   - Описание → «Объявление снято с публикации»
  - Если `days_in_exposition ≤ 7` → записываем во вкладку **«Продано»**
   - Если объявление ранее было в Offers_Parser/Signals_Parser → строка окрашивается сероватым цветом `#D9D9D9`
4. **Если активно**:
   - Если `unique_views ≥ 200` → **Offers_Parser** + Telegram-уведомление
   - Если сработал `signal_reason` → **Signals_Parser** + Telegram-уведомление
   - Если критерий больше не выполняется → строка удаляется из соответствующей вкладки

### Сигналы (Signals_Parser)

Критерии (OR-логика):

| Критерий | Условие |
|----------|---------|
| Крупное снижение цены | Любое снижение ≥ 5% от предыдущей цены за всю историю |
| Частые снижения | ≥ 3 снижений цены за последние 30 дней |

Результат записывается в колонку `W: Reason` (например: `drops>=3 AND max_drop>=7.2%`).

---

## Сервис `category_counter`

Подсчитывает количество активных объявлений на Cian по четырём категориям:

- Вторичка Москва
- Первичка Москва
- Первичка МО
- Вторичка МО

Результат записывается во вкладку **Balans** (дата + количество по категориям + формула суммы + точка равновесия 150,000).

Скрейпинг HTML страниц Cian через `curl_cffi` с ротацией прокси из `data/proxies.txt`.

---

## База данных (SQLite)

Файл: `data/parser_cian.db`

### Таблицы

| Таблица | Назначение |
|---------|-----------|
| `cian_active_ads` | Активные объявления (URL, source, parsed_data JSON, is_parsed, is_active) |
| `cian_sold_ads` | Снятые с публикации (URL, parsed_data, publish_date) |
| `cian_filters` | Поисковые URL из FILTERS (URL + meta строки FILTERS) |

### Жизненный цикл объявления в БД

```
Новый URL с поисковой страницы
  ↓
cian_active_ads (is_parsed=False)
  ↓ парсинг Firecrawl
cian_active_ads (is_parsed=True, parsed_data=JSON)
  ↓
  ├─ is_active=True → остаётся, парсится повторно при следующем запуске
  ├─ is_active=False → перемещается в cian_sold_ads, удаляется из active
  └─ (опционально) publish_date > ad_max_age_days → удаляется из active (remove_stale_active_ads),
     если `CLEANUP_STALE_ACTIVE_ADS=true`
```

---

## Прокси

Файл: `data/proxies.txt` (формат: `host:port:user:password`, по строке).

Используются для:
- Скрейпинга поисковых страниц Cian (`cianparser`, `curl_cffi`)
- Скрейпинга категорий (`category_counter`)

Ротация: случайный выбор из списка для каждого запроса.

Firecrawl работает со своими прокси/без прокси (отдельная инфраструктура).

---

## Конфигурация (.env)

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `FIRECRAWL_API_KEY` | API-ключ Firecrawl (обязательно) | — |
| `FIRECRAWL_BASE_URL` | URL self-hosted Firecrawl | `http://localhost:3002` |
| `SPREADSHEET_ID` | ID документа Google Sheets | — |
| `CREDENTIALS_PATH` | Путь к JSON Service Account | `/app/credentials.json` |
| `PARSER_CONCURRENCY` | Количество параллельных воркеров | `20` |
| `MIN_UNIQUE_VIEWS` | Порог уникальных просмотров для Offers_Parser | `200` |
| `CLEANUP_STALE_ACTIVE_ADS` | Чистить активные объявления старше порога | `False` |
| `AD_MAX_AGE_DAYS` | Порог возраста (дней от publish_date) для очистки, если включена | `7` |
| `SOLD_MAX_AGE_DAYS` | Окно для попадания в «Продано» (дней от publish_date) | `7` |
| `USE_PROXIES_FOR_SEARCH` | Использовать прокси из файла | `True` |
| `CIAN_PROXIES_FILE` | Путь к файлу с прокси | `data/proxies.txt` |
| `TG_BOT_TOKEN` | Токен Telegram-бота для уведомлений | — |
| `TG_CHAT_ID` | ID чата для Telegram-уведомлений | — |
| `PARSER_CIAN_LOG_FILE` | Файл логов (ротация 10 МБ × 5) | `data/logs/parser_cian.log` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |

---

## Telegram-уведомления

Отправляются при:
- `Offers_Parser Match` — объявление набрало ≥ 200 уникальных просмотров (подсветка)
- `Signals_Parser Match` — сработал сигнал снижения цены
- `Avans Match` — объявление из режима avans набрало ≥ 200 просмотров

---

## Структура колонок Google Sheets (A–W)

| Колонка | Содержимое |
|---------|-----------|
| A | URL объявления |
| B | Дата публикации |
| C | Цена (руб.) |
| D | Заголовок |
| E | Полный адрес |
| F | Описание (или «Объявление снято с публикации» если `is_active = False`) |
| G | Цена за м² |
| H | Площадь (м²) |
| I | Год постройки |
| J | Дней в каталоге |
| K | Район |
| L | Этаж (текущий/всего) |
| M | Тип жилья (Вторичка/Новостройка) |
| N | Станция метро |
| O | Время до метро (мин.) |
| P | Округ |
| Q | Ремонт |
| R | Количество комнат |
| S | Всего просмотров |
| T | Уникальных просмотров (сегодня) |
| U | Cian ID |
| V | Время парсинга (МСК) |
| W | Reason (для Signals_Parser) |

После колонки **W** в `Offers_Parser` / `Signals_Parser` дописываются данные из строки вкладки **FILTERS**
(чтобы понимать, из какого фильтра пришло объявление): сначала реальный URL фильтра, затем отображаемое значение ячейки A,
дальше — значения остальных колонок этой строки FILTERS.

---

## Цветовая кодировка строк

| Цвет | Hex | Значение |
|------|-----|----------|
| Белый (по умолчанию) | `#FFFFFF` | Активное объявление |
| Зелёный (для просмотров) | `#B5D6A8` | `unique_views ≥ 200` (выделение в Offers_Parser и Signals_Parser) |
| Серый | `#D9D9D9` | Объявление снято с публикации (в Offers_Parser, Signals_Parser) |

---

## Docker-команды

```bash
# Запуск парсера (offers)
docker compose --profile manual run --rm parser_cian python -m services.parser_cian.main --mode offers

# Запуск парсера (avans)
docker compose --profile manual run --rm parser_cian python -m services.parser_cian.main --mode avans

# Только сбор ссылок (без парсинга карточек)
docker compose --profile manual run --rm parser_cian python -m services.parser_cian.main --mode offers --only-links

# Парсинг без сбора ссылок (из БД)
docker compose --profile manual run --rm parser_cian python -m services.parser_cian.main --mode offers --skip-links

# Alias (старое имя режима)
# docker compose --profile manual run --rm parser_cian python -m services.parser_cian.main --mode regular

# Подсчёт категорий
docker compose --profile manual run --rm category_counter python -m services.category_counter.main
```
