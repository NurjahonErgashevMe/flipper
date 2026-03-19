# HTML to Markdown Converter Service

Go-сервис для конвертации HTML в Markdown с HTTP API и пулом воркеров для высокой производительности.

## Архитектура

- **Worker Pool**: 10 параллельных воркеров (настраивается)
- **Конкурентность**: Каждый HTTP-запрос обрабатывается в отдельной goroutine
- **Без блокировок**: Запросы не блокируют друг друга
- **Graceful Shutdown**: Корректное завершение всех задач при остановке

## Производительность

- Поддержка множественных одновременных запросов
- Автоматическое управление нагрузкой через worker pool
- Максимальный размер документа: 150MB
- Timeout на чтение/запись: 1 минута

## Быстрый старт

### Запуск через Docker (рекомендуется)

```bash
cd packages/go-html-to-md
docker-compose up -d
```

Сервис будет доступен на `http://localhost:8080`

### Запуск локально

```bash
cd packages/go-html-to-md
go run .
```

## API Endpoints

### Health Check
```bash
GET /health
```

### Convert HTML to Markdown
```bash
POST /convert
Content-Type: application/json

{
  "html": "<h1>Hello</h1><p>World</p>"
}
```

Response:
```json
{
  "markdown": "# Hello\n\nWorld",
  "success": true
}
```

## Использование из Python

### Установка зависимостей
```bash
pip install requests
```

### Одиночная конвертация

```python
from packages.flipper_core import HTMLToMarkdownConverter

converter = HTMLToMarkdownConverter()

if converter.is_healthy():
    markdown = converter.convert("<h1>Hello</h1>")
    print(markdown)  # # Hello
```

### Batch-конвертация (параллельная)

```python
from packages.flipper_core import HTMLToMarkdownConverter

converter = HTMLToMarkdownConverter(max_workers=5)

html_list = [
    "<h1>Doc 1</h1>",
    "<h2>Doc 2</h2>",
    "<h3>Doc 3</h3>",
]

results = converter.convert_batch(html_list)

for result in results:
    if result["error"]:
        print(f"Error: {result['error']}")
    else:
        print(result["markdown"])
```

### Полные примеры

Одиночная конвертация:
```bash
python example_html_to_md.py
```

Batch-конвертация:
```bash
python example_batch_conversion.py
```

## Управление сервисом

### Остановить
```bash
docker-compose down
```

### Перезапустить
```bash
docker-compose restart
```

### Логи
```bash
docker-compose logs -f
```

## Конфигурация

Переменные окружения:
- `PORT` - порт сервиса (по умолчанию: 8080)
- `ENV` - окружение (production/development)

Worker Pool (в коде):
- `defaultWorkers` - количество воркеров (по умолчанию: 10)

## Лимиты

- Максимальный размер запроса: 150MB
- Timeout чтения: 1 минута
- Timeout записи: 1 минута
- Параллельных воркеров: 10

## Как это работает без блокировок?

1. **Go HTTP Server** - автоматически создает goroutine для каждого запроса
2. **Worker Pool** - ограничивает количество одновременных конвертаций (10 воркеров)
3. **Очередь задач** - если все воркеры заняты, запросы ждут в буферизованном канале
4. **Python ThreadPoolExecutor** - позволяет делать параллельные запросы из Python

### Пример потока:
```
Python Request 1 ──┐
Python Request 2 ──┼──> HTTP Server ──> Worker Pool (10 workers) ──> Converter
Python Request 3 ──┘                         ↓
                                        Queue (buffer: 20)
```

Запросы не блокируют друг друга, но ограничены пулом воркеров для контроля ресурсов.
