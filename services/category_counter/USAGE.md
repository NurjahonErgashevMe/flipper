# Category Counter - Инструкция по использованию

## Быстрый запуск

### Windows
```bash
run_category_counter.bat
```

### Linux/Mac
```bash
chmod +x run_category_counter.sh
./run_category_counter.sh
```

### Docker Compose
```bash
docker-compose run --rm category_counter
```

## Что делает сервис

1. Парсит 4 категории объявлений на Cian.ru:
   - Вторичка Москва
   - Первичка Москва
   - Первичка МО
   - Вторичка МО

2. Для каждой категории:
   - Делает запрос через curl_cffi (имитация браузера)
   - Использует мобильный прокси с ротацией IP (если настроен)
   - Конвертирует HTML в Markdown
   - Извлекает количество объявлений из текста "Найдено {n} объявлений"

3. Записывает результаты в Google Sheets (таб "Balans"):
   - Новая строка с датой и временем парсинга
   - Количество объявлений по каждой категории
   - Итоговая сумма (формула =SUM(B:E))
   - Точка равновесия (150000)

## Настройка

### Обязательные переменные в .env

```env
# Google Sheets ID
SPREADSHEET_ID=your_spreadsheet_id_here

# Путь к credentials.json
CREDENTIALS_PATH=/app/credentials.json
```

### Опциональные переменные

```env
# Мобильный прокси с ротацией IP (опционально)
# Format: http://username:password@proxy:port
HTTP_PROXY=http://your_proxy_username:your_proxy_password@proxy.example.com:8080

# Endpoint для смены IP (если поддерживается прокси)
CHANGE_IP_URL=http://proxy.example.com:8080/changeip
```

## Структура записи в Google Sheets

Каждый запуск добавляет новую строку:

```
| A                    | B              | C              | D            | E            | F              | G                  |
|----------------------|----------------|----------------|--------------|--------------|----------------|--------------------|
| Дата                 | Вторичка МСК   | Первичка МСК   | Первичка МО  | Вторичка МО  | Всего          | Точка равновесия   |
| 19.03.2026 14:30:00  | 123456         | 234567         | 345678       | 456789       | =SUM(B2:E2)    | 150000             |
| 19.03.2026 18:45:00  | 124000         | 235000         | 346000       | 457000       | =SUM(B3:E3)    | 150000             |
```

Формат даты: `DD.MM.YYYY HH:MM:SS` (совместим с Google Sheets)

## Логи

Сервис выводит подробные логи:
- Используемые прокси
- Процесс парсинга каждой категории
- Найденное количество объявлений
- Результат записи в Google Sheets

## Troubleshooting

### Ошибка "SPREADSHEET_ID not set"
Проверьте файл `.env` - должна быть переменная `SPREADSHEET_ID`

### Ошибка "Failed to fetch HTML"
- Проверьте мобильный прокси (если используется)
- Возможно, Cian заблокировал IP
- Попробуйте сменить IP через CHANGE_IP_URL endpoint

### Ошибка "Failed to convert HTML to markdown"
- Проверьте, что сервис `html_to_markdown` запущен
- Выполните: `docker-compose ps` - должен быть `html_to_markdown (healthy)`

### Ошибка "Failed to extract count"
- Возможно, изменился формат страницы Cian
- Проверьте логи - там будет preview HTML
- Может потребоваться обновить regex паттерн

## Частота запуска

Рекомендуется запускать:
- 1 раз в день (для отслеживания динамики)
- В одно и то же время (для консистентности данных)

Можно настроить cron/Task Scheduler для автоматического запуска.
