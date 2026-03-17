# Новая логика работы Cookie Manager

## 🔄 Как это работает

### 1. Автоматическая проверка каждые 10 минут
Cookie Manager автоматически проверяет валидность кук каждые **10 минут** (600 секунд):
- Открывает `https://my.cian.ru/profile`
- Ищет `"isAuthenticated":true` в HTML
- Делает **3 попытки** с паузой 5 секунд между ними

### 2. Проверка критичных кук
Перед проверкой валидности проверяются критичные куки:
- `DMIR_AUTH` - основной токен авторизации
- `remixsid` - сессия VK

Если они пустые - сессия считается невалидной.

### 3. Проверка при парсинге
Когда парсер получает ошибку "No JSON data extracted":
1. Проверяет аутентификацию (3 попытки)
2. Если не авторизован - отправляет запрос в Cookie Manager
3. Cookie Manager запускает recovery

### 4. Recovery процесс
Когда куки невалидны:
1. **Все процессы парсинга останавливаются** (получают 503 ошибку)
2. Открывается браузер в NoVNC
3. Отправляется уведомление в Telegram
4. Ожидается авторизация пользователя
5. После нажатия кнопки "ГОТОВО" куки сохраняются
6. Парсинг возобновляется

---

## 🚀 Как использовать

### Перезапустите Cookie Manager с новыми изменениями:

```bash
# Остановите контейнер
docker-compose down cookie_manager

# Пересоберите образ
docker-compose build --no-cache cookie_manager

# Запустите заново
docker-compose up -d cookie_manager

# Проверьте логи
docker-compose logs -f cookie_manager
```

### Проверьте статус:

```bash
python check_cookie_status.py
```

Или:

```bash
curl http://localhost:8000/status | python -m json.tool
```

### Если куки невалидны:

1. **Откройте NoVNC:** http://localhost:8080/vnc.html
2. **Авторизуйтесь на Cian.ru** через VK
3. **Нажмите зеленую кнопку "✅ ГОТОВО"** в верхней части страницы
4. Куки автоматически сохранятся

---

## 📊 Мониторинг

### Логи Cookie Manager

```bash
docker-compose logs -f cookie_manager
```

Вы увидите:
```
[Monitor] 🔍 Running scheduled cookie check (every 600s)...
🔍 Cookie validity check attempt 1/3...
✅ Session is valid (attempt 1)
✅ Cookies are VALID (attempt 1)
[Monitor] ✅ Cookies OK. Next check in 600 seconds.
```

Или при проблемах:
```
❌ Critical cookies are empty or missing: DMIR_AUTH, remixsid
❌❌❌ All attempts failed — cookies are INVALID
[Monitor] ❌ Cookies INVALID. Consecutive failures: 1
[Monitor] 🚨 Triggering recovery session...
```

### Логи парсера

```bash
docker-compose logs -f parser_cian
```

Если куки невалидны:
```
⚠️ Cookies are empty, checking Cookie Manager status...
🚨 Cookie Manager confirmed: cookies are INVALID
ValueError: Cookies are empty. Recovery triggered. Please retry later.
```

---

## 🔧 Настройки

### Изменить интервал проверки

В `.env`:
```bash
# Проверять каждые 5 минут
COOKIE_CHECK_INTERVAL=300

# Проверять каждые 15 минут
COOKIE_CHECK_INTERVAL=900
```

### Изменить количество попыток

В `services/cookie_manager/main.py`:
```python
MAX_FAIL_ATTEMPTS = 3  # Изменить на нужное значение
```

---

## ⚠️ Важно

1. **Парсинг останавливается** когда recovery в процессе
2. **Все воркеры получат 503 ошибку** и должны подождать
3. **NoVNC должен быть доступен** для ручной авторизации
4. **Telegram уведомления** помогут не пропустить момент

---

## 🐛 Troubleshooting

### Recovery застрял

```bash
# Перезапустите контейнер
docker-compose restart cookie_manager

# Откройте NoVNC и завершите авторизацию
open http://localhost:8080/vnc.html
```

### Куки не обновляются

```bash
# Проверьте, что кнопка ГОТОВО видна в NoVNC
# Если нет - обновите страницу в браузере NoVNC

# Или запустите recovery вручную
curl -X POST http://localhost:8000/refresh
```

### Парсер не видит, что куки невалидны

```bash
# Принудительно проверьте куки
curl -X POST http://localhost:8000/check

# Проверьте статус
curl http://localhost:8000/status | python -m json.tool
```

---

**Готово!** Теперь система автоматически отслеживает валидность кук и запрашивает новую авторизацию при необходимости.
