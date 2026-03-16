FROM python:3.11-slim

# Устанавливаем рабочую директорию как корень проекта
WORKDIR /app

# Копируем и устанавливаем зависимости
COPY services/parser_cian/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общие пакеты (ядро) в корень контейнера
COPY packages/ ./packages/

# Копируем код микросервиса парсера
COPY services/parser_cian/ ./services/parser_cian/

# Добавляем корень проекта в пути поиска Python
ENV PYTHONPATH="/app"

# Запускаем оркестратор
CMD ["python", "services/parser_cian/main.py"]