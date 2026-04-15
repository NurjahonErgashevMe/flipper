FROM python:3.11-slim

# Set timezone to Moscow
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Устанавливаем рабочую директорию как корень проекта
WORKDIR /app

# Копируем и устанавливаем зависимости
COPY services/parser_cian/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем общие пакеты (ядро) в корень контейнера
COPY packages/ ./packages/

# Копируем код микросервиса парсера
COPY services/parser_cian/ ./services/parser_cian/

# Добавляем корень проекта и встроенный cianparser в пути поиска Python
ENV PYTHONPATH="/app:/app/services:/app/services/parser_cian/cianparser"

# ENTRYPOINT чтобы docker compose run ... --skip-links --mode avans прокидывались
ENTRYPOINT ["python", "-m", "services.parser_cian.main"]