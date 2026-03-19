FROM python:3.11-slim

# Set timezone to Moscow
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Устанавливаем рабочую директорию как корень проекта
WORKDIR /app

# Копируем и устанавливаем зависимости
COPY services/parser_cian/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем локальный cianparser
COPY services/parser_cian/cianparser/ ./cianparser_temp/
RUN pip install --no-cache-dir ./cianparser_temp/ && rm -rf ./cianparser_temp/

# Копируем общие пакеты (ядро) в корень контейнера
COPY packages/ ./packages/

# Копируем код микросервиса парсера
COPY services/parser_cian/ ./services/parser_cian/

# Добавляем корень проекта в пути поиска Python
ENV PYTHONPATH="/app:/app/services"

# Запускаем оркестратор
CMD ["python", "-m", "services.parser_cian.main"]