FROM python:3.11-slim

# Set timezone to Moscow
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Copy shared packages first
COPY packages /app/packages

# Copy service files
COPY services/category_counter /app/services/category_counter

# Install dependencies
RUN pip install --no-cache-dir -r /app/services/category_counter/requirements.txt

# Set PYTHONPATH so imports work correctly
ENV PYTHONPATH="/app"

CMD ["python", "-m", "services.category_counter.main"]
