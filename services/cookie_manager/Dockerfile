FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Set timezone to Moscow
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# ── Python + System deps ──────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    xvfb \
    x11vnc \
    git \
    python3-numpy \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && python -m pip install --upgrade pip

# ── Install noVNC v1.4.0 (стабильная версия без JS ошибок) ───────────────────
RUN git clone --depth=1 --branch v1.4.0 https://github.com/novnc/noVNC.git /opt/novnc \
    && git clone --depth=1 https://github.com/novnc/websockify.git /opt/novnc/utils/websockify \
    && chmod +x /opt/novnc/utils/novnc_proxy \
    && ln -s /opt/novnc/vnc.html /opt/novnc/index.html

# ── App ───────────────────────────────────────────────────────────────────────
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium \
    && playwright install-deps chromium

COPY . .

RUN chmod +x /app/entrypoint.sh

RUN echo "[]" > /app/cookies.json

EXPOSE 8000 8080

ENTRYPOINT ["/app/entrypoint.sh"]
