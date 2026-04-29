#!/bin/bash
set -e

echo "[entrypoint] Starting Cookie Manager (no-browser, request-only)..."

# Защита от ситуации «docker создал директорию вместо файла на хосте»:
# если volume bind-mount указывает на несуществующий хост-файл, docker
# молча создаёт ДИРЕКТОРИЮ на хосте — и потом любая попытка прочитать
# /app/cookies.json как файл падает. Сразу даём понятную ошибку.
for f in /app/cookies.json /app/accounts.json; do
    if [ -d "$f" ]; then
        echo "[entrypoint] FATAL: $f смонтирован как директория, а должен быть файлом."
        echo "[entrypoint] На хосте: 'rm -rf <host_path>' и положить нормальный файл."
        exit 78
    fi
    if [ ! -e "$f" ]; then
        echo "[entrypoint] $f не существует — создаю с дефолтным содержимым"
        case "$f" in
            *cookies.json) echo '[]' > "$f" ;;
            *accounts.json) echo '{"current": null, "accounts": []}' > "$f" ;;
        esac
    fi
done

exec uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info
