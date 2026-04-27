#!/bin/bash
set -e

echo "[entrypoint] Starting Cookie Manager (no-browser, request-only)..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info
