#!/bin/bash
set -e

echo "[entrypoint] Starting Cookie Manager services..."

# ── 1. Start Xvfb (virtual display for headless Chromium via NoVNC) ──────────
echo "[entrypoint] Starting Xvfb on :99..."
Xvfb :99 -screen 0 1280x800x24 -nolisten tcp &
XVFB_PID=$!
export DISPLAY=:99

# Give Xvfb a moment to initialize
sleep 1

# ── 2. Start x11vnc (VNC server connected to Xvfb) ───────────────────────────
echo "[entrypoint] Starting x11vnc..."
x11vnc -display :99 -nopw -forever -shared -rfbport 5900 -bg -o /tmp/x11vnc.log
sleep 1

# ── 3. Start noVNC (web-based VNC client on port 8080) ───────────────────────
echo "[entrypoint] Starting noVNC on port 8080..."
/opt/novnc/utils/novnc_proxy --vnc localhost:5900 --listen 8080 &
NOVNC_PID=$!
sleep 1

echo "[entrypoint] noVNC available at http://localhost:8080/vnc.html"

# ── 4. Start FastAPI (Cookie Manager API on port 8000) ───────────────────────
echo "[entrypoint] Starting FastAPI on port 8000..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --log-level info