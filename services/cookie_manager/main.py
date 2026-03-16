import os
import json
import asyncio
import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

from browser import check_session_validity, start_recovery_session

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
COOKIES_FILE      = os.getenv("COOKIES_FILE", "/app/cookies.json")
CHECK_INTERVAL    = int(os.getenv("COOKIE_CHECK_INTERVAL", "1800"))  # 30 минут
MAX_FAIL_ATTEMPTS = 3  # сколько раз подряд false → считаем невалидными

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Cookie Manager", version="2.0.0")

# ── State ─────────────────────────────────────────────────────────────────────
_recovery_lock        = asyncio.Lock()
_recovery_in_progress = False
_last_check_result: Optional[bool] = None   # последний результат проверки
_consecutive_failures = 0                    # счётчик провалов подряд


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_cookies() -> list:
    path = Path(COOKIES_FILE)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.error(f"Failed to read cookies: {e}")
        return []


def _cookies_to_str(cookies: list) -> str:
    return "; ".join(f"{c['name']}={c['value']}" for c in cookies)


async def _run_recovery():
    """Запускает NoVNC-сессию для обновления кук."""
    global _recovery_in_progress, _consecutive_failures
    async with _recovery_lock:
        _recovery_in_progress = True
        try:
            logger.info("Starting cookie recovery session...")
            await start_recovery_session(COOKIES_FILE)
            _consecutive_failures = 0
            logger.info("Cookie recovery session finished successfully.")
        except Exception as e:
            logger.error(f"Recovery session error: {e}")
        finally:
            _recovery_in_progress = False


async def _check_cookies_validity() -> bool:
    """
    Проверяет валидность кук.
    Делает до MAX_FAIL_ATTEMPTS попыток с паузой 10 сек между ними.
    Возвращает True если хоть одна попытка успешна.
    """
    cookies = _load_cookies()
    if not cookies:
        logger.warning("No cookies to check.")
        return False

    cookie_str = _cookies_to_str(cookies)

    for attempt in range(MAX_FAIL_ATTEMPTS):
        logger.info(f"Cookie validity check attempt {attempt + 1}/{MAX_FAIL_ATTEMPTS}...")
        try:
            valid = await check_session_validity(cookie_str)
            if valid:
                logger.info("✅ Cookies are valid.")
                return True
            logger.warning(f"Attempt {attempt + 1}: isAuthenticated=false")
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} error: {e}")

        if attempt < MAX_FAIL_ATTEMPTS - 1:
            await asyncio.sleep(10)

    logger.error("❌ All attempts failed — cookies are invalid.")
    return False


async def _monitor_loop():
    """
    Фоновый цикл: каждые CHECK_INTERVAL секунд проверяет куки.
    Если все 3 попытки провалились — запускает recovery.
    """
    global _last_check_result, _consecutive_failures

    # Первая проверка через 60 сек после старта (даём время подняться сервисам)
    await asyncio.sleep(60)

    while True:
        logger.info("[Monitor] Running scheduled cookie check...")
        valid = await _check_cookies_validity()
        _last_check_result = valid

        if not valid:
            _consecutive_failures += 1
            logger.warning(f"[Monitor] Cookies invalid. Consecutive failures: {_consecutive_failures}")

            if not _recovery_in_progress:
                logger.info("[Monitor] Triggering recovery session...")
                asyncio.create_task(_run_recovery())
        else:
            _consecutive_failures = 0
            logger.info("[Monitor] Cookies OK. Next check in %d seconds.", CHECK_INTERVAL)

        await asyncio.sleep(CHECK_INTERVAL)


@app.on_event("startup")
async def startup():
    """Запускает фоновый мониторинг при старте."""
    asyncio.create_task(_monitor_loop())
    logger.info(f"Cookie monitor started. Check interval: {CHECK_INTERVAL}s")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/cookies")
async def get_cookies():
    """
    Возвращает куки как JSON-список.
    Вызывается AdParser.get_cookies_str().
    503 если куки пустые (запускает recovery автоматически).
    """
    cookies = _load_cookies()

    if not cookies:
        logger.warning("Cookies are empty — triggering recovery.")
        if not _recovery_in_progress:
            asyncio.create_task(_run_recovery())
        raise HTTPException(
            status_code=503,
            detail="Cookies are empty. Recovery session started.",
        )

    return JSONResponse(content=cookies)


@app.post("/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    """
    Ручной запуск обновления кук (NoVNC сессия).
    Вызывается AdParser.trigger_refresh() при isAuthenticated=false.
    """
    if _recovery_in_progress:
        return JSONResponse(
            status_code=202,
            content={"status": "already_running"},
        )

    background_tasks.add_task(_run_recovery)
    logger.info("Manual refresh triggered.")
    return {"status": "started"}


@app.get("/status")
async def get_status():
    """Текущий статус: куки, валидность, recovery."""
    cookies = _load_cookies()
    return {
        "cookie_count": len(cookies),
        "last_check_valid": _last_check_result,
        "consecutive_failures": _consecutive_failures,
        "recovery_in_progress": _recovery_in_progress,
        "check_interval_seconds": CHECK_INTERVAL,
        "cookies_file": COOKIES_FILE,
    }


@app.post("/check")
async def force_check():
    """Принудительная проверка валидности кук прямо сейчас."""
    global _last_check_result, _consecutive_failures

    valid = await _check_cookies_validity()
    _last_check_result = valid

    if not valid:
        _consecutive_failures += 1
        if not _recovery_in_progress:
            asyncio.create_task(_run_recovery())
    else:
        _consecutive_failures = 0

    return {
        "valid": valid,
        "consecutive_failures": _consecutive_failures,
        "recovery_triggered": not valid and not _recovery_in_progress,
    }


@app.post("/cookies")
async def upload_cookies(cookies: list):
    """Ручная загрузка кук (для тестов)."""
    if not isinstance(cookies, list):
        raise HTTPException(status_code=400, detail="Expected JSON array.")

    Path(COOKIES_FILE).write_text(
        json.dumps(cookies, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"Uploaded {len(cookies)} cookies manually.")
    return {"status": "ok", "cookie_count": len(cookies)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
