"""
Cookie Manager — без браузера и NoVNC.

Логика:
    • cookies.json — единственное место, откуда парсер берёт куки.
    • Никаких периодических HTTP-проверок: мы НЕ долбим cian.ru, чтобы не
      ловить капчу. Считаем куки «валидными», пока в них есть DMIR_AUTH.
    • Когда парсер видит, что куки протухли — он сам дёргает POST /refresh,
      и мы тут же запускаем auto-login по пулу аккаунтов (auto_login.py).
    • Если на старте кук нет / нет DMIR_AUTH — авто-логин запускается сам.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from auto_login import DEFAULT_PASSWORD, AutoLogin

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
COOKIES_FILE = os.getenv("COOKIES_FILE", "/app/cookies.json")
ACCOUNTS_FILE = os.getenv("ACCOUNTS_FILE", "/app/accounts.json")
AUTO_LOGIN_MAX_ACCOUNTS = int(os.getenv("AUTO_LOGIN_MAX_ACCOUNTS", "5"))
LOGIN_PASSWORD = os.getenv("CIAN_DEFAULT_PASSWORD", DEFAULT_PASSWORD)

CRITICAL_COOKIE = "DMIR_AUTH"

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Cookie Manager", version="4.0.0")

# ── State ─────────────────────────────────────────────────────────────────────
_login_lock = asyncio.Lock()
_login_in_progress = False
_last_login_email: Optional[str] = None
_last_login_at: Optional[int] = None
_last_login_message: Optional[str] = None

_auto_login = AutoLogin(
    accounts_file=ACCOUNTS_FILE,
    cookies_file=COOKIES_FILE,
    password=LOGIN_PASSWORD,
)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_cookies() -> list:
    path = Path(COOKIES_FILE)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.error(f"Failed to read cookies: {exc}")
        return []


def _has_auth_cookie(cookies: list) -> bool:
    for c in cookies:
        if c.get("name") == CRITICAL_COOKIE and (c.get("value") or "").strip():
            return True
    return False


async def _do_auto_login(force_new_email: bool = False) -> bool:
    """Запускает один цикл авто-логина (с ротацией). Возвращает True при успехе."""
    global _last_login_email, _last_login_at, _last_login_message, _login_in_progress

    if _login_in_progress:
        logger.info("Auto-login skipped: уже идёт")
        return False

    async with _login_lock:
        _login_in_progress = True
        try:
            logger.info("🔐 Starting auto-login cycle...")
            ok, message, email = await _auto_login.perform_login_cycle(
                max_accounts=AUTO_LOGIN_MAX_ACCOUNTS,
                force_new_email=force_new_email,
            )
            _last_login_message = message
            if ok:
                import time as _t

                _last_login_email = email
                _last_login_at = int(_t.time())
                logger.info(f"✅ Auto-login OK as {email}: {message}")
            else:
                logger.error(f"❌ Auto-login failed: {message}")
            return ok
        finally:
            _login_in_progress = False


@app.on_event("startup")
async def _startup():
    logger.info(
        f"Cookie Manager v4 (no-browser). cookies={COOKIES_FILE}, "
        f"accounts={ACCOUNTS_FILE}, max_accounts_per_cycle={AUTO_LOGIN_MAX_ACCOUNTS}"
    )

    if not _has_auth_cookie(_load_cookies()):
        logger.warning(
            f"⚠️ {CRITICAL_COOKIE} отсутствует в {COOKIES_FILE} — запускаю авто-логин."
        )
        asyncio.create_task(_do_auto_login())


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/cookies")
async def get_cookies():
    """
    Возвращает куки парсеру. 503 — если идёт авто-логин или DMIR_AUTH пуст
    (и тогда параллельно стартуем авто-логин).
    """
    if _login_in_progress:
        raise HTTPException(
            status_code=503,
            detail="Auto-login in progress. Try again shortly.",
        )

    cookies = _load_cookies()
    if not cookies or not _has_auth_cookie(cookies):
        logger.warning(f"⚠️ {CRITICAL_COOKIE} отсутствует — стартую авто-логин")
        asyncio.create_task(_do_auto_login())
        raise HTTPException(
            status_code=503,
            detail=f"{CRITICAL_COOKIE} cookie missing. Auto-login started.",
        )

    return JSONResponse(content=cookies)


@app.post("/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    """
    Парсер дергает этот эндпоинт, когда видит, что куки протухли.
    Мы прогоняем цикл авто-логина (текущий аккаунт → следующие в пуле →
    свежий e-mail если все упали).
    """
    if _login_in_progress:
        return JSONResponse(
            status_code=202,
            content={"status": "already_running"},
        )

    background_tasks.add_task(_do_auto_login, False)
    return {"status": "started"}


@app.post("/login")
async def trigger_login(force_new_email: bool = False):
    """
    Ручной запуск авто-логина. `force_new_email=true` → сначала генерируем
    новый e-mail и логинимся им.
    """
    if _login_in_progress:
        return JSONResponse(
            status_code=202,
            content={"status": "already_running"},
        )

    asyncio.create_task(_do_auto_login(force_new_email=force_new_email))
    return {"status": "started", "force_new_email": force_new_email}


@app.post("/check")
async def quick_check():
    """
    Быстрая проверка БЕЗ запросов к cian: смотрим, есть ли DMIR_AUTH в файле.
    Если нет — стартуем авто-логин.
    """
    cookies = _load_cookies()
    has_auth = _has_auth_cookie(cookies)

    if not has_auth and not _login_in_progress:
        asyncio.create_task(_do_auto_login())

    return {
        "has_auth_cookie": has_auth,
        "login_in_progress": _login_in_progress,
        "login_triggered": (not has_auth) and (not _login_in_progress),
    }


@app.get("/accounts")
async def list_accounts():
    """Состояние пула аккаунтов: текущий, заблокированные, последние ошибки."""
    accounts = [a.to_dict() for a in _auto_login.get_accounts()]
    return {
        "current": _auto_login.get_current_email(),
        "accounts": accounts,
        "last_login_email": _last_login_email,
        "last_login_at": _last_login_at,
        "last_login_message": _last_login_message,
    }


@app.post("/accounts/unblock")
async def unblock_account(email: str):
    """Снять флаг blocked у аккаунта (если cian его разбанит)."""
    state = _auto_login._load_state()
    found = False
    for acc in state.get("accounts", []):
        if acc.get("email") == email:
            acc["blocked"] = False
            acc["fail_count"] = 0
            acc["last_error"] = None
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail=f"account {email} not found")
    _auto_login._save_state(state)
    return {"status": "ok", "email": email}


@app.get("/status")
async def get_status():
    cookies = _load_cookies()
    return {
        "cookie_count": len(cookies),
        "has_auth_cookie": _has_auth_cookie(cookies),
        "login_in_progress": _login_in_progress,
        "cookies_file": COOKIES_FILE,
        "accounts_file": ACCOUNTS_FILE,
        "auto_login": {
            "current_email": _auto_login.get_current_email(),
            "last_login_email": _last_login_email,
            "last_login_at": _last_login_at,
            "last_login_message": _last_login_message,
            "max_accounts_per_cycle": AUTO_LOGIN_MAX_ACCOUNTS,
        },
    }


@app.post("/cookies")
async def upload_cookies(cookies: list):
    """Ручная загрузка кук (для отладки)."""
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
