"""
Cookie Manager — без браузера и NoVNC.

Логика:
    • cookies.json — единственное место, откуда парсер берёт куки.
    • Никаких периодических HTTP-проверок: мы НЕ долбим cian.ru, чтобы не
      ловить капчу. Считаем куки «валидными», пока в них есть DMIR_AUTH.
    • Когда парсер видит, что куки протухли — он сам дёргает POST /refresh,
      и мы тут же запускаем auto-login по пулу аккаунтов (auto_login.py).
    • Если на старте кук нет / нет DMIR_AUTH — авто-логин запускается сам.

Гарантии для парсера:
    • GET /cookies НЕ возвращает 503 «retry later» сразу: если идёт recovery
      или кук нет, держит соединение и ждёт окончания авто-логина (до
      COOKIES_WAIT_FOR_LOGIN_S сек). Поэтому 50 параллельных воркеров не
      падают разом — они блокируются на одном `_login_lock` и получают
      свежие куки одновременно после успешного логина.
    • GET /health возвращает 503 пока DMIR_AUTH не получен. Docker compose
      `depends_on: service_healthy` НЕ запускает парсер пока куки не
      готовы (start_period=180s даёт время на первый логин).
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
# LOG_LEVEL=DEBUG → детальные логи каждого HTTP-шага auto-login.
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
# Гарантируем что наш logger тоже подхватит DEBUG, даже если basicConfig
# уже настроен в родительском процессе (uvicorn).
logging.getLogger("auto_login").setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# ── Config ────────────────────────────────────────────────────────────────────
COOKIES_FILE = os.getenv("COOKIES_FILE", "/app/cookies.json")
ACCOUNTS_FILE = os.getenv("ACCOUNTS_FILE", "/app/accounts.json")
AUTO_LOGIN_MAX_ACCOUNTS = int(os.getenv("AUTO_LOGIN_MAX_ACCOUNTS", "5"))
LOGIN_PASSWORD = os.getenv("CIAN_DEFAULT_PASSWORD", DEFAULT_PASSWORD)

# Сколько секунд /cookies ждёт окончания текущего авто-логина прежде чем
# вернуть 503. Нужно чтобы при race condition «парсер стартовал параллельно
# с recovery» воркеры не падали все одновременно, а спокойно дождались.
COOKIES_WAIT_FOR_LOGIN_S = float(os.getenv("COOKIES_WAIT_FOR_LOGIN_S", "90"))

# Background retry: если первый логин упал (rate-limit, сетевой блип, все
# аккаунты в bad-state) — НЕ виснем навсегда, а пытаемся снова с backoff'ом.
# Без этого контейнер один раз стартанул, не залогинился — и парсер навсегда
# unhealthy до ручного `docker compose restart`.
BACKGROUND_LOGIN_RETRY_MIN_S = float(os.getenv("BACKGROUND_LOGIN_RETRY_MIN_S", "30"))
BACKGROUND_LOGIN_RETRY_MAX_S = float(os.getenv("BACKGROUND_LOGIN_RETRY_MAX_S", "600"))

# Файл с резидентскими прокси (host:port:user:pass по строке). Тот же файл,
# что использует parser_cian — единый источник истины. Если файла нет —
# логинимся напрямую с IP контейнера. На забаненном сервере без прокси
# логин не пройдёт никогда — рекомендуется задать.
CIAN_PROXIES_FILE = os.getenv("CIAN_PROXIES_FILE", "/app/data/proxies.txt")

CRITICAL_COOKIE = "DMIR_AUTH"

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Cookie Manager", version="4.0.0")

# ── State ─────────────────────────────────────────────────────────────────────
_login_lock = asyncio.Lock()
_login_in_progress = False
_last_login_email: Optional[str] = None
_last_login_at: Optional[int] = None
_last_login_message: Optional[str] = None

def _load_proxies(path: str) -> list:
    """
    Читает data/proxies.txt → список URL `http://user:pass@host:port`.
    Использует тот же формат, что parser_cian (host:port:user:pass).
    Если файла нет / пуст — возвращает [], логинимся напрямую.
    """
    if not path or not Path(path).is_file():
        logger.info(f"_load_proxies: {path} нет — пул прокси пустой")
        return []
    try:
        # Локальная реализация парсера: cookie_manager не имеет PYTHONPATH
        # к packages/flipper_core/proxy_loader.py — проще продублировать.
        from urllib.parse import quote

        out: list = []
        for raw in Path(path).read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) < 4:
                logger.warning(f"_load_proxies: пропуск (нужно host:port:user:pass): {line[:120]}")
                continue
            host, port, user = parts[0], parts[1], parts[2]
            password = ":".join(parts[3:])
            out.append(
                f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}"
            )
        logger.info(f"_load_proxies: загружено {len(out)} прокси из {path}")
        return out
    except Exception as exc:
        logger.error(f"_load_proxies: ошибка чтения {path}: {exc}")
        return []


_auto_login = AutoLogin(
    accounts_file=ACCOUNTS_FILE,
    cookies_file=COOKIES_FILE,
    password=LOGIN_PASSWORD,
    proxies=_load_proxies(CIAN_PROXIES_FILE),
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
        # Защита от лишнего цикла: пока мы стояли в очереди за локом, кто-то
        # другой (например /cookies через _ensure_login_and_wait) уже мог
        # успешно залогиниться — тогда пропускаем.
        if not force_new_email and _has_auth_cookie(_load_cookies()):
            logger.info(
                "Auto-login skipped: DMIR_AUTH уже получен другим путём"
            )
            return True

        _login_in_progress = True
        try:
            logger.info(
                f"🔐 Starting auto-login cycle (force_new_email={force_new_email})..."
            )
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


async def _background_login_keeper():
    """
    Фоновая задача: гарантирует наличие DMIR_AUTH. Если кук нет — пытается
    логин, при провале ждёт с экспоненциальным backoff'ом и пробует снова.
    Так контейнер никогда не «зависает» в no-cookies state.

    Когда DMIR_AUTH есть — задача спит и просыпается раз в минуту проверить.
    Если куки протухли (`/refresh` сбросил их) — заново стартует цикл.
    """
    delay = BACKGROUND_LOGIN_RETRY_MIN_S
    while True:
        if _has_auth_cookie(_load_cookies()):
            delay = BACKGROUND_LOGIN_RETRY_MIN_S
            await asyncio.sleep(60)
            continue

        if _login_in_progress:
            await asyncio.sleep(5)
            continue

        logger.info(
            f"🔁 background-keeper: DMIR_AUTH нет, пробую авто-логин "
            f"(retry delay будет {delay:.0f}s при провале)"
        )
        ok = await _do_auto_login()
        if ok:
            logger.info("🔁 background-keeper: ✅ DMIR_AUTH получен")
            delay = BACKGROUND_LOGIN_RETRY_MIN_S
            continue

        logger.warning(
            f"🔁 background-keeper: ❌ авто-логин упал, повтор через {delay:.0f}s"
        )
        await asyncio.sleep(delay)
        delay = min(delay * 2, BACKGROUND_LOGIN_RETRY_MAX_S)


@app.on_event("startup")
async def _startup():
    logger.info(
        f"Cookie Manager v4 (no-browser). cookies={COOKIES_FILE}, "
        f"accounts={ACCOUNTS_FILE}, max_accounts_per_cycle={AUTO_LOGIN_MAX_ACCOUNTS}, "
        f"log_level={LOG_LEVEL}, cookies_wait_for_login_s={COOKIES_WAIT_FOR_LOGIN_S}, "
        f"background_retry={BACKGROUND_LOGIN_RETRY_MIN_S}..{BACKGROUND_LOGIN_RETRY_MAX_S}s"
    )

    cookies = _load_cookies()
    has_auth = _has_auth_cookie(cookies)
    logger.debug(
        f"startup: cookies.json содержит {len(cookies)} записей, "
        f"DMIR_AUTH={'yes' if has_auth else 'no'}"
    )
    if not has_auth:
        logger.warning(
            f"⚠️ {CRITICAL_COOKIE} отсутствует в {COOKIES_FILE} — запускаю авто-логин."
        )

    # Стартуем фонового «хранителя кук». Он сам:
    #   • запустит первый логин если кук нет,
    #   • при провале — пробует снова с backoff,
    #   • когда куки есть — раз в минуту проверяет что они на месте.
    # Healthcheck /health вернёт 503 пока DMIR_AUTH не появится — поэтому
    # depends_on: service_healthy у парсера дождётся.
    asyncio.create_task(_background_login_keeper())


async def _ensure_login_and_wait(timeout_s: float) -> bool:
    """
    Гарантирует, что один и только один авто-логин выполнен/выполняется,
    и ждёт его окончания. Возвращает True если у нас есть DMIR_AUTH после
    ожидания, иначе False.

    Логика без race condition:
      • acquire `_login_lock` → если лок свободен — никто не логинится;
        если занят — кто-то уже логинится, мы просто дожидаемся очереди
        и попадаем в крит. секцию когда тот закончит.
      • Внутри крит. секции снова проверяем DMIR_AUTH:
          - если уже есть → отпускаем и возвращаем True (нас опередил
            предыдущий держатель лока — отлично);
          - если нет → запускаем _do_auto_login (он сам захватит лок
            повторно — но Lock не реэнтрантный, поэтому логиним ИНЛАЙНОМ
            а не через таску).
    """
    deadline = asyncio.get_event_loop().time() + timeout_s
    try:
        remaining = max(0.1, deadline - asyncio.get_event_loop().time())
        await asyncio.wait_for(_login_lock.acquire(), timeout=remaining)
    except asyncio.TimeoutError:
        logger.warning(
            f"_ensure_login_and_wait: не дождались освобождения лока за {timeout_s}s"
        )
        return _has_auth_cookie(_load_cookies())

    try:
        if _has_auth_cookie(_load_cookies()):
            return True

        # Лок наш, авто-логина никто не делает — запускаем сами.
        global _last_login_email, _last_login_at, _last_login_message
        global _login_in_progress
        _login_in_progress = True
        try:
            logger.info("🔐 _ensure_login_and_wait: starting auto-login cycle")
            ok, message, email = await _auto_login.perform_login_cycle(
                max_accounts=AUTO_LOGIN_MAX_ACCOUNTS,
                force_new_email=False,
            )
            _last_login_message = message
            if ok:
                import time as _t

                _last_login_email = email
                _last_login_at = int(_t.time())
                logger.info(f"✅ Auto-login OK as {email}: {message}")
            else:
                logger.error(f"❌ Auto-login failed: {message}")
            return ok and _has_auth_cookie(_load_cookies())
        finally:
            _login_in_progress = False
    finally:
        _login_lock.release()


# ── Routes ────────────────────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """
    Health-check: 200 если у нас есть валидный DMIR_AUTH (парсеру есть что
    раздавать). 503 — если кук нет (только что стартанули и идёт первый логин,
    или recovery после протухания). Docker compose `depends_on: service_healthy`
    использует это, чтобы НЕ запускать парсер пока куки не получены.
    """
    if _login_in_progress:
        return JSONResponse(
            status_code=503,
            content={"status": "starting", "detail": "auto-login in progress"},
        )
    if not _has_auth_cookie(_load_cookies()):
        return JSONResponse(
            status_code=503,
            content={"status": "no_auth_cookie", "detail": f"{CRITICAL_COOKIE} missing"},
        )
    return {"status": "ok"}


@app.get("/cookies")
async def get_cookies():
    """
    Возвращает куки парсеру.

    Если в момент запроса идёт авто-логин ИЛИ кук нет — НЕ падаем с 503 сразу,
    а ждём окончания (или сами триггерим) логин. Так параллельные воркеры
    парсера не валятся скопом, а спокойно дожидаются recovery и получают
    свежие куки.
    """
    cookies = _load_cookies()
    has_auth = _has_auth_cookie(cookies)
    logger.debug(
        f"/cookies: в файле {len(cookies)} cookies, "
        f"DMIR_AUTH={'yes' if has_auth else 'no'}, "
        f"login_in_progress={_login_in_progress}"
    )

    if has_auth and not _login_in_progress:
        return JSONResponse(content=cookies)

    logger.info(
        f"/cookies: DMIR_AUTH нет или идёт recovery — ждём авто-логин "
        f"(до {COOKIES_WAIT_FOR_LOGIN_S}s)"
    )
    ok = await _ensure_login_and_wait(COOKIES_WAIT_FOR_LOGIN_S)
    if not ok:
        logger.error(
            f"/cookies: после ожидания {COOKIES_WAIT_FOR_LOGIN_S}s "
            f"{CRITICAL_COOKIE} так и не появился"
        )
        raise HTTPException(
            status_code=503,
            detail=f"{CRITICAL_COOKIE} cookie missing after wait.",
            headers={"Retry-After": "30"},
        )

    cookies = _load_cookies()
    logger.debug(f"/cookies: дождались, отдаём {len(cookies)} cookies")
    return JSONResponse(content=cookies)


@app.post("/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    """
    Парсер дергает этот эндпоинт, когда видит, что куки протухли.
    Мы прогоняем цикл авто-логина (текущий аккаунт → следующие в пуле →
    свежий e-mail если все упали).
    """
    logger.info(
        f"📩 /refresh: пришёл запрос (login_in_progress={_login_in_progress})"
    )
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
    logger.info(
        f"📩 /login: пришёл запрос (force_new_email={force_new_email}, "
        f"login_in_progress={_login_in_progress})"
    )
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
    logger.debug(
        f"/check: cookies={len(cookies)}, "
        f"DMIR_AUTH={'yes' if has_auth else 'no'}, "
        f"login_in_progress={_login_in_progress}"
    )

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
