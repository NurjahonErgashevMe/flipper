"""
test_cookie_manager.py — тест микросервиса cookie_manager через HTTP.

Проверяет ровно то, что важно парсеру:
    1. /health отвечает.
    2. /status показывает текущий стейт пула аккаунтов.
    3. /cookies реально отдаёт куки (включая DMIR_AUTH).
       Если кук пока нет — триггерится /login и ждём, пока он завершится.

Запуск:
    docker compose up -d cookie_manager
    python test_cookie_manager.py
"""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

# Если в .env прописан docker-host — переписываем на localhost для запуска с хоста.
_DOCKER_TO_LOCAL = {
    "COOKIE_MANAGER_URL": ("cookie_manager", "localhost"),
}
for _env_key, (_docker_host, _local_host) in _DOCKER_TO_LOCAL.items():
    _val = os.environ.get(_env_key, "")
    if _docker_host in _val:
        os.environ[_env_key] = _val.replace(_docker_host, _local_host)

COOKIE_MANAGER_URL = os.getenv("COOKIE_MANAGER_URL", "http://localhost:8000").rstrip("/")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────


async def _get(client: httpx.AsyncClient, path: str) -> httpx.Response:
    url = f"{COOKIE_MANAGER_URL}{path}"
    logger.info(f"GET  {url}")
    resp = await client.get(url, timeout=10.0)
    logger.info(f"     → {resp.status_code}")
    return resp


async def _post(
    client: httpx.AsyncClient,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
) -> httpx.Response:
    url = f"{COOKIE_MANAGER_URL}{path}"
    logger.info(f"POST {url} params={params or {}}")
    resp = await client.post(url, params=params, timeout=10.0)
    logger.info(f"     → {resp.status_code}")
    return resp


def _has_dmir_auth(cookies: List[Dict[str, Any]]) -> bool:
    for c in cookies:
        if c.get("name") == "DMIR_AUTH" and (c.get("value") or "").strip():
            return True
    return False


def _critical_cookie_summary(cookies: List[Dict[str, Any]]) -> str:
    """Короткий отчёт по «важным» кукам."""
    important = ["DMIR_AUTH", "cookieUserID", "cian_ruid", "_CIAN_GK"]
    found = {c.get("name"): c for c in cookies}
    lines = []
    for name in important:
        c = found.get(name)
        if not c:
            lines.append(f"   ❌ {name}: missing")
        elif not (c.get("value") or "").strip():
            lines.append(f"   ⚠️  {name}: empty")
        else:
            value = c["value"]
            short = value if len(value) < 40 else value[:37] + "..."
            lines.append(f"   ✅ {name}: {short}")
    return "\n".join(lines)


async def _wait_login_done(
    client: httpx.AsyncClient, max_wait_sec: float = 90.0
) -> Dict[str, Any]:
    """Опрашивает /status, пока login_in_progress=False (или вышло время)."""
    deadline = asyncio.get_event_loop().time() + max_wait_sec
    last: Dict[str, Any] = {}
    while asyncio.get_event_loop().time() < deadline:
        resp = await _get(client, "/status")
        last = resp.json() if resp.status_code == 200 else {}
        if not last.get("login_in_progress", False):
            return last
        await asyncio.sleep(2.0)
    logger.warning(f"⚠️ login не завершился за {max_wait_sec}s, последнее состояние: {last}")
    return last


# ── Tests ────────────────────────────────────────────────────────────────────


async def test_health(client: httpx.AsyncClient) -> bool:
    resp = await _get(client, "/health")
    if resp.status_code != 200:
        logger.error(f"❌ /health вернул {resp.status_code}: {resp.text[:200]}")
        return False
    logger.info(f"✅ /health: {resp.json()}")
    return True


async def test_status(client: httpx.AsyncClient) -> Dict[str, Any]:
    resp = await _get(client, "/status")
    if resp.status_code != 200:
        logger.error(f"❌ /status вернул {resp.status_code}: {resp.text[:200]}")
        return {}
    data = resp.json()
    auto = data.get("auto_login", {})
    logger.info(
        f"✅ /status: cookies={data.get('cookie_count')}, "
        f"has_auth={data.get('has_auth_cookie')}, "
        f"login_in_progress={data.get('login_in_progress')}, "
        f"current_email={auto.get('current_email')}"
    )
    if auto.get("last_login_message"):
        logger.info(f"   last login: {auto.get('last_login_email')} — {auto['last_login_message']}")
    return data


async def test_accounts(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    resp = await _get(client, "/accounts")
    if resp.status_code != 200:
        logger.error(f"❌ /accounts вернул {resp.status_code}: {resp.text[:200]}")
        return []
    data = resp.json()
    accounts = data.get("accounts", [])
    blocked = [a["email"] for a in accounts if a.get("blocked")]
    logger.info(
        f"✅ /accounts: всего={len(accounts)}, current={data.get('current')}, "
        f"blocked={len(blocked)}"
    )
    if blocked:
        logger.info(f"   blocked: {', '.join(blocked)}")
    return accounts


async def test_cookies(client: httpx.AsyncClient) -> Optional[List[Dict[str, Any]]]:
    """
    Главное, что тестируем: парсер берёт куки через GET /cookies.
    Если 503 — менеджер ушёл логиниться, мы дождёмся и попробуем ещё раз.
    """
    for attempt in (1, 2):
        resp = await _get(client, "/cookies")
        if resp.status_code == 200:
            cookies = resp.json()
            if not isinstance(cookies, list):
                logger.error(f"❌ /cookies вернул не list: {type(cookies)}")
                return None
            logger.info(f"✅ /cookies: получено {len(cookies)} кук")
            logger.info(_critical_cookie_summary(cookies))
            if not _has_dmir_auth(cookies):
                logger.error("❌ DMIR_AUTH в ответе отсутствует — куки бесполезны.")
                return None
            return cookies

        if resp.status_code == 503:
            logger.warning(
                f"⚠️ /cookies вернул 503 ({resp.json().get('detail', '')}). "
                f"Похоже, идёт авто-логин. Ждём…"
            )
            await _wait_login_done(client, max_wait_sec=120.0)
            if attempt == 1:
                continue

        logger.error(f"❌ /cookies неожиданный статус {resp.status_code}: {resp.text[:200]}")
        return None
    return None


async def test_force_login(client: httpx.AsyncClient) -> bool:
    """
    Прогоняем /login и ждём, пока завершится. Проверяем, что после этого
    /cookies снова отдаёт DMIR_AUTH.
    """
    logger.info("─── force /login (без force_new_email) ───")
    resp = await _post(client, "/login")
    if resp.status_code not in (200, 202):
        logger.error(f"❌ /login вернул {resp.status_code}: {resp.text[:200]}")
        return False
    logger.info(f"   {resp.json()}")

    final_status = await _wait_login_done(client, max_wait_sec=120.0)
    auto = final_status.get("auto_login", {}) if final_status else {}
    msg = auto.get("last_login_message")
    email = auto.get("last_login_email")
    if final_status.get("has_auth_cookie"):
        logger.info(f"✅ авто-логин закончен: {email} — {msg}")
        return True

    logger.error(f"❌ авто-логин закончен, но DMIR_AUTH так и не появился. message={msg}")
    return False


# ── Main ─────────────────────────────────────────────────────────────────────


async def run_all():
    logger.info("=" * 80)
    logger.info(f"Тест cookie_manager: {COOKIE_MANAGER_URL}")
    logger.info("=" * 80)

    async with httpx.AsyncClient(trust_env=False) as client:
        results: Dict[str, bool] = {}

        try:
            results["health"] = await test_health(client)
        except Exception as exc:
            logger.error(f"❌ /health недоступен: {exc}")
            logger.error(
                f"   Сервис не запущен? Попробуй: docker compose up -d cookie_manager"
            )
            return 1

        await test_status(client)
        await test_accounts(client)

        cookies = await test_cookies(client)
        results["cookies"] = cookies is not None and _has_dmir_auth(cookies)

        # На всякий случай пробуем ещё один цикл логина, чтобы убедиться,
        # что повторный логин работает (релогин в текущий же аккаунт).
        results["relogin"] = await test_force_login(client)

        # Финальная сводка
        logger.info("=" * 80)
        logger.info("ИТОГ:")
        for name, ok in results.items():
            mark = "✅" if ok else "❌"
            logger.info(f"  {mark} {name}: {'OK' if ok else 'FAIL'}")
        logger.info("=" * 80)

        return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(run_all()))
