"""
test_auto_login.py — прямой тест auto-login флоу против cian.ru,
БЕЗ поднятия микросервиса.

Запускает реальные HTTP-запросы:
    1. GET https://www.cian.ru/                         (warm-up, _CIAN_GK)
    2. GET https://api.cian.ru/users/v1/is-email-registered/?email=…
    3. POST validate-login-password / quick-register
    4. Достаёт куки из jar, проверяет наличие DMIR_AUTH.

Полезно, чтобы убедиться: «эндпоинты живы, наша мок-почта пролезает,
формат ответа не поменялся».

Запуск:
    python test_auto_login.py                  # полный цикл по пулу
    python test_auto_login.py one              # только первый аккаунт
    python test_auto_login.py one foo@bar.com  # только указанный e-mail
"""

import asyncio
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

# curl_cffi на Windows ругается на ProactorEventLoop. Переключаем на Selector.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# Импортируем напрямую модуль из services/cookie_manager.
_CM_DIR = Path(__file__).parent / "services" / "cookie_manager"
sys.path.insert(0, str(_CM_DIR))

try:
    from auto_login import AutoLogin, generate_email  # noqa: E402
except ImportError as _exc:
    if "curl_cffi" in str(_exc):
        sys.stderr.write(
            "\n❌ curl_cffi не установлен. Поставь его:\n"
            "    pip install curl-cffi==0.7.4\n\n"
        )
        sys.exit(2)
    raise

# На Windows PowerShell stdout по умолчанию cp1251 — эмодзи в логах падают
# с UnicodeEncodeError. Принудительно переключаем stdout/stderr на UTF-8.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# LOG_LEVEL=DEBUG → видим каждый HTTP-шаг (URL, params, status, set-cookie, body).
_LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logging.getLogger("auto_login").setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_auto_login(seed_emails: Optional[List[str]] = None) -> AutoLogin:
    """
    Делаем экземпляр AutoLogin с временными accounts.json/cookies.json,
    чтобы тест не трогал реальные файлы.
    """
    tmpdir = Path(tempfile.mkdtemp(prefix="cian_test_"))
    accounts_file = tmpdir / "accounts.json"
    cookies_file = tmpdir / "cookies.json"

    state = {
        "current": None,
        "accounts": [
            {
                "email": e,
                "password": "12345678me",
                "registered": False,
                "blocked": False,
                "last_login_at": 0,
                "last_error": None,
                "fail_count": 0,
            }
            for e in (seed_emails or [])
        ],
    }
    accounts_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    cookies_file.write_text("[]", encoding="utf-8")

    logger.info(f"Временные файлы: {accounts_file}")
    return AutoLogin(accounts_file=accounts_file, cookies_file=cookies_file)


def _summary(auto: AutoLogin) -> None:
    accounts = auto.get_accounts()
    cookies = auto._load_cookies()  # noqa: SLF001
    has_auth = any(c.get("name") == "DMIR_AUTH" and c.get("value") for c in cookies)
    logger.info("─" * 80)
    logger.info(
        f"Итог: cookies_total={len(cookies)}, has_DMIR_AUTH={has_auth}, "
        f"current={auto.get_current_email()}"
    )
    for a in accounts:
        mark = "✅" if a.last_login_at else ("🚫" if a.blocked else "·")
        logger.info(
            f"  {mark} {a.email} fails={a.fail_count} "
            f"registered={a.registered} err={a.last_error or '-'}"
        )
    logger.info("─" * 80)


# ── Tests ────────────────────────────────────────────────────────────────────


async def test_login_one(email: Optional[str] = None) -> int:
    """Один аккаунт: idem `auto_login.login_one()`. Без ротации."""
    if email is None:
        email = generate_email()
        logger.info(f"E-mail не указан, сгенерировал: {email}")

    auto = _make_auto_login(seed_emails=[email])
    logger.info(f"=== Пробуем войти ОДИН раз: {email} ===")
    result = await auto.login_one(email, "12345678me")
    logger.info(
        f"  ok={result.ok}, registered={result.registered}, message={result.message}, "
        f"cookies={len(result.cookies)}"
    )
    if result.ok:
        # Запишем куки в файл, чтобы посмотреть формат.
        auto._save_cookies(result.cookies)  # noqa: SLF001

    _summary(auto)
    return 0 if result.ok else 1


async def test_full_cycle() -> int:
    """
    Прогон полного цикла по нескольким мок-аккаунтам.
    Если первый упал — должен пойти ко второму, третьему и т. д.
    """
    seed = [
        "arseniy.kovalenko.flat@gmail.com",
        "boguslav.tabakov.realtor@mail.ru",
        "tikhon.hramov.invest@yandex.ru",
        # один заведомо «плохой» — чтобы убедиться, что ротация работает
        generate_email(),
    ]
    auto = _make_auto_login(seed_emails=seed)

    logger.info(f"=== Полный цикл по {len(seed)} аккаунтам ===")
    for e in seed:
        logger.info(f"  • {e}")

    ok, message, used = await auto.perform_login_cycle(max_accounts=len(seed) + 2)
    logger.info(f"  → ok={ok}, used={used}, message={message}")

    _summary(auto)

    # Дополнительная проверка: после успеха в cookies.json должен лежать DMIR_AUTH.
    cookies = auto._load_cookies()  # noqa: SLF001
    has_auth = any(c.get("name") == "DMIR_AUTH" and c.get("value") for c in cookies)
    if ok and not has_auth:
        logger.error("❌ login сказал ok=True, но DMIR_AUTH в файле нет!")
        return 1
    return 0 if ok else 1


# ── Main ─────────────────────────────────────────────────────────────────────


def _print_banner() -> None:
    logger.info("=" * 80)
    logger.info("test_auto_login: реальные запросы к cian.ru")
    logger.info("=" * 80)


def main() -> int:
    args = sys.argv[1:]
    _print_banner()

    if args and args[0] == "one":
        email = args[1] if len(args) > 1 else None
        return asyncio.run(test_login_one(email))

    return asyncio.run(test_full_cycle())


if __name__ == "__main__":
    sys.exit(main())
