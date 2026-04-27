"""
auto_login.py — автоматический логин/регистрация на cian.ru через публичные API
с пулом аккаунтов и ротацией при ошибке.

Поток (см. services/accaountant/*.bash и image*.png):
    1. GET https://api.cian.ru/users/v1/is-email-registered/?email=<E>
       → {"isRegistered": bool}
    2a. Если зарегистрирован → POST https://api.cian.ru/authentication/v1/validate-login-password/
        с JSON {"login": E, "password": "12345678me"}.
    2b. Если НЕ зарегистрирован → POST https://www.cian.ru/api/users/v1/quick-register/
        с form-data email=&password=12345678me&isProfessional=false&...
    3. На 200 сервер ставит Set-Cookie (DMIR_AUTH, cookieUserID, cian_ruid, DeviceId_*).
       Эти куки httpx сохраняет в jar — мы их сериализуем в Playwright-формат
       (тот же, что использует services/cookie_manager/cookies.json).

Если перебрали все аккаунты и ни один не залогинился — вернёт ошибку и
вызывающий код решит, что делать (например, упасть в ручной NoVNC).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from http.cookiejar import Cookie, CookieJar
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_PASSWORD = "12345678me"

CIAN_HOME = "https://www.cian.ru/"
URL_IS_REGISTERED = "https://api.cian.ru/users/v1/is-email-registered/"
URL_VALIDATE_LOGIN = "https://api.cian.ru/authentication/v1/validate-login-password/"
URL_QUICK_REGISTER = "https://www.cian.ru/api/users/v1/quick-register/"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
)

BASE_HEADERS: Dict[str, str] = {
    "accept": "*/*",
    "accept-language": "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.cian.ru",
    "priority": "u=1, i",
    "referer": "https://www.cian.ru/",
    "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "user-agent": USER_AGENT,
}

# Кук, наличие которой будем считать признаком успешного логина.
AUTH_COOKIE_NAME = "DMIR_AUTH"

# Сколько раз подряд аккаунт может упасть, прежде чем мы пометим его blocked.
MAX_FAIL_BEFORE_BLOCK = 3

# Email-генератор: имитируем «настоящих» риелторов / инвесторов с обычной почтой.
# Берём редкие, но реальные русские имена + редкие фамилии, добавляем тематический
# контекст (realty/estate/invest/flat) и/или год — чтобы получить адрес, который
# почти наверняка ещё не зарегистрирован на cian, но смотрится живым человеком.

_FIRST_NAMES = [
    "arseniy", "vsevolod", "boguslav", "efim", "tikhon",
    "nikifor", "ostap", "evlampiy", "serafim", "emelyan",
    "akim", "iosif", "kuzma", "luka", "rostislav",
    "savva", "valeriy", "yaroslav", "anatoly", "georgy",
    "marianna", "radmila", "glafira", "agafya", "vladlena",
    "marfa", "polina", "zinaida", "serafima", "evlampiya",
]

_LAST_NAMES = [
    "kovalenko", "podgornov", "shumilov", "zubarev", "cherny",
    "tabakov", "ostrovsky", "voloshin", "chestnov", "hramov",
    "suslov", "pereverzev", "zharikov", "vorobyov", "blagov",
    "kuzmin", "tarasov", "lebedev", "morozov", "novikov",
    "fedorov", "smirnov", "popov", "volkov", "sokolov",
]

_CONTEXT_WORDS = [
    "realty", "estate", "invest", "flat", "home",
    "homes", "realtor", "property", "spb", "msc",
]

# Домены — публичные провайдеры (вес большего количества mail.ru/gmail.com)
# плюс несколько «корпоративных» доменов в стиле риелторских контор.
_EMAIL_DOMAINS = [
    "gmail.com", "gmail.com", "gmail.com",
    "mail.ru", "mail.ru", "mail.ru",
    "yandex.ru", "yandex.ru",
    "list.ru", "bk.ru", "inbox.ru", "rambler.ru", "ya.ru",
    "spb-realty.ru", "homefinder.pro", "flatinvest.io",
]


def generate_email() -> str:
    """
    Случайный e-mail в духе живого пользователя, например:
        `arseniy.kovalenko.realty@gmail.com`
        `glafira_voloshin_1989@mail.ru`
        `tikhon.hramov92@yandex.ru`
    """
    first = random.choice(_FIRST_NAMES)
    last = random.choice(_LAST_NAMES)

    # На фамилиях с -ova/-aya для женских имён не заморачиваемся —
    # многие так и пишут «по-английски» в нике.
    parts: List[str] = [first, last]

    # Контекст — в ~40% случаев.
    if random.random() < 0.4:
        parts.append(random.choice(_CONTEXT_WORDS))

    # Цифры — в ~60% случаев. Год рождения / счастливое число.
    if random.random() < 0.6:
        if random.random() < 0.4:
            parts.append(str(random.randint(1980, 2003)))
        else:
            parts.append(str(random.randint(2, 99)))

    sep = random.choice([".", "_"])
    name = sep.join(parts)
    domain = random.choice(_EMAIL_DOMAINS)
    return f"{name}@{domain}"


# ── Account model ────────────────────────────────────────────────────────────


@dataclass
class Account:
    email: str
    password: str = DEFAULT_PASSWORD
    registered: bool = False
    blocked: bool = False
    last_login_at: int = 0
    last_error: Optional[str] = None
    fail_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "email": self.email,
            "password": self.password,
            "registered": self.registered,
            "blocked": self.blocked,
            "last_login_at": self.last_login_at,
            "last_error": self.last_error,
            "fail_count": self.fail_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Account":
        return cls(
            email=data["email"],
            password=data.get("password", DEFAULT_PASSWORD),
            registered=bool(data.get("registered", False)),
            blocked=bool(data.get("blocked", False)),
            last_login_at=int(data.get("last_login_at") or 0),
            last_error=data.get("last_error"),
            fail_count=int(data.get("fail_count") or 0),
        )


class LoginResult:
    """Результат одной попытки логина в один аккаунт."""

    __slots__ = ("ok", "email", "registered", "message", "cookies")

    def __init__(
        self,
        ok: bool,
        email: str,
        registered: bool,
        message: str,
        cookies: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.ok = ok
        self.email = email
        self.registered = registered
        self.message = message
        self.cookies = cookies or []


# ── Cookie helpers ───────────────────────────────────────────────────────────


def _cookie_to_playwright(c: Cookie) -> Dict[str, Any]:
    """`http.cookiejar.Cookie` → dict в формате Playwright (как в cookies.json)."""

    def _nonstd(attr: str) -> Optional[str]:
        rest = getattr(c, "_rest", {}) or {}
        for k, v in rest.items():
            if str(k).lower() == attr.lower():
                return v
        return None

    same_site = _nonstd("SameSite") or "Lax"
    http_only = _nonstd("HttpOnly") is not None or bool(getattr(c, "rfc2109", False) is False and _nonstd("HttpOnly"))

    return {
        "name": c.name,
        "value": c.value or "",
        "domain": c.domain or "",
        "path": c.path or "/",
        "expires": c.expires if c.expires else -1,
        "httpOnly": http_only,
        "secure": bool(c.secure),
        "sameSite": same_site,
    }


def jar_to_list(jar: CookieJar) -> List[Dict[str, Any]]:
    return [_cookie_to_playwright(c) for c in jar]


def merge_cookie_lists(
    existing: List[Dict[str, Any]],
    new: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Сливаем существующий список и новый: новые перетирают по ключу (name, domain, path)."""
    by_key: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for c in existing:
        key = (c.get("name", ""), c.get("domain", ""), c.get("path", "/"))
        by_key[key] = c
    for c in new:
        key = (c.get("name", ""), c.get("domain", ""), c.get("path", "/"))
        by_key[key] = c
    return list(by_key.values())


# ── AutoLogin ────────────────────────────────────────────────────────────────


class AutoLogin:
    """
    Управляет пулом аккаунтов в `accounts.json` и выполняет цикл логина.
    Сохраняет полученные куки в `cookies.json`.
    """

    def __init__(
        self,
        accounts_file: str | Path,
        cookies_file: str | Path,
        password: str = DEFAULT_PASSWORD,
        request_timeout: float = 20.0,
    ) -> None:
        self.accounts_file = Path(accounts_file)
        self.cookies_file = Path(cookies_file)
        self.password = password
        self.request_timeout = request_timeout
        self._lock = asyncio.Lock()

    # ── persistence ──────────────────────────────────────────────────────────

    def _load_state(self) -> Dict[str, Any]:
        if not self.accounts_file.exists():
            return {"current": None, "accounts": []}
        try:
            data = json.loads(self.accounts_file.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {"current": None, "accounts": []}
            data.setdefault("current", None)
            data.setdefault("accounts", [])
            return data
        except Exception as exc:
            logger.error(f"Не удалось прочитать {self.accounts_file}: {exc}")
            return {"current": None, "accounts": []}

    def _save_state(self, state: Dict[str, Any]) -> None:
        self.accounts_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.accounts_file.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self.accounts_file)

    def _load_cookies(self) -> List[Dict[str, Any]]:
        if not self.cookies_file.exists():
            return []
        try:
            data = json.loads(self.cookies_file.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception as exc:
            logger.error(f"Не удалось прочитать {self.cookies_file}: {exc}")
            return []

    def _save_cookies(self, cookies: List[Dict[str, Any]]) -> None:
        self.cookies_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.cookies_file.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(cookies, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(self.cookies_file)

    # ── account selection ────────────────────────────────────────────────────

    def get_accounts(self) -> List[Account]:
        return [Account.from_dict(a) for a in self._load_state().get("accounts", [])]

    def get_current_email(self) -> Optional[str]:
        return self._load_state().get("current")

    def _pick_account(self, state: Dict[str, Any], skip: List[str]) -> Optional[Account]:
        """Выбираем аккаунт: сначала current, затем первый незаблокированный, не из skip."""
        accounts: List[Account] = [Account.from_dict(a) for a in state.get("accounts", [])]
        current = state.get("current")

        # 1. current, если он есть и не в skip и не blocked
        if current and current not in skip:
            for acc in accounts:
                if acc.email == current and not acc.blocked:
                    return acc

        # 2. любой не-blocked, не в skip
        for acc in accounts:
            if not acc.blocked and acc.email not in skip:
                return acc

        return None

    def _add_new_account(self, state: Dict[str, Any]) -> Account:
        """Создаём свежий e-mail и кладём его в пул."""
        existing_emails = {a.get("email") for a in state.get("accounts", [])}
        for _ in range(20):
            email = generate_email()
            if email not in existing_emails:
                break
        else:
            email = generate_email()  # ну и пусть, маловероятно

        acc = Account(email=email, password=self.password)
        state.setdefault("accounts", []).append(acc.to_dict())
        return acc

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    async def _warmup(self, client: httpx.AsyncClient) -> None:
        """Заходим на главную, чтобы получить _CIAN_GK и анти-бот куки."""
        try:
            resp = await client.get(
                CIAN_HOME,
                headers={
                    "user-agent": USER_AGENT,
                    "accept": (
                        "text/html,application/xhtml+xml,application/xml;q=0.9,"
                        "image/avif,image/webp,*/*;q=0.8"
                    ),
                    "accept-language": "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7",
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "none",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1",
                },
                timeout=self.request_timeout,
            )
            logger.debug(f"warm-up GET {CIAN_HOME} → {resp.status_code}, cookies={len(list(client.cookies.jar))}")
        except Exception as exc:
            logger.warning(f"warm-up не удался ({exc}); идём дальше — куки могут быть частичными")

    async def _is_email_registered(self, client: httpx.AsyncClient, email: str) -> bool:
        headers = BASE_HEADERS.copy()
        headers["sec-fetch-site"] = "same-site"
        resp = await client.get(
            URL_IS_REGISTERED,
            headers=headers,
            params={"email": email},
            timeout=self.request_timeout,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"is-email-registered HTTP {resp.status_code}: {resp.text[:200]}"
            )
        try:
            payload = resp.json()
        except Exception as exc:
            raise RuntimeError(f"is-email-registered невалидный JSON: {exc}") from exc
        return bool(payload.get("isRegistered"))

    async def _validate_login(
        self, client: httpx.AsyncClient, email: str, password: str
    ) -> httpx.Response:
        headers = BASE_HEADERS.copy()
        headers["content-type"] = "application/json"
        headers["sec-fetch-site"] = "same-site"
        return await client.post(
            URL_VALIDATE_LOGIN,
            headers=headers,
            json={"login": email, "password": password},
            timeout=self.request_timeout,
        )

    async def _quick_register(
        self, client: httpx.AsyncClient, email: str, password: str
    ) -> httpx.Response:
        headers = BASE_HEADERS.copy()
        headers["content-type"] = "application/x-www-form-urlencoded"
        headers["sec-fetch-site"] = "same-origin"
        return await client.post(
            URL_QUICK_REGISTER,
            headers=headers,
            data={
                "email": email,
                "password": password,
                "isProfessional": "false",
                "enableSubscription": "true",
                "isAcceptLicence": "true",
            },
            timeout=self.request_timeout,
        )

    @staticmethod
    def _login_response_ok(resp: httpx.Response) -> Tuple[bool, str]:
        """Анализ ответа на validate-login / quick-register."""
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
        # Сервер на 200 иногда возвращает payload с ошибкой.
        try:
            payload = resp.json()
        except Exception:
            return True, "OK"
        if isinstance(payload, dict):
            status = (payload.get("status") or "").lower()
            if status in {"error", "failed", "fail"}:
                return False, f"server status=error: {payload}"
            err = payload.get("errors") or payload.get("error")
            if err:
                return False, f"server error payload: {err}"
        return True, "OK"

    # ── high-level login of a single account ────────────────────────────────

    async def login_one(self, email: str, password: str) -> LoginResult:
        """
        Полный цикл для одного e-mail: warm-up → проверка регистрации → логин/регистрация.
        Возвращает LoginResult c cookies (если успех) либо c message (если ошибка).
        """
        async with httpx.AsyncClient(
            timeout=self.request_timeout,
            follow_redirects=True,
            trust_env=False,
            headers={"user-agent": USER_AGENT},
        ) as client:
            await self._warmup(client)

            # 1. is-email-registered
            try:
                registered = await self._is_email_registered(client, email)
            except Exception as exc:
                return LoginResult(False, email, False, f"is-email-registered: {exc}")

            # 2. validate-login или quick-register
            try:
                if registered:
                    logger.info(f"[{email}] уже зарегистрирован → validate-login")
                    resp = await self._validate_login(client, email, password)
                else:
                    logger.info(f"[{email}] не зарегистрирован → quick-register")
                    resp = await self._quick_register(client, email, password)
            except Exception as exc:
                return LoginResult(False, email, registered, f"login request: {exc}")

            ok, why = self._login_response_ok(resp)
            if not ok:
                return LoginResult(False, email, registered, f"login {why}")

            # 3. Куки выдали? Проверяем DMIR_AUTH.
            cookie_list = jar_to_list(client.cookies.jar)
            has_auth = any(
                c.get("name") == AUTH_COOKIE_NAME and c.get("value") for c in cookie_list
            )
            if not has_auth:
                return LoginResult(
                    False,
                    email,
                    registered,
                    f"login HTTP 200, но {AUTH_COOKIE_NAME} не выдан. "
                    f"Получено кук: {len(cookie_list)}",
                )

            return LoginResult(
                True,
                email,
                registered,
                f"OK ({'login' if registered else 'registered'})",
                cookies=cookie_list,
            )

    # ── high-level login cycle with rotation ─────────────────────────────────

    async def perform_login_cycle(
        self,
        max_accounts: int = 5,
        force_new_email: bool = False,
    ) -> Tuple[bool, str, Optional[str]]:
        """
        Перебираем аккаунты, пока один не залогинится.
        Если все упали — генерируем новые e-mail'ы и пробуем (но не больше `max_accounts` всего).

        Returns:
            (ok, message, used_email)
        """
        async with self._lock:
            tried: List[str] = []

            for attempt in range(1, max_accounts + 1):
                state = self._load_state()

                if force_new_email and attempt == 1:
                    acc = self._add_new_account(state)
                else:
                    acc = self._pick_account(state, skip=tried)
                    if acc is None:
                        acc = self._add_new_account(state)

                # фиксируем как «пробуем сейчас»
                state["current"] = acc.email
                self._save_state(state)

                logger.info(
                    f"🔐 [{attempt}/{max_accounts}] пробуем войти как {acc.email} "
                    f"(registered={acc.registered}, fail_count={acc.fail_count})"
                )
                tried.append(acc.email)

                result = await self.login_one(acc.email, acc.password)

                # Перечитываем state (на случай гонок), чтобы обновить именно этот акк.
                state = self._load_state()
                accounts_list = state.get("accounts", [])
                target_idx: Optional[int] = None
                for i, a in enumerate(accounts_list):
                    if a.get("email") == acc.email:
                        target_idx = i
                        break
                if target_idx is None:
                    accounts_list.append(acc.to_dict())
                    target_idx = len(accounts_list) - 1
                    state["accounts"] = accounts_list
                acc_dict = accounts_list[target_idx]

                if result.ok:
                    # Сохраняем куки (мерджим с тем, что уже было).
                    existing = self._load_cookies()
                    merged = merge_cookie_lists(existing, result.cookies)
                    self._save_cookies(merged)

                    acc_dict["registered"] = True
                    acc_dict["blocked"] = False
                    acc_dict["last_login_at"] = int(time.time())
                    acc_dict["last_error"] = None
                    acc_dict["fail_count"] = 0
                    state["current"] = acc.email
                    self._save_state(state)

                    logger.info(
                        f"✅ Логин успешен: {acc.email} ({result.message}). "
                        f"Сохранено кук: {len(merged)} (DMIR_AUTH: yes)"
                    )
                    return True, result.message, acc.email

                # ❌ ошибка в этом аккаунте
                logger.warning(f"❌ {acc.email}: {result.message}")
                acc_dict["last_error"] = result.message
                acc_dict["fail_count"] = int(acc_dict.get("fail_count") or 0) + 1
                if acc_dict["fail_count"] >= MAX_FAIL_BEFORE_BLOCK:
                    acc_dict["blocked"] = True
                    logger.warning(
                        f"🚫 {acc.email} помечен blocked (fail_count={acc_dict['fail_count']})"
                    )
                # снимаем current, чтобы в следующей итерации не выбрать тот же
                if state.get("current") == acc.email:
                    state["current"] = None
                self._save_state(state)

                # лёгкая пауза, чтобы не долбить cian
                await asyncio.sleep(1.5)

            return (
                False,
                f"all {len(tried)} login attempts failed: {tried}",
                None,
            )
