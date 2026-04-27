"""
auto_login.py — автоматический логин/регистрация на cian.ru через публичные API
с пулом аккаунтов и ротацией при ошибке.

ВАЖНО: используем curl_cffi с impersonate="chrome" — обычный httpx ловит cian-captcha
из-за нестандартного TLS-фингерпринта. curl_cffi мимикрирует Chrome на уровне TLS/JA3
(тот же приём использует основной парсер services/parser_cian).

ВАЖНО-2: cian smart-банит «слишком пустых» клиентов — если в jar нет полного набора
аналитических + анти-бот кук (_ga, _gcl_au, _ym_*, tmr_*, sopr_*, **domain_sid**,
**_spx**, **tmr_detect**), сервер возвращает 200 на quick-register/validate-login,
но в logOnInfo всё равно выдаёт «мёртвый» token. Поэтому мы пред-засеваем в
_seed_cookies() полный набор реалистичных значений.

ВАЖНО-3: validate-login и quick-register НЕ выдают DMIR_AUTH в Set-Cookie напрямую.
Они возвращают одноразовый logOnInfo.token. Этот token надо обменять на DMIR_AUTH
GET-запросом на api-субдомен:
    GET https://api.cian.ru/authentication/v1/logon/?token=<token>&login=<email>
В ответе HTTP 200, body 'true' и Set-Cookie: DMIR_AUTH=…
ВНИМАНИЕ: `logOnUrl` в ответе сервера обманчив — он указывает на www.cian.ru,
но тот эндпоинт возвращает 405/500. Реальный обменник — на api.cian.ru.

Поток:
    0. seed cookies + GET https://www.cian.ru/  (warm-up, забираем _yasc)
    1. GET https://api.cian.ru/users/v1/is-email-registered/?email=<E>
       → {"isRegistered": bool}
    2a. Если зарегистрирован → POST validate-login-password (JSON)
    2b. Если НЕ зарегистрирован → POST quick-register (form-data)
    3. Из ответа достаём logOnInfo[0].token и делаем GET-обмен на logon endpoint.
    4. В Set-Cookie прилетает DMIR_AUTH — сохраняем в jar.

Если перебрали все аккаунты и ни один не залогинился — возвращаем ошибку.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass
from http.cookiejar import Cookie, CookieJar
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from curl_cffi.requests import AsyncSession

logger = logging.getLogger(__name__)


class RateLimitedError(Exception):
    """cian вернул 429 — IP зарейтлимичен. Прерываем цикл."""


def _atomic_write_json(path: Path, data: Any) -> None:
    """
    Пишет JSON в файл с двумя стратегиями:
      1. tmp + os.replace (атомарно) — для обычных файлов.
      2. Fallback: write напрямую — нужен для docker bind-mount файлов
         (volume `./host/file.json:/container/file.json`). В таком случае
         `os.replace` падает с OSError(EBUSY/EXDEV), потому что docker
         монтирует inode исходного файла, и заменить его внутри контейнера
         нельзя — можно только переписать содержимое.

    Без этого fallback'а в реальном деплое cookies.json никогда не
    обновляется и парсер не получает свежих кук.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)
        return
    except OSError as exc:
        logger.warning(
            f"_atomic_write_json: rename {tmp} → {path} упал ({exc!r}), "
            "переключаюсь на in-place запись (видимо bind-mount файла)"
        )
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    # Fallback: пишем напрямую в смонтированный файл (truncate + write).
    with path.open("w", encoding="utf-8") as f:
        f.write(payload)
        f.flush()


# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_PASSWORD = "12345678me"

CIAN_HOME = "https://www.cian.ru/"
URL_IS_REGISTERED = "https://api.cian.ru/users/v1/is-email-registered/"
URL_VALIDATE_LOGIN = "https://api.cian.ru/authentication/v1/validate-login-password/"
URL_QUICK_REGISTER = "https://www.cian.ru/api/users/v1/quick-register/"
# Реальный «обмен logOnInfo.token на DMIR_AUTH» live на api-субдомене.
# Сервер в logOnInfo возвращает обманчивый `//www.cian.ru/api/users/logon/`,
# но тот эндпоинт всегда отвечает 405/500. Реальный обменник —
#   GET /authentication/v1/logon/?token=<token>&login=<email>
# и в Set-Cookie приходит DMIR_AUTH.
URL_LOGON = "https://api.cian.ru/authentication/v1/logon/"

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


# ── Логи: вспомогательные функции ────────────────────────────────────────────


def _mask(value: Any, head: int = 6, tail: int = 4) -> str:
    """Маскируем длинные секреты (token, password, DMIR_AUTH cookie) для DEBUG."""
    s = str(value or "")
    if len(s) <= head + tail + 3:
        return s
    return f"{s[:head]}…{s[-tail:]}({len(s)}b)"


def _mask_proxy(proxy_url: str) -> str:
    """
    Из 'http://user:pass@host:port' → 'host:port'. Не светим креды в логи.
    """
    try:
        from urllib.parse import urlparse

        u = urlparse(proxy_url)
        return f"{u.hostname}:{u.port}" if u.hostname else proxy_url
    except Exception:
        return "<proxy>"


def _jar_names(client_or_jar) -> List[str]:
    """Имена всех cookies в jar — для краткого DEBUG-отображения."""
    try:
        jar = getattr(client_or_jar, "cookies", None)
        if jar is not None:
            jar = jar.jar
        else:
            jar = client_or_jar
        return sorted(c.name for c in jar)
    except Exception:
        return []


def _log_response_brief(label: str, resp, body_limit: int = 200) -> None:
    """Логируем ответ HTTP в одной строке (DEBUG)."""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    try:
        body = (resp.text or "")
    except Exception:
        body = ""
    snippet = body[:body_limit].replace("\n", " ")
    if len(body) > body_limit:
        snippet += f"…(+{len(body) - body_limit}b)"
    sc = []
    try:
        h = resp.headers
        if hasattr(h, "get_list"):
            sc = h.get_list("set-cookie") or []
        elif h.get("set-cookie"):
            sc = [h.get("set-cookie")]
    except Exception:
        pass
    sc_brief = [(c.split(";", 1)[0]) for c in sc] if sc else []
    logger.debug(
        f"  ← {label}: HTTP {resp.status_code} "
        f"set-cookie={sc_brief or '∅'} body={snippet!r}"
    )

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
        proxies: Optional[List[str]] = None,
    ) -> None:
        """
        Args:
            proxies: Список прокси-URL вида `http://user:pass@host:port`.
                На каждый цикл логина рандомно выбирается один прокси.
                Нужно когда «голый» IP сервера зарейтлимичен/забанен на cian.
                Пустой список / None → запросы напрямую (поведение по умолчанию).
        """
        self.accounts_file = Path(accounts_file)
        self.cookies_file = Path(cookies_file)
        self.password = password
        self.request_timeout = request_timeout
        self.proxies: List[str] = list(proxies or [])
        self._lock = asyncio.Lock()
        if self.proxies:
            logger.info(
                f"AutoLogin: подключено прокси из пула: {len(self.proxies)} шт."
            )
        else:
            logger.info("AutoLogin: прокси не заданы, запросы напрямую")

    def _pick_proxy(self) -> Optional[str]:
        """Случайный прокси из пула (или None если пул пуст)."""
        if not self.proxies:
            return None
        return random.choice(self.proxies)

    # ── persistence ──────────────────────────────────────────────────────────

    def _load_state(self) -> Dict[str, Any]:
        if not self.accounts_file.exists():
            logger.debug(f"_load_state: {self.accounts_file} не существует")
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
        _atomic_write_json(self.accounts_file, state)
        logger.debug(
            f"_save_state: current={state.get('current')}, "
            f"accounts={len(state.get('accounts', []))} → {self.accounts_file}"
        )

    def _load_cookies(self) -> List[Dict[str, Any]]:
        if not self.cookies_file.exists():
            logger.debug(f"_load_cookies: {self.cookies_file} не существует")
            return []
        try:
            data = json.loads(self.cookies_file.read_text(encoding="utf-8"))
            cookies = data if isinstance(data, list) else []
            logger.debug(f"_load_cookies: прочитано {len(cookies)} из {self.cookies_file}")
            return cookies
        except Exception as exc:
            logger.error(f"Не удалось прочитать {self.cookies_file}: {exc}")
            return []

    def _save_cookies(self, cookies: List[Dict[str, Any]]) -> None:
        _atomic_write_json(self.cookies_file, cookies)
        names = sorted({c.get("name") for c in cookies if c.get("name")})
        has_auth = any(c.get("name") == AUTH_COOKIE_NAME for c in cookies)
        logger.debug(
            f"_save_cookies: записано {len(cookies)} cookies "
            f"(DMIR_AUTH={'yes' if has_auth else 'no'}) → {self.cookies_file}; "
            f"names={names}"
        )

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
                    logger.debug(f"_pick_account: использую current={acc.email}")
                    return acc

        # 2. любой не-blocked, не в skip
        for acc in accounts:
            if not acc.blocked and acc.email not in skip:
                logger.debug(
                    f"_pick_account: первый незаблокированный={acc.email} "
                    f"(всего в пуле={len(accounts)}, skip={skip})"
                )
                return acc

        logger.debug(
            f"_pick_account: подходящих нет "
            f"(всего={len(accounts)}, blocked={sum(1 for a in accounts if a.blocked)}, "
            f"skip={skip})"
        )
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

    # ── HTTP helpers (curl_cffi с impersonate=chrome) ────────────────────────

    @staticmethod
    def _seed_cookies(client: AsyncSession) -> None:
        """
        Пред-засеваем полный набор «аналитических» и анти-бот кук,
        как у живого юзера. Снимок взят из реального quick-register.bash:
        там пользователь — свежий аноним (без DMIR_AUTH), и сразу после
        quick-register сервер выдаёт DMIR_AUTH прямо в Set-Cookie.
        То есть никакого «второго logon-шага» нет — нужны все нужные cookies.

        Критически важные (из quick-register.bash):
          • domain_sid  — anti-bot session id (формат `<base64>:<timestamp_ms>`)
          • _spx        — base64-JSON с UUID (anti-bot session protection)
          • tmr_detect  — Mail.ru detector (`0|<timestamp_ms>`)

        Без этих трёх cian возвращает 200, но MOLCHA НЕ выдаёт DMIR_AUTH.
        """
        try:
            import base64

            now = int(time.time())
            ms = now * 1000 + random.randint(0, 999)
            ga_random = random.randint(10**9, 10**10 - 1)
            ym_uid = f"{now}{random.randint(10**5, 10**6 - 1)}"
            tmr_lvid = uuid.uuid4().hex  # 32-hex
            sopr_session = uuid.uuid4().hex[:16]  # 16-hex

            # domain_sid: 22-символьный base64-id, потом ':', потом timestamp_ms.
            domain_sid_id = (
                base64.urlsafe_b64encode(uuid.uuid4().bytes)
                .decode()
                .rstrip("=")[:22]
            )
            domain_sid = f"{domain_sid_id}:{ms}"

            # _spx: base64(JSON). Структура взята из quick-register.bash.
            spx_payload = json.dumps(
                {
                    "id": str(uuid.uuid4()),
                    "source": "",
                    "fixed": {"stack": [0]},
                },
                separators=(",", ":"),
            )
            spx = base64.b64encode(spx_payload.encode()).decode()

            cookies = {
                "_CIAN_GK": str(uuid.uuid4()),
                "_gcl_au": f"1.1.{ga_random}.{now}",
                "_ga": f"GA1.1.{ga_random}.{now}",
                "_ga_3369S417EL": (
                    f"GS2.1.s{now}$o1$g0$t{now}$j60$l0$h0"
                ),
                "_ym_uid": ym_uid,
                "_ym_d": str(now),
                "_ym_isad": "2",
                "_ym_visorc": "b",
                "tmr_lvid": tmr_lvid,
                "tmr_lvidTS": str(ms),
                "tmr_detect": f"0|{ms}",
                "uxs_uid": str(uuid.uuid4()),
                "uxfb_usertype": "searcher",
                "cookie_agreement_accepted": "1",
                "login_mro_popup": "1",
                "sopr_utm": "%7B%22utm_source%22%3A+%22direct%22%2C+%22utm_medium%22%3A+%22None%22%7D",
                "sopr_session": sopr_session,
                "domain_sid": domain_sid,
                "_spx": spx,
            }
            for name, value in cookies.items():
                client.cookies.set(name, value, domain=".cian.ru")
            logger.debug(
                f"seed_cookies: посеяли {len(cookies)} cookies → "
                f"{sorted(cookies.keys())}"
            )
        except Exception as exc:
            logger.debug(f"seed_cookies: не удалось проставить ({exc})")

    async def _warmup(self, client: AsyncSession) -> None:
        """Заходим на главную, чтобы получить _yasc и прочие анти-бот куки."""
        try:
            before = set(_jar_names(client))
            logger.debug(f"  → warm-up GET {CIAN_HOME}  (jar before={len(before)})")
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
                allow_redirects=True,
            )
            final_url = str(resp.url or "")
            after = set(_jar_names(client))
            new = sorted(after - before)
            logger.info(
                f"warm-up: {resp.status_code}, final_url={final_url}, jar={len(after)}"
            )
            logger.debug(
                f"  ← warm-up: HTTP {resp.status_code} new_cookies={new or '∅'}"
            )
            if "captcha" in final_url:
                logger.warning(
                    f"⚠️ warm-up redirected to CAPTCHA ({final_url}). "
                    f"Логин почти наверняка не пройдёт — нужен другой IP / прокси."
                )
        except Exception as exc:
            logger.warning(f"warm-up не удался ({exc}); пробуем без него")

    async def _is_email_registered(self, client: AsyncSession, email: str) -> bool:
        headers = BASE_HEADERS.copy()
        headers["sec-fetch-site"] = "same-site"
        logger.debug(f"  → is-email-registered GET {URL_IS_REGISTERED}?email={email}")
        resp = await client.get(
            URL_IS_REGISTERED,
            headers=headers,
            params={"email": email},
            timeout=self.request_timeout,
        )
        _log_response_brief("is-email-registered", resp)
        if resp.status_code == 429:
            # IP зарейтлимичен — нет смысла дёргать дальше: только усугубим.
            raise RateLimitedError(
                f"is-email-registered HTTP 429: rate-limit, нужно подождать"
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"is-email-registered HTTP {resp.status_code}: {resp.text[:200]}"
            )
        try:
            payload = resp.json()
        except Exception as exc:
            raise RuntimeError(f"is-email-registered невалидный JSON: {exc}") from exc
        is_reg = bool(payload.get("isRegistered"))
        logger.debug(f"     isRegistered={is_reg} (payload={payload})")
        return is_reg

    async def _validate_login(
        self, client: AsyncSession, email: str, password: str
    ):
        headers = BASE_HEADERS.copy()
        headers["content-type"] = "application/json"
        headers["sec-fetch-site"] = "same-site"
        logger.debug(
            f"  → validate-login POST {URL_VALIDATE_LOGIN} "
            f"json={{login:{email}, password:{_mask(password)}}}"
        )
        before = set(_jar_names(client))
        resp = await client.post(
            URL_VALIDATE_LOGIN,
            headers=headers,
            json={"login": email, "password": password},
            timeout=self.request_timeout,
        )
        _log_response_brief("validate-login", resp, body_limit=300)
        new = sorted(set(_jar_names(client)) - before)
        if new:
            logger.debug(f"     new cookies after validate-login: {new}")
        return resp

    async def _quick_register(
        self, client: AsyncSession, email: str, password: str
    ):
        headers = BASE_HEADERS.copy()
        headers["content-type"] = "application/x-www-form-urlencoded"
        headers["sec-fetch-site"] = "same-origin"
        logger.debug(
            f"  → quick-register POST {URL_QUICK_REGISTER} "
            f"form={{email:{email}, password:{_mask(password)}, "
            f"isProfessional:false, enableSubscription:true, isAcceptLicence:true}}"
        )
        before = set(_jar_names(client))
        resp = await client.post(
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
        _log_response_brief("quick-register", resp, body_limit=300)
        new = sorted(set(_jar_names(client)) - before)
        if new:
            logger.debug(f"     new cookies after quick-register: {new}")
        return resp

    @staticmethod
    def _extract_logon_info(resp) -> Optional[Dict[str, str]]:
        """
        Из ответа validate-login / quick-register достаём первый logOnInfo:
            { "logOnUrl": "https://www.cian.ru/api/users/logon/", "token": "..." }
        Сервер отдаёт `//www.cian.ru/...` (protocol-relative) — добавляем https.
        """
        try:
            payload = resp.json()
        except Exception:
            logger.debug("extract_logon_info: ответ не JSON")
            return None
        if not isinstance(payload, dict):
            return None
        logon_info = payload.get("logOnInfo") or []
        if not isinstance(logon_info, list) or not logon_info:
            logger.debug(f"extract_logon_info: нет logOnInfo в payload (keys={list(payload.keys())})")
            return None
        first = logon_info[0]
        if not isinstance(first, dict):
            return None
        url = first.get("logOnUrl")
        token = first.get("token")
        if not url or not token:
            logger.debug(f"extract_logon_info: пусто url={url!r} token={_mask(token)}")
            return None
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = "https://www.cian.ru" + url
        logger.debug(
            f"extract_logon_info: url={url} token={_mask(token)}"
        )
        return {"logOnUrl": url, "token": token}

    async def _perform_logon(
        self, client: AsyncSession, token: str, login: str) -> None:
        """
        Меняем одноразовый logOnInfo.token на cookie DMIR_AUTH.

        Реальный обменник:
            GET https://api.cian.ru/authentication/v1/logon/
                ?token=<token>&login=<email>
        В Set-Cookie прилетает DMIR_AUTH (httponly, домен `.cian.ru`).
        Body — просто строка `true`.

        Token одноразовый — делаем строго ОДНУ попытку.
        """
        headers = BASE_HEADERS.copy()
        headers["sec-fetch-site"] = "same-site"
        headers["accept"] = "*/*"

        logger.debug(
            f"  → logon GET {URL_LOGON}?token={_mask(token)}&login={login}"
        )
        before = {c.name for c in client.cookies.jar}
        resp = await client.get(
            URL_LOGON,
            headers=headers,
            params={"token": token, "login": login},
            timeout=self.request_timeout,
        )
        _log_response_brief("logon", resp)
        after = {c.name for c in client.cookies.jar}
        new_cookies = sorted(after - before)
        body = ""
        try:
            body = (resp.text or "")[:200]
        except Exception:
            pass
        # DMIR_AUTH мы не показываем целиком (это секрет) — только prefix.
        if AUTH_COOKIE_NAME in new_cookies:
            for c in client.cookies.jar:
                if c.name == AUTH_COOKIE_NAME:
                    logger.debug(
                        f"     получен {AUTH_COOKIE_NAME}={_mask(c.value, 8, 6)}"
                    )
                    break
        logger.info(
            f"logon GET: HTTP {resp.status_code}, "
            f"new cookies={new_cookies}, body={body!r}"
        )

    @staticmethod
    def _login_response_ok(resp) -> Tuple[bool, str]:
        """Анализ ответа на validate-login / quick-register."""
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
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
        Полный цикл для одного e-mail: warm-up → is-email-registered →
        validate-login | quick-register. curl_cffi с impersonate="chrome"
        мимикрирует TLS-фингерпринт настоящего Chrome, иначе cian редиректит
        на /cian-captcha/.
        """
        proxy = self._pick_proxy()
        # Логируем без логина/пароля — `_mask_proxy` оставляет только host:port.
        proxy_label = _mask_proxy(proxy) if proxy else "direct"
        logger.debug(
            f"login_one[{email}]: старт (password={_mask(password)}, proxy={proxy_label})"
        )

        session_kwargs: Dict[str, Any] = {
            "timeout": self.request_timeout,
            "impersonate": "chrome",
            "headers": {"user-agent": USER_AGENT},
        }
        if proxy:
            # curl_cffi.AsyncSession принимает proxies={"http": ..., "https": ...}
            session_kwargs["proxies"] = {"http": proxy, "https": proxy}

        async with AsyncSession(**session_kwargs) as client:
            self._seed_cookies(client)
            await self._warmup(client)

            # 1. is-email-registered
            try:
                registered = await self._is_email_registered(client, email)
            except RateLimitedError:
                # Пробрасываем выше — perform_login_cycle прервёт цикл.
                raise
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

            # 3. DMIR_AUTH в Set-Cookie прямо от quick-register / validate-login
            #    приходит крайне редко — обычно сервер отдаёт одноразовый
            #    `logOnInfo.token`, который надо обменять на DMIR_AUTH через
            #    отдельный POST application/x-www-form-urlencoded на logOnUrl
            #    (//www.cian.ru/api/users/logon/).
            cookie_list = jar_to_list(client.cookies.jar)
            has_auth = any(
                c.get("name") == AUTH_COOKIE_NAME and c.get("value") for c in cookie_list
            )

            if not has_auth:
                logon_info = self._extract_logon_info(resp)
                if logon_info:
                    logger.debug(
                        f"[{email}] DMIR_AUTH ещё нет → нужен logon-обмен"
                    )
                    try:
                        await self._perform_logon(
                            client, token=logon_info["token"], login=email
                        )
                    except Exception as exc:
                        logger.warning(f"[{email}] logon: {exc}")
                    cookie_list = jar_to_list(client.cookies.jar)
                    has_auth = any(
                        c.get("name") == AUTH_COOKIE_NAME and c.get("value")
                        for c in cookie_list
                    )
                else:
                    logger.debug(
                        f"[{email}] DMIR_AUTH нет и logOnInfo тоже нет — провал"
                    )

            if not has_auth:
                names = sorted({c.get("name") for c in cookie_list if c.get("name")})
                body = ""
                try:
                    body = (resp.text or "")[:300]
                except Exception:
                    pass
                return LoginResult(
                    False,
                    email,
                    registered,
                    (
                        f"login HTTP 200 + logon не выдал {AUTH_COOKIE_NAME}. "
                        f"jar={len(cookie_list)} {names}. Body: {body!r}"
                    ),
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
        logger.debug(
            f"perform_login_cycle: max_accounts={max_accounts}, "
            f"force_new_email={force_new_email}"
        )
        async with self._lock:
            tried: List[str] = []

            for attempt in range(1, max_accounts + 1):
                state = self._load_state()
                logger.debug(
                    f"  pool: current={state.get('current')}, "
                    f"total={len(state.get('accounts', []))}, "
                    f"blocked={sum(1 for a in state.get('accounts', []) if a.get('blocked'))}, "
                    f"tried_so_far={tried}"
                )

                if force_new_email and attempt == 1:
                    acc = self._add_new_account(state)
                    logger.debug(f"  → выбран свежесгенерированный e-mail: {acc.email}")
                else:
                    acc = self._pick_account(state, skip=tried)
                    if acc is None:
                        acc = self._add_new_account(state)
                        logger.debug(
                            f"  → пул исчерпан, сгенерирован новый: {acc.email}"
                        )
                    else:
                        logger.debug(
                            f"  → выбран из пула: {acc.email} "
                            f"(registered={acc.registered}, fail_count={acc.fail_count})"
                        )

                # фиксируем как «пробуем сейчас»
                state["current"] = acc.email
                self._save_state(state)

                logger.info(
                    f"🔐 [{attempt}/{max_accounts}] пробуем войти как {acc.email} "
                    f"(registered={acc.registered}, fail_count={acc.fail_count})"
                )
                tried.append(acc.email)

                try:
                    result = await self.login_one(acc.email, acc.password)
                except RateLimitedError as exc:
                    # IP зарейтлимичен: бессмысленно пытаться следующих аккаунтов.
                    logger.warning(
                        f"🛑 cian вернул 429 на {acc.email}: {exc}. "
                        f"Прерываю цикл, нужно подождать (~5-15 минут) и попробовать снова."
                    )
                    return False, f"rate-limited by cian: {exc}", None

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
                    logger.debug(
                        f"  cookies merge: existing={len(existing)} + "
                        f"new={len(result.cookies)} → saved={len(merged)} "
                        f"в {self.cookies_file}"
                    )

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

                # Пауза между аккаунтами: cian быстро ловит 429-rate-limit,
                # если запросы идут чаще чем ~1/5s. Делаем 6-10s + джиттер.
                await asyncio.sleep(random.uniform(6.0, 10.0))

            return (
                False,
                f"all {len(tried)} login attempts failed: {tried}",
                None,
            )
