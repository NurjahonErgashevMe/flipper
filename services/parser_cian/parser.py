"""
services.parser_cian.parser - AdParser for self-hosted Firecrawl integration

Парсер объявлений недвижимости с использованием self-hosted Firecrawl API.
Данные поступают из трёх источников:
  1. Firecrawl JSON (AI-экстракция через GLM-4.7-Flash / OpenRouter)
  2. rawHtml страницы (creationDate из embedded JSON скриптов)
  3. Cian Statistics API (days_in_exposition, total_views, unique_views)
"""

import os
import logging
import httpx
import asyncio
import re
import time
from typing import Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from services.parser_cian.models import ParsedAdData

logger = logging.getLogger(__name__)


def _sanitize_building_type(raw: Any) -> str:
    """
    Только реальный «Тип дома» (материал/конструкция). Не «Строительная серия»
    (Индивидуальный проект, II-49, П-44Т и т.д.) — иначе пустая строка.
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    low = s.lower()
    if "индивидуальн" in low and "проект" in low:
        return ""
    if low in ("нет информации", "нет данных", "-", "—"):
        return ""
    if re.match(r"^[IVX]+\s*[-–]\s*\d", s, re.I):
        return ""
    if re.match(r"^\s*П-?\d", s, re.I):
        return ""
    if re.match(r"^\s*\d+\s*[-–/]\s*\d+", s):
        return ""
    return s


def _parse_price_history_date_str(d_str: Any):
    """Дата для сортировки: datetime.date или None."""
    if d_str is None:
        return None
    if isinstance(d_str, (int, float)):
        return None
    if not isinstance(d_str, str):
        return None
    d_str = d_str.strip().strip(".")
    if not d_str:
        return None
    # ISO внутри длинной строки
    m_iso = re.search(r"(20\d{2})-(\d{2})-(\d{2})", d_str)
    if m_iso:
        try:
            return datetime(
                int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3))
            ).date()
        except Exception:
            pass
    try:
        return datetime.strptime(d_str, "%Y-%m-%d").date()
    except Exception:
        pass
    try:
        return datetime.strptime(d_str, "%d.%m.%Y").date()
    except Exception:
        pass
    try:
        # «8 апр 2026», «8 апр. 2026 г.», «30 марта 2026»
        months = {
            "янв": 1,
            "фев": 2,
            "мар": 3,
            "апр": 4,
            "май": 5,
            "мая": 5,
            "июн": 6,
            "июл": 7,
            "авг": 8,
            "сен": 9,
            "окт": 10,
            "ноя": 11,
            "дек": 12,
            # длинные префиксы (января, марта, апреля…)
            "январ": 1,
            "феврал": 2,
            "март": 3,
            "апрел": 4,
            "август": 8,
            "сентябр": 9,
            "октябр": 10,
            "ноябр": 11,
            "декабр": 12,
        }
        parts = d_str.replace(" г.", "").replace("г.", "").split()
        if len(parts) >= 3:
            day = int(re.sub(r"\D", "", parts[0]))
            mon_word = re.sub(r"[^а-яёa-z]", "", parts[1].lower())
            mon_raw = mon_word[:3] if len(mon_word) >= 3 else mon_word
            year = int(re.sub(r"\D", "", parts[2]))
            mon = months.get(mon_raw)
            if mon is None and len(mon_word) >= 5:
                for k, v in months.items():
                    if len(k) >= 4 and mon_word.startswith(k[:4]):
                        mon = v
                        break
            if mon:
                return datetime(year, mon, day).date()
    except Exception:
        pass
    return None


def _is_cian_markdown_ui_line(line: str) -> bool:
    """Строки из markdown-карточки Циана, не являющиеся текстом описания."""
    t = line.strip()
    if not t:
        return False
    low = t.lower()
    # Блок «О доме», ипотечные/агентские виджеты — не описание продавца
    if low in (
        "строительная серия",
        "количество лифтов",
        "тип перекрытий",
        "о подъезде",
        "спросите умного помощника",
        "получите экспертное мнение о жилье",
        "спросить",
        "ипотечный калькулятор",
        "стоимость недвижимости",
        "ит-ипотека",
        "срок кредита",
        "загружаем предложения от застройщика и банков",
        "зимой будет тепло",
        "показать телефон",
        "следить за изменением цены",
        "один запрос в 9 банков",
        "планировка этой квартиры",
    ):
        return True
    if "риелтор" in low and "суперагент" in low:
        return True
    if re.match(r"^ставки\s+от\s+\d", low):
        return True
    if low in ("на карте", "сравнить", "пожаловаться"):
        return True
    if low.startswith("скачать в"):
        return True
    if low in ("3d-тур", "3d тур"):
        return True
    if re.fullmatch(r"\d+\s*фото", t, flags=re.I):
        return True
    if re.fullmatch(r"\*+\s*\d+\s*фото", t, flags=re.I):
        return True
    if re.fullmatch(r"планировка", low):
        return True
    # Маркер метро: «*   Новослободская — 10 мин.»
    if re.match(r"^\*+\s+.+\d+\s*мин", t):
        return True
    # Короткая строка «Станция — N мин» / «Станция N мин» (без длинного текста)
    if len(t) <= 72:
        if re.match(r"^[А-ЯЁ0-9«»\w\s\.,\-]+—\s*\d{1,4}\s*мин", t):
            return True
        if re.match(
            r"^[А-ЯЁ][а-яё\-A-Za-z0-9\s\.\-«»]{1,40}\s+\d{1,2}\s*мин\.?\s*$", t
        ):
            return True
    if re.fullmatch(r"\[[^\]]+\]\([^)]+\)", t):
        return True
    # Дубли параметров из верхней части markdown (без пробела после метки — как на Циан)
    if re.match(r"^Общая\s+площадь\s*[\d.,]", t, re.I):
        return True
    if re.match(r"^Жилая\s+площадь\s*[\d.,]", t, re.I):
        return True
    if re.match(r"^Площадь\s+кухни\s*[\d.,]", t, re.I):
        return True
    if re.match(r"^Этаж\s*\d", t, re.I):
        return True
    if re.match(r"^Год\s+постройки\s*\d", t, re.I):
        return True
    # «Тип дома» как отдельная метка в блоке О доме (значение на следующей строке)
    if low == "тип дома":
        return True
    return False


def _description_looks_like_sidebar_ui(text: str) -> bool:
    """Типичный мусор: блок О доме + ипотека/агент, не связный текст продавца."""
    if not text or len(text.strip()) < 40:
        return False
    low = text.lower()
    markers = (
        "строительная серия",
        "количество лифтов",
        "спросите умного помощника",
        "ипотечный калькулятор",
        "суперагент",
        "загружаем предложения от застройщика",
        "один запрос в 9 банков",
        "ставки от ",
    )
    hits = sum(1 for m in markers if m in low)
    if hits >= 2:
        return True
    if text.lstrip().lower().startswith("строительная серия"):
        return True
    if "ипотечный калькулятор" in low and "показать телефон" in low:
        return True
    return False


def _extract_seller_text_before_sidebar(s: str) -> str:
    """
    Берёт текст до типичного начала блока «О доме» / виджетов (если они идут после описания).
    """
    # Начало блока дома/сайдбара в markdown
    stop = re.search(
        r"(?:^|\n)\s*(?:Строительная\s+серия|Количество\s+лифтов|"
        r"Спросите\s+умного\s+помощника|Ипотечный\s+калькулятор)\s*(?:\n|$)",
        s,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    if stop:
        head = s[: stop.start()].strip()
        if len(head) >= 12:
            return head
    return ""


def _pick_best_description_paragraph(s: str) -> str:
    """
    Если в ответе ИИ смешаны короткое описание и длинный UI — выбираем параграф без маркеров сайдбара.
    """
    parts = re.split(r"\n\s*\n+", s)
    best = ""
    best_score = -1
    bad_sub = (
        "строительная серия",
        "количество лифтов",
        "ипотечный калькулятор",
        "спросите умного",
        "суперагент",
        "загружаем предложения",
        "один запрос в 9 банков",
    )
    for p in parts:
        p = p.strip()
        if len(p) < 12:
            continue
        low = p.lower()
        if any(b in low for b in bad_sub):
            continue
        if _description_looks_like_sidebar_ui(p):
            continue
        # Короткие связные фразы продавца — предпочтительнее длинного шума
        score = min(len(p), 800) - 50 * sum(low.count(b) for b in bad_sub)
        if "\n" in p and p.count("\n") > 8:
            score -= 200
        if score > best_score:
            best_score = score
            best = p
    return best


def _clean_cian_description(raw: str) -> str:
    """
    Убирает из description «сырой» markdown карточки (хлебные крошки, списки метро, кнопки).
    Оставляет основной текст: обычно всё после блока «… Год постройки YYYY».
    """
    if not raw or not str(raw).strip():
        return ""
    original = str(raw).replace("\r\n", "\n").replace("\r", "\n").strip()
    s = original

    # Иногда Циан пишет «за объект внесли аванс/задаток» отдельной строкой
    # в верхней части карточки (до параметров/«Год постройки»). Сохраняем эту строку,
    # иначе она может пропасть при обрезке description.
    avans_hint = ""
    for pat in (
        r"За\s+объект\s+уже\s+внесли\s+(?:аванс|задаток)",
        r"За\s+квартир[ау]\s+внес[её]н\s+(?:аванс|задаток)",
        r"Внес[её]н\s+(?:аванс|задаток)",
        r"Принят\s+задаток",
        r"Получен\s+аванс",
        r"Обеспечительн\w*\s+плат[её]ж\s+(?:внес[её]н|получен)",
        r"Квартир[ау]\s+забронирован[ао]",
        r"Объект\s+забронирован",
    ):
        m = re.search(pat, s, flags=re.IGNORECASE)
        if not m:
            continue
        ls = s.rfind("\n", 0, m.start())
        ls = 0 if ls < 0 else ls + 1
        le = s.find("\n", m.end())
        le = len(s) if le < 0 else le
        snippet = s[ls:le].strip().strip("-•* \t")
        snippet = re.sub(r"\s+", " ", snippet).strip()
        if snippet:
            avans_hint = snippet
            break

    # 1) Текст продавца часто идёт ДО блока «Строительная серия» / виджетов — сначала отрезаем сайдбар
    head_before_sidebar = _extract_seller_text_before_sidebar(s)
    if head_before_sidebar and not _description_looks_like_sidebar_ui(head_before_sidebar):
        s = head_before_sidebar

    # 2) После «Год постройки YYYY» иногда идёт короткое описание, а после него — блок «О доме».
    #    Берём фрагмент после ПЕРВОГО «Год постройки», обрезая по маркеру сайдбара; не берём «хвост» после
    #    последнего вхождения года — там часто только мусор из «О доме».
    year_first = re.search(r"Год\s+постройки\s*\d{4}", s, flags=re.IGNORECASE)
    if year_first:
        chunk = s[year_first.end() :].lstrip()
        stop_sb = re.search(
            r"(?:^|\n)\s*(?:Строительная\s+серия|Количество\s+лифтов|"
            r"Спросите\s+умного\s+помощника|Ипотечный\s+калькулятор)\b",
            chunk,
            flags=re.IGNORECASE | re.MULTILINE,
        )
        if stop_sb:
            chunk = chunk[: stop_sb.start()].strip()
        if 20 <= len(chunk) <= 15000 and not _description_looks_like_sidebar_ui(chunk):
            s = chunk
        else:
            year_last = list(re.finditer(r"Год\s+постройки\s*\d{4}", s, flags=re.IGNORECASE))
            if year_last:
                tail = s[year_last[-1].end() :].lstrip()
                if len(tail) >= 20 and not _description_looks_like_sidebar_ui(tail):
                    s = tail
    else:
        # 3) Якоря начала продающего текста (если года в тексте нет)
        for pat in (
            r"Код\s+объекта\s*:\s*\d+",
            r"За\s+объект\s+уже\s+внесли",
            r"За\s+квартиру\s+внесен",
            r"Внесен\s+аванс",
        ):
            m = re.search(pat, s, flags=re.IGNORECASE)
            if m and len(s[m.start() :].strip()) >= 30:
                s = s[m.start() :].strip()
                break

    if _description_looks_like_sidebar_ui(s):
        s = original
        alt = _pick_best_description_paragraph(original)
        if alt:
            s = alt
        else:
            head2 = _extract_seller_text_before_sidebar(original)
            if head2:
                s = head2

    lines_out: list[str] = []
    for line in s.split("\n"):
        t = line.strip()
        if not t:
            if lines_out and lines_out[-1] != "":
                lines_out.append("")
            continue
        if _is_cian_markdown_ui_line(t):
            continue
        lines_out.append(line.rstrip())

    out = "\n".join(lines_out).strip()
    out = re.sub(r"\n{3,}", "\n\n", out)

    if avans_hint and avans_hint.lower() not in out.lower():
        out = f"{avans_hint}\n\n{out}" if out else avans_hint
    return out


# System prompt для AI-экстракции
SYSTEM_PROMPT = (
    "Экстрактор объявлений Cian.ru: заполни поля по схеме из markdown; нет данных — null. "
    "ВАЖНО: housing_type — это 'Тип жилья' из раздела 'О квартире' (Вторичка или Новостройка). "
    "building_type — ТОЛЬКО строка 'Тип дома' в блоке 'О доме' (Панельный, Кирпичный, Монолитный…). "
    "Если строки 'Тип дома' на странице нет — building_type = null. "
    "НИКОГДА не подставляй сюда 'Строительную серию' (Индивидуальный проект, II-49, П-44Т и т.п.). "
    "renovation — тип ремонта из 'О квартире'. district — район, okrug — ЦАО, ЮВАО и т.д. "
    "description — только связный текст описания продавца (может быть коротким, 1–2 предложения). "
    "НИКОГДА не копируй блок «О доме» (Строительная серия, П-44, лифты, перекрытия), "
    "виджеты «Ипотечный калькулятор», «Спросите умного помощника», карточки риелторов/«Суперагент», "
    "«Показать телефон», хлебные крошки, списки метро (* … мин), «На карте», «Скачать», «Пожаловаться», "
    "дубли строк «Общая площадь / Этаж / Год постройки» (эти поля уже в других полях схемы). "
    "Просмотры: «X просмотров, Y за сегодня» — X→total_views, Y→unique_views. "
    "is_active: true, если карточка доступна. "
    "has_avans_deposit: true если на странице есть признаки, что за объект внесён "
    "аванс/задаток/обеспечительный платёж (покупатель уже забронировал квартиру). "
    "Это может быть не только в описании/заголовке продавца, но и в бейдже/уведомлении "
    "в верхней части карточки (например: «За объект уже внесли аванс»). "
    "Просто наличие слова «аванс» в описании НЕ достаточно — нужен смысловой контекст "
    "подтверждения внесения платежа. false если таких сведений нет. "
    "price_history: строки таблицы 'История цены' в хронологическом порядке (сначала старые события), "
    "для каждой строки: date в формате YYYY-MM-DD и price; сумму изменения и тип пересчитает бэкенд."
)

# Теги для исключения из HTML перед конвертацией в markdown
EXCLUDE_TAGS = [
    "svg",
    "img",
    "script",
    "style",
    "footer",
    "header",
    # "[data-name='CardSectionNew']",
    "[data-name='OfferCardPageLayoutFooter']",
    "[id='adfox-stretch-banner']",
]


class AdParser:
    """
    Парсер объявлений Cian через self-hosted Firecrawl API v2.

    Источники данных:
    - Firecrawl JSON: основные поля через AI-экстракцию (GLM-4.7-Flash)
    - rawHtml: creationDate для запроса статистики
    - Cian API: days_in_exposition, total_views, unique_views (точные данные)

    Особенности:
    - Использует Cookie Manager для получения валидных куков
    - Асинхронные запросы через httpx
    """

    def __init__(
        self,
        cookie_manager_url: str = "http://cookie_manager:8000",
        firecrawl_base_url: Optional[str] = None,
        firecrawl_api_key: Optional[str] = None,
        cookies_cache_ttl_sec: float = 90.0,
    ):
        """
        Args:
            cookie_manager_url: URL микросервиса управления куками
                Для локальной разработки: http://localhost:8000
                Для Docker: http://cookie_manager:8000
            firecrawl_base_url: База self-hosted Firecrawl (из settings / FIRECRAWL_BASE_URL)
            firecrawl_api_key: Ключ API (из settings / FIRECRAWL_API_KEY)
            cookies_cache_ttl_sec: Кэш строки Cookie на N секунд (меньше дублей GET /cookies)
        """
        self.api_key = (firecrawl_api_key or os.getenv("FIRECRAWL_API_KEY", "test-key")).strip()

        base = (firecrawl_base_url or os.getenv("FIRECRAWL_BASE_URL", "http://localhost:3002")).rstrip("/")
        self.firecrawl_api_url = f"{base}/v2/scrape"

        self.cookie_manager_url = cookie_manager_url.rstrip("/")
        self._cookies_lock = asyncio.Lock()
        self._cookies_cache: Optional[str] = None
        self._cookies_cache_mono: float = 0.0
        self._cookies_cache_ttl_sec = cookies_cache_ttl_sec

        logger.info(
            f"AdParser initialized with self-hosted Firecrawl: {self.firecrawl_api_url}"
        )
        logger.info(f"Cookie Manager URL: {self.cookie_manager_url}")

    async def _fetch_cookies_from_manager(self) -> str:
        """
        Один запрос GET /cookies (без кэша). cookie_manager обычно в NO_PROXY.

        Таймаут 120s — потому что cookie_manager при идущем авто-логине
        ДЕРЖИТ соединение и ждёт окончания (до ~90s), чтобы все воркеры
        дождались свежих кук, а не падали кучей с 503 «retry later».
        """
        try:
            async with httpx.AsyncClient(timeout=120.0, trust_env=False) as client:
                resp = await client.get(f"{self.cookie_manager_url}/cookies")

                if resp.status_code == 503:
                    logger.warning("Cookie Manager: Recovery in progress")
                    return ""

                if resp.status_code == 200:
                    cookies = resp.json()
                    cookie_str = "; ".join(
                        [f"{c['name']}={c['value']}" for c in cookies]
                    )
                    logger.debug(f"Fetched {len(cookies)} cookies from manager")
                    return cookie_str
                logger.warning(f"Cookie manager returned {resp.status_code}")
                return ""
        except Exception as e:
            logger.error(f"Failed to fetch cookies from manager: {e}")
            return ""

    async def _get_cookies(self) -> str:
        """
        Куки из cookie_manager с коротким кэшем: при concurrency воркеры не делают
        десятки параллельных GET /cookies на каждое объявление.
        """
        async with self._cookies_lock:
            now = time.monotonic()
            if (
                self._cookies_cache is not None
                and (now - self._cookies_cache_mono) < self._cookies_cache_ttl_sec
            ):
                return self._cookies_cache

            cookie_str = await self._fetch_cookies_from_manager()
            if cookie_str:
                self._cookies_cache = cookie_str
                self._cookies_cache_mono = time.monotonic()
            else:
                self._cookies_cache = None
            return cookie_str

    async def _check_authentication(self, cookie_str: str, attempts: int = 3) -> bool:
        """
        Проверяет авторизацию на Cian.ru.
        Делает 3 попытки с паузой 5 сек.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Cookie": cookie_str,
        }

        for attempt in range(attempts):
            try:
                # follow_redirects=True: иначе часто приходит 302 с пустым body и проверка ломается
                async with httpx.AsyncClient(
                    timeout=10.0, follow_redirects=True, trust_env=False
                ) as client:
                    resp = await client.get(
                        "https://my.cian.ru/profile", headers=headers
                    )

                final_url = str(resp.url or "")
                if "authenticate" in final_url or "/login" in final_url.lower():
                    logger.warning(
                        f"Attempt {attempt + 1}: попали на страницу логина ({final_url[:80]})"
                    )
                    if attempt < attempts - 1:
                        await asyncio.sleep(5)
                    continue

                if resp.status_code != 200:
                    logger.warning(
                        f"Attempt {attempt + 1}: profile HTTP {resp.status_code}"
                    )
                    if attempt < attempts - 1:
                        await asyncio.sleep(5)
                    continue

                html = resp.text or ""
                if '"isAuthenticated":true' in html:
                    logger.info(
                        f"✅ isAuthenticated: true (attempt {attempt + 1})"
                    )
                    return True
                if '"isAuthenticated":false' in html:
                    logger.warning(
                        f"❌ isAuthenticated: false (attempt {attempt + 1})"
                    )
                else:
                    logger.warning(
                        f"Attempt {attempt + 1}: в HTML нет isAuthenticated (len={len(html)})"
                    )
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt + 1} error: {e}")

            if attempt < attempts - 1:
                logger.info("⏳ Waiting 5 seconds before retry...")
                await asyncio.sleep(5)

        logger.error("❌❌❌ All authentication checks failed")

        return False

    async def _check_and_trigger_recovery(self):
        """Проверяет статус Cookie Manager и запускает recovery если нужно."""
        try:
            async with httpx.AsyncClient(timeout=5.0, trust_env=False) as client:
                resp = await client.post(f"{self.cookie_manager_url}/check")
                if resp.status_code == 200:
                    data = resp.json()
                    if not data.get("valid"):
                        logger.warning(
                            "🚨 Cookie Manager confirmed: cookies are INVALID"
                        )
                    else:
                        logger.info("✅ Cookie Manager says cookies are valid")
        except Exception as e:
            logger.error(f"Failed to trigger recovery check: {e}")

    def _extract_creation_date_from_html(self, html: str) -> Optional[str]:
        """
        Извлекает creationDate из rawHtml страницы.
        Нужен для запроса статистики через Cian API.

        Returns:
            Дата создания в формате YYYY-MM-DD или None
        """
        try:
            patterns = [
                r'"creationDate"\s*:\s*"(\d{4}-\d{2}-\d{2})T[^"]*"',
                r'"creationDate"\s*:\s*"(\d{4}-\d{2}-\d{2})"',
            ]
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    creation_date_str = match.group(1)
                    logger.info(f"✅ Найдена creationDate: {creation_date_str}")
                    return creation_date_str

            logger.warning("⚠️ creationDate не найден в HTML")
            return None
        except Exception as e:
            logger.error(f"❌ Ошибка извлечения creationDate: {e}")
            return None

    async def _get_statistics(
        self, cian_id: str, creation_date: str, cookies_str: str
    ) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Получает точную статистику из Cian Statistics API.
        Данные из AI-экстракции (total_views, unique_views) будут перезаписаны
        более точными данными из этого метода.

        Returns:
            Tuple: (days_in_exposition, total_views, unique_views)
        """
        url = (
            f"https://api.cian.ru/offer-card/v1/get-offer-card-statistic/"
            f"?offerCreationDate={creation_date}&offerId={cian_id}"
        )

        headers = {
            "accept": "*/*",
            "accept-language": "en,ru;q=0.9,en-US;q=0.8",
            "origin": "https://www.cian.ru",
            "referer": "https://www.cian.ru/",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "Cookie": cookies_str,
        }

        logger.info(f"📊 Статистика для {cian_id} (creation_date: {creation_date})...")

        try:
            response = None
            for attempt in range(3):
                try:
                    async with httpx.AsyncClient(timeout=15.0, trust_env=False) as client:
                        response = await client.get(url, headers=headers)
                    break
                except (httpx.ConnectError, httpx.ReadTimeout, OSError) as e:
                    if attempt < 2:
                        logger.warning(
                            f"⚠️ Статистика api.cian.ru попытка {attempt + 1}/3: {e}, повтор..."
                        )
                        await asyncio.sleep(1.0 * (attempt + 1))
                        continue
                    logger.error(f"❌ Статистика: сеть после 3 попыток: {e}")
                    return None, None, None

            if response is None:
                return None, None, None

            if response.status_code != 200:
                logger.warning(f"⚠️ Статистика API вернул {response.status_code}")
                return None, None, None

            data = response.json()

            def _parse_number(s: str) -> Optional[int]:
                if not s:
                    return None
                m = re.search(r"(\d+[\d\s\u00A0]*)", s)
                if not m:
                    return None
                num = re.sub(r"[\s\u00A0]", "", m.group(1))
                try:
                    return int(num)
                except Exception:
                    return None

            def _parse_date(s: str) -> Optional[datetime]:
                if not s:
                    return None
                m = re.search(r"(\d{2}\.\d{2}\.\d{4})", s)
                if m:
                    try:
                        return datetime.strptime(m.group(1), "%d.%m.%Y")
                    except Exception:
                        pass
                m2 = re.search(r"(\d{4}-\d{2}-\d{2})", s)
                if m2:
                    try:
                        return datetime.strptime(m2.group(1), "%Y-%m-%d")
                    except Exception:
                        pass
                return None

            daily = data.get("daily", {}) or {}
            daily_views = daily.get("dailyViews") or []

            parsed_entries = []
            for entry in daily_views:
                date_raw = entry.get("date")
                views = entry.get("views")
                dt = None
                if isinstance(date_raw, str):
                    try:
                        dt = datetime.strptime(date_raw, "%Y-%m-%d")
                    except Exception:
                        dt = _parse_date(date_raw)
                if dt and isinstance(views, int):
                    parsed_entries.append((dt.date(), views))

            publish_date_from_daily = None
            days_in_exposition = None
            unique_views = None
            total_views = None

            root_total_views_str = data.get("totalViews")
            root_total, root_total_date = None, None
            if isinstance(root_total_views_str, str):
                root_total = _parse_number(root_total_views_str)
                dt = _parse_date(root_total_views_str)
                if dt:
                    root_total_date = dt.date()

            daily_total_views_str = daily.get("totalViews")
            daily_total = None
            if isinstance(daily_total_views_str, str):
                daily_total = _parse_number(daily_total_views_str)

            if parsed_entries:
                dates = [d for d, _ in parsed_entries]
                earliest = min(dates)
                latest = max(dates)
                publish_date_from_daily = earliest
                days_in_exposition = max(0, (latest - earliest).days)
                for d, v in parsed_entries:
                    if d == latest:
                        unique_views = v
                        break
                if root_total is not None:
                    total_views = root_total
                elif daily_total is not None:
                    total_views = daily_total
                else:
                    total_views = sum(v for _, v in parsed_entries)
            else:
                if root_total is not None:
                    total_views = root_total
                elif daily_total is not None:
                    total_views = daily_total

            publish_date = None
            if root_total_date:
                publish_date = root_total_date
                ref_date = (
                    max(d for d, _ in parsed_entries)
                    if parsed_entries
                    else datetime.utcnow().date()
                )
                days_in_exposition = max(0, (ref_date - publish_date).days)
            elif publish_date_from_daily:
                publish_date = publish_date_from_daily

            logger.info(
                f"✅ Статистика: {days_in_exposition} дней, {total_views} просмотров, {unique_views} сегодня"
            )
            return days_in_exposition, total_views, unique_views

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return None, None, None

    def _get_schema(self) -> Dict[str, Any]:
        """
        JSON Schema для AI-экстракции через Firecrawl.
        Использует вложенные объекты address и floor_info.
        """
        return {
            "type": "object",
            "properties": {
                "cian_id": {
                    "type": "string",
                    "description": "ID объявления из URL (число в конце /sale/flat/XXXXXX/)",
                },
                "price": {"type": "integer", "description": "Цена в рублях"},
                "price_per_m2": {"type": "integer", "description": "Цена за м²"},
                "title": {"type": "string", "description": "Заголовок объявления"},
                "description": {
                    "type": "string",
                    "description": "Только текст описания продавца: без markdown-навигации, хлебных крошек, "
                    "списков метро, кнопок «Скачать». Без дубля строк параметров (площадь/этаж/год).",
                },
                "address": {
                    "type": "object",
                    "properties": {
                        "full": {"type": "string", "description": "Полный адрес объекта как указан на странице"},
                        "district": {
                            "type": "string",
                            "description": "Район Москвы (например: Лефортово, Хамовники, Южнопортовый, Останкинский, Котловка). Берётся из хлебных крошек или адресной строки после 'р-н'.",
                        },
                        "metro_station": {
                            "type": "string",
                            "description": "Ближайшая станция метро (только название, без слова 'метро')",
                        },
                        "okrug": {
                            "type": "string",
                            "description": "Административный округ Москвы — аббревиатура: ЦАО, ЮВАО, СВАО, ЗАО, ЮЗАО, САО, ВАО, СЗАО, ЮАО и т.д.",
                        },
                    },
                },
                "area": {"type": "number", "description": "Общая площадь в м²"},
                "rooms": {"type": "integer", "description": "Количество комнат"},
                "housing_type": {
                    "type": "string",
                    "description": "Тип жилья из раздела 'О квартире' — только 'Вторичка' или 'Новостройка'. НЕ путать с типом дома.",
                },
                "building_type": {
                    "type": ["string", "null"],
                    "description": "Только поле 'Тип дома' в блоке 'О доме' (Панельный, Кирпичный, Монолитный…). "
                    "Если строки 'Тип дома' нет — null. НЕ брать 'Строительную серию' (Индивидуальный проект, II-49, П-44Т).",
                },
                "floor_info": {
                    "type": "object",
                    "properties": {
                        "current": {"type": "integer", "description": "Этаж квартиры"},
                        "all": {
                            "type": "integer",
                            "description": "Всего этажей в доме",
                        },
                    },
                },
                "construction_year": {
                    "type": "integer",
                    "description": "Год постройки дома",
                },
                "renovation": {
                    "type": "string",
                    "description": "Тип ремонта из раздела 'О квартире' — например: Евроремонт, Косметический, Дизайнерский, Без ремонта. НЕ путать с типом дома.",
                },
                "metro_walk_time": {
                    "type": "integer",
                    "description": "Минут пешком до БЛИЖАЙШЕЙ станции метро",
                },
                "total_views": {
                    "type": "integer",
                    "description": "Всего просмотров — число ДО запятой в строке 'X просмотров, Y за сегодня'",
                },
                "unique_views": {
                    "type": "integer",
                    "description": "Просмотров сегодня — число ПОСЛЕ запятой в строке 'X просмотров, Y за сегодня'",
                },
                "is_active": {
                    "type": "boolean",
                    "description": "Активно ли объявление. False если 'снято с публикации', 'снято с продажи'.",
                },
                "has_avans_deposit": {
                    "type": "boolean",
                    "description": (
                        "True если на странице есть признак, что за данный объект внесён аванс "
                        "(задаток, обеспечительный платёж). Важно: это может быть отдельный бейдж/"
                        "уведомление в верхней части карточки (например: «За объект уже внесли аванс»), "
                        "а не только текст описания продавца. "
                        "Примеры: «внесён аванс», «принят задаток», «за квартиру внесли аванс», "
                        "«объект снят с продажи — внесён задаток», «получен аванс». "
                        "Если таких упоминаний нет — false."
                    ),
                },
                "price_history": {
                    "type": "array",
                    "description": "История изменения цены (если есть раздел 'История цены')",
                    "items": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "Дата изменения (например: '10 мар 2026')",
                            },
                            "price": {
                                "type": "integer",
                                "description": "Цена в рублях на эту дату",
                            },
                            "change_amount": {
                                "type": "integer",
                                "description": "Опционально; пересчитывается на сервере по порядку строк.",
                            },
                            "change_type": {
                                "type": "string",
                                "enum": ["initial", "decrease", "increase"],
                                "description": "Опционально; пересчитывается на сервере.",
                            },
                        },
                        "required": ["date", "price"],
                    },
                },
            },
            "required": ["price", "area", "cian_id"],
        }

    async def parse_async(self, url: str) -> ParsedAdData:
        """
        Парсит одно объявление через self-hosted Firecrawl API.

        Этапы:
        1. Получаем куки из Cookie Manager
        2. Запрашиваем Firecrawl: markdown + rawHtml + json(AI)
        3. Из rawHtml извлекаем creationDate
        4. Из Cian API получаем точную статистику (переопределяет AI данные)
        5. Собираем ParsedAdData

        Args:
            url: URL объявления на cian.ru

        Returns:
            ParsedAdData с извлеченными данными
        """
        logger.info(f"🔍 Начинаю парсинг: {url}")

        # Jitter: случайная задержка 1-4 сек чтобы не выглядеть ботом
        import random
        await asyncio.sleep(random.uniform(1.0, 4.0))

        # 1. Получаем куки
        cookies_str = await self._get_cookies()

        if not cookies_str:
            logger.warning("⚠️ Cookies are empty, checking Cookie Manager status...")
            await self._check_and_trigger_recovery()
            raise ValueError(
                "Cookies are empty. Recovery triggered. Please retry later."
            )

        # 2. Формируем payload для Firecrawl
        payload = {
            "url": url,
            "excludeTags": EXCLUDE_TAGS,
            "formats": [
                "markdown",  # Для AI-экстракции (передается в LLM)
                "rawHtml",   # Для извлечения creationDate из embedded JSON
                {
                    "type": "json",
                    "schema": self._get_schema(),
                    "systemPrompt": SYSTEM_PROMPT,
                },
            ],
            "headers": {"Cookie": cookies_str} if cookies_str else {},
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            # Firecrawl иногда отвечает 500 SCRAPE_ALL_ENGINES_FAILED (часто из‑за таймаута/прокси).
            # В этом случае ждём 5 секунд и пробуем до 3 раз, затем сдаёмся.
            response = None
            for attempt in range(3):
                try:
                    # trust_env=False: иначе HTTP(S)_PROXY шлёт запрос на внутренний хост Firecrawl
                    # через мобильный прокси → getaddrinfo flippercrawl-api-1 не резолвится.
                    async with httpx.AsyncClient(timeout=180.0, trust_env=False) as client:
                        response = await client.post(
                            self.firecrawl_api_url, json=payload, headers=headers
                        )
                except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout, OSError) as e:
                    if attempt < 2:
                        logger.warning(
                            "Firecrawl network error (attempt %s/3) for %s: %s. Retrying in 5s...",
                            attempt + 1,
                            url,
                            e,
                        )
                        await asyncio.sleep(5)
                        continue
                    raise

                if response.status_code == 200:
                    break

                # retryable 500 with known code
                retryable = False
                if response.status_code >= 500:
                    try:
                        j = response.json()
                        if (
                            isinstance(j, dict)
                            and j.get("code") == "SCRAPE_ALL_ENGINES_FAILED"
                        ):
                            retryable = True
                    except Exception:
                        retryable = False

                if retryable and attempt < 2:
                    logger.warning(
                        "Firecrawl 5xx %s (SCRAPE_ALL_ENGINES_FAILED) attempt %s/3 for %s. Retrying in 5s...",
                        response.status_code,
                        attempt + 1,
                        url,
                    )
                    await asyncio.sleep(5)
                    continue

                # non-retryable or last attempt
                raise ValueError(
                    f"Firecrawl API error: {response.status_code} - {response.text[:200]}"
                )

            if response is None or response.status_code != 200:
                raise ValueError("Firecrawl API error: no successful response after retries")

            result = response.json()

            if not result.get("success"):
                logger.warning(f"Firecrawl returned success=false for {url}")
                raise ValueError("Firecrawl API returned success=false")

            if "data" not in result:
                raise ValueError("No data in Firecrawl response")

            data_obj = result["data"]

            # 3. Извлекаем creationDate из rawHtml (нужен для Cian Stats API)
            creation_date = None
            raw_html = data_obj.get("rawHtml", "")
            if raw_html:
                creation_date = self._extract_creation_date_from_html(raw_html)

            # 4. Проверяем наличие JSON данных от AI
            if "json" not in data_obj:
                logger.warning(
                    f"No JSON data extracted for {url}, checking authentication..."
                )
                is_auth = await self._check_authentication(cookies_str)
                if not is_auth:
                    logger.error("❌ Authentication failed! Triggering recovery...")
                    await self._check_and_trigger_recovery()
                    raise ValueError("Authentication failed. Recovery triggered.")
                raise ValueError(
                    "No JSON data extracted: Firecrawl не вернул json (сбой/таймаут LLM или пустая схема); "
                    "профиль my.cian.ru при этом доступен — это не обязательно проблема куков."
                )

            extracted_data = data_obj["json"]
            extracted_data["url"] = url

            # 5. Получаем точную статистику из Cian API (переопределяет AI данные)
            cian_id = extracted_data.get("cian_id")

            if cian_id and creation_date:
                creation_date_obj = datetime.strptime(creation_date, "%Y-%m-%d")
                api_date = (creation_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info(f"📅 Запрос статистики с датой: {api_date}")

                (
                    days_in_exposition,
                    total_views,
                    unique_views,
                ) = await self._get_statistics(cian_id, api_date, cookies_str)

                # Перезаписываем данные от AI точными данными из Cian API
                extracted_data["publish_date"] = creation_date
                extracted_data["days_in_exposition"] = days_in_exposition
                extracted_data["total_views"] = total_views
                extracted_data["unique_views"] = unique_views
            elif not cian_id:
                logger.warning("⚠️ cian_id не найден, пропускаем статистику")
                extracted_data.setdefault("publish_date", None)
                extracted_data.setdefault("days_in_exposition", None)
            elif not creation_date:
                logger.warning("⚠️ creationDate не найден в HTML, пропускаем статистику")
                extracted_data.setdefault("publish_date", None)
                extracted_data.setdefault("days_in_exposition", None)

            # 6. Нормализуем и создаём Pydantic модель
            normalized = self._normalize_data(extracted_data)
            parsed = ParsedAdData(**normalized)

            logger.info(
                f"✅ Успешно: {url} | Цена: {parsed.price:,} руб | Площадь: {parsed.area} м²"
            )
            return parsed

        except Exception as e:
            logger.error(f"Parse error for {url}: {e}")
            raise

    def _normalize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Нормализует данные от Firecrawl для Pydantic моделей.

        Поддерживает два формата:
        - Новый (вложенный): address={full, district, ...}, floor_info={current, all}
        - Старый (плоский): address_full, address_district, floor_current, floor_all

        Args:
            data: Словарь от Firecrawl JSON

        Returns:
            Нормализованный словарь для ParsedAdData
        """
        result = {}
        address_data = {}
        floor_data = {}

        for key, value in data.items():
            # Новый формат: уже вложенные объекты
            if key == "address" and isinstance(value, dict):
                address_data = {k: v for k, v in value.items() if v is not None}

            elif key == "floor_info" and isinstance(value, dict):
                floor_data = {k: v for k, v in value.items() if v is not None}

            # Старый плоский формат (обратная совместимость)
            elif key.startswith("address_"):
                address_key = key.replace("address_", "")
                if value:
                    address_data[address_key] = value

            elif key.startswith("floor_"):
                floor_key = key.replace("floor_", "")
                if value is not None:
                    floor_data[floor_key] = value

            else:
                if value is not None:
                    result[key] = value

        if address_data:
            result["address"] = address_data
        if floor_data:
            result["floor_info"] = floor_data

        # История цен: хронология по (дата, порядок в ответе ИИ), дельта от предыдущей строки
        ph = data.get("price_history")
        if ph and isinstance(ph, list):
            rows = []
            for idx, entry in enumerate(ph):
                if not isinstance(entry, dict):
                    continue
                d_str = entry.get("date")
                p = entry.get("price")
                price_i = int(p) if isinstance(p, (int, float)) else None
                if price_i is None:
                    continue
                d_key = _parse_price_history_date_str(d_str)
                rows.append(
                    {
                        "orig_index": idx,
                        "date_str": d_str if isinstance(d_str, str) else None,
                        "date": d_key,
                        "price": price_i,
                    }
                )

            if rows:
                # Циан: сверху новее. Нужен порядок «сначала старые по времени», затем
                # change_amount = price[i] − price[i−1] (снижение → отрицательная дельта).
                dated_in_input = [r for r in rows if r.get("date") is not None]

                if len(dated_in_input) < 2:
                    # Нет двух валидных дат — сортировка по дате ненадёжна; порядок ИИ как на сайте (сверху новее).
                    rows.sort(key=lambda e: -e["orig_index"])
                else:
                    first_date = dated_in_input[0]["date"]
                    last_date = dated_in_input[-1]["date"]
                    input_is_desc = bool(
                        first_date and last_date and first_date > last_date
                    )
                    # Несколько строк в один календарный день — сверху новее (как в модалке Циан)
                    unique_dates = {r["date"] for r in dated_in_input}
                    if len(unique_dates) == 1:
                        input_is_desc = True

                    def _sort_key(e):
                        date_rank = 1 if e["date"] is None else 0
                        date_key = e["date"] or datetime.min.date()
                        intra = -e["orig_index"] if input_is_desc else e["orig_index"]
                        return (date_rank, date_key, intra)

                    rows.sort(key=_sort_key)

                out = []
                for i, item in enumerate(rows):
                    cur = item["price"]
                    # Всегда нормализуем в ISO-дату, если можем распарсить.
                    # Это устраняет вариативность вида "2 ноя 2024" vs "2024-11-02".
                    date_out = item["date"].strftime("%Y-%m-%d") if item.get("date") else None
                    if date_out is None:
                        date_out = (
                            item["date_str"].strip()
                            if isinstance(item["date_str"], str)
                            and item["date_str"].strip()
                            else None
                        )
                    if i == 0:
                        out.append(
                            {
                                "date": date_out,
                                "price": cur,
                                "change_amount": 0,
                                "change_type": "initial",
                            }
                        )
                        continue
                    prev = rows[i - 1]["price"]
                    delta = cur - prev
                    if delta < 0:
                        ct = "decrease"
                    elif delta > 0:
                        ct = "increase"
                    else:
                        ct = "initial"
                    out.append(
                        {
                            "date": date_out,
                            "price": cur,
                            "change_amount": delta,
                            "change_type": ct,
                        }
                    )

                result["price_history"] = out

        result["building_type"] = _sanitize_building_type(result.get("building_type"))

        desc = result.get("description")
        if isinstance(desc, str) and desc.strip():
            result["description"] = _clean_cian_description(desc)

        return result
