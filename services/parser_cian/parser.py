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

# System prompt для AI-экстракции
SYSTEM_PROMPT = (
    "Экстрактор объявлений Cian.ru: заполни поля по схеме из markdown; нет данных — null. "
    "Просмотры: в одной строке «X просмотров, Y за сегодня» — X→total_views, Y→unique_views. "
    "is_active: true, если карточка доступна. "
    "price_history: если есть раздел 'История цены' — заполни массив записей с date, price, "
    "change_amount (0 для первой), change_type (initial/decrease/increase). Нет раздела — null."
)

# Теги для исключения из HTML перед конвертацией в markdown
EXCLUDE_TAGS = [
    "svg",
    "img",
    "script",
    "style",
    "footer",
    "header",
    "[data-name='CardSectionNew']",
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
        """Один запрос GET /cookies (без кэша). cookie_manager обычно в NO_PROXY."""
        try:
            async with httpx.AsyncClient(timeout=5.0, trust_env=False) as client:
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
                async with httpx.AsyncClient(
                    timeout=10.0, follow_redirects=False, trust_env=False
                ) as client:
                    resp = await client.get(
                        "https://my.cian.ru/profile", headers=headers
                    )

                    if resp.status_code in [301, 302, 303, 307, 308]:
                        location = resp.headers.get("location", "")
                        if "authenticate" in location:
                            logger.warning(
                                f"Attempt {attempt + 1}: Redirected to login"
                            )
                            if attempt < attempts - 1:
                                await asyncio.sleep(5)
                            continue

                        html = resp.text

                        if '"isAuthenticated":true' in html:
                            logger.info(
                                f"✅ isAuthenticated: true (attempt {attempt + 1})"
                            )
                            return True
                        elif '"isAuthenticated":false':
                            logger.warning(
                                f"❌ isAuthenticated: false (attempt {attempt + 1})"
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
                    "description": "Текст описания объявления",
                },
                "address": {
                    "type": "object",
                    "properties": {
                        "full": {"type": "string", "description": "Полный адрес"},
                        "district": {"type": "string", "description": "Район"},
                        "metro_station": {
                            "type": "string",
                            "description": "Ближайшая станция метро",
                        },
                        "okrug": {
                            "type": "string",
                            "description": "Округ (ЦАО, ЮВАО и т.д.)",
                        },
                    },
                },
                "area": {"type": "number", "description": "Общая площадь в м²"},
                "rooms": {"type": "integer", "description": "Количество комнат"},
                "housing_type": {
                    "type": "string",
                    "description": "Тип жилья (Вторичка, Новостройка)",
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
                "renovation": {"type": "string", "description": "Тип ремонта"},
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
                                "description": "На сколько изменилась цена (отрицательное = снижение). 0 для первой записи.",
                            },
                            "change_type": {
                                "type": "string",
                                "enum": ["initial", "decrease", "increase"],
                                "description": "Тип изменения: initial (первая цена), decrease (снижение), increase (повышение)",
                            },
                        },
                        "required": ["date", "price", "change_amount", "change_type"],
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
            # trust_env=False: иначе HTTP(S)_PROXY шлёт запрос на внутренний хост Firecrawl
            # через мобильный прокси → getaddrinfo flippercrawl-api-1 не резолвится.
            async with httpx.AsyncClient(timeout=180.0, trust_env=False) as client:
                response = await client.post(
                    self.firecrawl_api_url, json=payload, headers=headers
                )

            if response.status_code != 200:
                raise ValueError(
                    f"Firecrawl API error: {response.status_code} - {response.text[:200]}"
                )

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
                raise ValueError("No JSON data extracted")

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

        # Нормализуем историю цен
        ph = data.get("price_history")
        if ph and isinstance(ph, list):
            parsed_ph = []
            for idx, entry in enumerate(ph):
                d_str = entry.get("date")
                p = entry.get("price")
                parsed_date = None
                if isinstance(d_str, str):
                    try:
                        parsed_date = datetime.strptime(d_str, "%Y-%m-%d")
                    except Exception:
                        try:
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
                            }
                            parts = d_str.strip().split()
                            if len(parts) >= 3:
                                day = int(parts[0])
                                mon_raw = parts[1].lower()[:3]
                                year = int(parts[2])
                                mon = months.get(mon_raw)
                                if mon:
                                    parsed_date = datetime(year, mon, day)
                        except Exception:
                            parsed_date = None

                parsed_ph.append(
                    {
                        "orig_index": idx,
                        "date_str": d_str,
                        "date": parsed_date.date() if parsed_date else None,
                        "price": int(p) if isinstance(p, (int, float)) else None,
                    }
                )

            parsed_ph = [e for e in parsed_ph if e.get("price") is not None]

            if parsed_ph:
                dated = [e for e in parsed_ph if e.get("date") is not None]
                order_desc = True
                if dated and len(dated) >= 2:
                    if dated[0]["date"] < dated[1]["date"]:
                        order_desc = False

                seq = parsed_ph if order_desc else list(reversed(parsed_ph))

                out = []
                for i, item in enumerate(seq):
                    cur_price = item["price"]
                    if i + 1 < len(seq):
                        prev_price = seq[i + 1]["price"]
                        change_amount = cur_price - prev_price
                        if change_amount < 0:
                            change_type = "decrease"
                        elif change_amount > 0:
                            change_type = "increase"
                        else:
                            change_type = "initial"
                    else:
                        change_type = "initial"
                        change_amount = 0

                    out.append(
                        {
                            "date": item.get("date_str")
                            or (
                                item["date"].strftime("%Y-%m-%d")
                                if item.get("date")
                                else None
                            ),
                            "price": cur_price,
                            "change_amount": change_amount,
                            "change_type": change_type,
                        }
                    )

                result["price_history"] = out

        return result
