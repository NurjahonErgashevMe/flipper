"""
services.parser_cian.parser - AdParser for Firecrawl integration

Парсер объявлений недвижимости с использованием Firecrawl API.
Получает куки из microservice cookie_manager, который работает внутри Docker.
"""

import os
import logging
import httpx
import asyncio
import re
from typing import Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from services.parser_cian.models import ParsedAdData

logger = logging.getLogger(__name__)


class AdParser:
    """
    Парсер объявлений Cian через Firecrawl API v2.
    
    Особенности:
    - Использует Cookie Manager для получения валидных куков
    - Структурированная экстракция через JSON Schema
    - Асинхронные запросы через httpx
    """

    def __init__(self, cookie_manager_url: str = "http://cookie_manager:8000"):
        """
        Args:
            cookie_manager_url: URL микросервиса управления куками
                Для локальной разработки: http://localhost:8000
                Для Docker: http://cookie_manager:8000
        """
        self.api_key = os.getenv("FIRECRAWL_API_KEY")
        if not self.api_key:
            raise ValueError("FIRECRAWL_API_KEY environment variable is not set")
        
        self.firecrawl_api_url = "https://api.firecrawl.dev/v2/scrape"
        self.cookie_manager_url = cookie_manager_url.rstrip("/")
        
        logger.info("AdParser initialized with Firecrawl API")
        logger.info(f"Cookie Manager URL: {self.cookie_manager_url}")

    async def _get_cookies(self) -> str:
        """
        Получает куки из микросервиса cookie_manager.
        
        Returns:
            Строка куков в формате "name1=value1; name2=value2"
            Если ошибка, возвращает пустую строку
        """
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{self.cookie_manager_url}/cookies")
                
                # Если 503 - recovery в процессе
                if resp.status_code == 503:
                    logger.warning("⚠️ Cookie Manager: Recovery in progress")
                    return ""
                
                if resp.status_code == 200:
                    cookies = resp.json()
                    cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
                    logger.debug(f"Fetched {len(cookies)} cookies from manager")
                    return cookie_str
                else:
                    logger.warning(f"Cookie manager returned {resp.status_code}")
                    return ""
        except Exception as e:
            logger.error(f"Failed to fetch cookies from manager: {e}")
            return ""
    
    async def _check_authentication(self, cookie_str: str, attempts: int = 3) -> bool:
        """
        Проверяет авторизацию на Cian.ru.
        Делает 3 попытки с паузой 5 сек.
        
        Args:
            cookie_str: Строка кук
            attempts: Количество попыток
            
        Returns:
            True если авторизован, False если нет
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Cookie": cookie_str
        }
        
        for attempt in range(attempts):
            try:
                logger.info(f"🔍 Checking authentication (attempt {attempt + 1}/{attempts})...")
                async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
                    resp = await client.get("https://my.cian.ru/profile", headers=headers)
                    
                    # Если редирект на /authenticate/ - не авторизованы
                    if resp.status_code in [301, 302, 303, 307, 308]:
                        location = resp.headers.get("location", "")
                        if "authenticate" in location:
                            logger.warning(f"⚠️ Attempt {attempt + 1}: Redirected to login page")
                            logger.warning(f"❌ isAuthenticated: false (redirect to {location})")
                            if attempt < attempts - 1:
                                await asyncio.sleep(5)
                            continue
                    
                    html = resp.text
                    
                    # Проверяем наличие isAuthenticated
                    if '"isAuthenticated":true' in html:
                        logger.info(f"✅ isAuthenticated: true (attempt {attempt + 1})")
                        logger.info(f"✅ Authentication OK")
                        return True
                    elif '"isAuthenticated":false' in html:
                        logger.warning(f"❌ isAuthenticated: false (attempt {attempt + 1})")
                    else:
                        logger.warning(f"⚠️ isAuthenticated not found in HTML (attempt {attempt + 1})")
                    
            except Exception as e:
                logger.warning(f"⚠️ Attempt {attempt + 1} error: {e}")
            
            if attempt < attempts - 1:
                logger.info(f"⏳ Waiting 5 seconds before retry...")
                await asyncio.sleep(5)
        
        logger.error("❌❌❌ All authentication checks failed - isAuthenticated: false")
        return False
    
    async def _check_and_trigger_recovery(self):
        """
        Проверяет статус Cookie Manager и запускает recovery если нужно.
        """
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                # Запускаем принудительную проверку
                resp = await client.post(f"{self.cookie_manager_url}/check")
                if resp.status_code == 200:
                    data = resp.json()
                    if not data.get("valid"):
                        logger.warning("🚨 Cookie Manager confirmed: cookies are INVALID")
                        # Recovery уже запущен автоматически
                    else:
                        logger.info("✅ Cookie Manager says cookies are valid")
        except Exception as e:
            logger.error(f"Failed to trigger recovery check: {e}")

    def _extract_creation_date_from_html(self, html: str) -> Optional[str]:
        """
        Извлекает creationDate из HTML страницы.
        
        Args:
            html: HTML контент страницы
            
        Returns:
            Дата создания в формате YYYY-MM-DD или None
        """
        try:
            # Ищем creationDate в HTML (обычно в JSON внутри script тега)
            # Паттерн: "creationDate":"2026-03-08T22:35:02.89" или "creationDate": "2026-03-08T22:35:02.89"
            patterns = [
                r'"creationDate"\s*:\s*"(\d{4}-\d{2}-\d{2})T[^"]*"',  # С T и временем
                r'"creationDate"\s*:\s*"(\d{4}-\d{2}-\d{2})"',  # Только дата
            ]
            
            for pattern in patterns:
                match = re.search(pattern, html)
                if match:
                    creation_date_str = match.group(1)
                    logger.info(f"✅ Найдена creationDate: {creation_date_str}")
                    logger.info(f"📅 Используем дату для API: {creation_date_str}")
                    return creation_date_str
            
            logger.warning("⚠️ creationDate не найден в HTML")
            logger.debug(f"HTML preview (first 500 chars): {html[:500]}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Ошибка извлечения creationDate: {e}")
            return None

    async def _get_statistics(self, cian_id: str, creation_date: str, cookies_str: str) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """
        Получает статистику просмотров объявления из Cian API.
        
        Args:
            cian_id: ID объявления на Cian
            creation_date: Дата создания - 1 день (формат: YYYY-MM-DD)
            cookies_str: Строка с куками
            
        Returns:
            Tuple: (days_in_exposition, total_views, unique_views)
            
        Raises:
            Exception: Если не удалось получить статистику
        """
        url = f"https://api.cian.ru/offer-card/v1/get-offer-card-statistic/?offerCreationDate={creation_date}&offerId={cian_id}"
        
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
            "Cookie": cookies_str
        }
        
        logger.info(f"📊 Получаем статистику для объявления {cian_id} (creation_date: {creation_date})...")
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code != 200:
                    logger.warning(f"⚠️ Статистика API вернул {response.status_code}")
                    return None, None, None, None
                
                data = response.json()

                # helper: parse numbers and dates from localized strings
                def _parse_number_from_string(s: str) -> Optional[int]:
                    if not s:
                        return None
                    m = re.search(r"(\d+[\d\s\u00A0]*)", s)
                    if not m:
                        return None
                    # remove spaces and non-breaking spaces
                    num = re.sub(r"[\s\u00A0]", "", m.group(1))
                    try:
                        return int(num)
                    except Exception:
                        return None

                def _parse_date_from_string(s: str) -> Optional[datetime]:
                    if not s:
                        return None
                    # look for dd.mm.yyyy
                    m = re.search(r"(\d{2}\.\d{2}\.\d{4})", s)
                    if m:
                        try:
                            return datetime.strptime(m.group(1), "%d.%m.%Y")
                        except Exception:
                            pass
                    # look for iso date yyyy-mm-dd
                    m2 = re.search(r"(\d{4}-\d{2}-\d{2})", s)
                    if m2:
                        try:
                            return datetime.strptime(m2.group(1), "%Y-%m-%d")
                        except Exception:
                            pass
                    return None

                # Проверяем наличие daily и dailyViews
                daily = data.get("daily", {}) or {}
                daily_views = daily.get("dailyViews") or []

                if not daily_views:
                    logger.warning("⚠️ Массив dailyViews пустой или отсутствует")

                # Собираем все даты и views из dailyViews (если есть)
                parsed_entries = []
                for entry in daily_views:
                    date_raw = entry.get("date")
                    views = entry.get("views")
                    dt = None
                    if isinstance(date_raw, str):
                        try:
                            dt = datetime.strptime(date_raw, "%Y-%m-%d")
                        except Exception:
                            dt = _parse_date_from_string(date_raw)
                    if dt and isinstance(views, int):
                        parsed_entries.append((dt.date(), views))

                publish_date_from_daily = None
                days_in_exposition = None
                unique_views = None
                total_views = None

                # Determine total_views: prefer top-level totalViews string if present
                root_total_views_str = data.get("totalViews")
                root_total, root_total_date = None, None
                if isinstance(root_total_views_str, str):
                    root_total = _parse_number_from_string(root_total_views_str)
                    dt = _parse_date_from_string(root_total_views_str)
                    if dt:
                        root_total_date = dt.date()

                # daily total string may be informative
                daily_total_views_str = daily.get("totalViews")
                daily_total = None
                if isinstance(daily_total_views_str, str):
                    daily_total = _parse_number_from_string(daily_total_views_str)

                # If we have parsed entries, compute earliest/latest and totals
                if parsed_entries:
                    dates = [d for d, _ in parsed_entries]
                    earliest = min(dates)
                    latest = max(dates)
                    publish_date_from_daily = earliest
                    # days between earliest and latest (non-negative)
                    days_in_exposition = max(0, (latest - earliest).days)
                    # unique views for latest day
                    # find entry with latest date
                    for d, v in parsed_entries:
                        if d == latest:
                            unique_views = v
                            break
                    # total views: prefer root_total, then daily_total, then sum of parsed
                    if root_total is not None:
                        total_views = root_total
                    elif daily_total is not None:
                        total_views = daily_total
                    else:
                        total_views = sum(v for _, v in parsed_entries)

                else:
                    # No parsed daily entries - try to fall back to strings
                    if root_total is not None:
                        total_views = root_total
                    elif daily_total is not None:
                        total_views = daily_total

                # If root_total_date present, use it as publish date override
                publish_date = None
                if root_total_date:
                    publish_date = root_total_date
                    # compute days_in_exposition against latest (if available) or today
                    ref_date = None
                    if parsed_entries:
                        ref_date = max(d for d, _ in parsed_entries)
                    else:
                        ref_date = datetime.utcnow().date()
                    days_in_exposition = max(0, (ref_date - publish_date).days)
                elif publish_date_from_daily:
                    publish_date = publish_date_from_daily

                logger.info("✅ Статистика получена:")
                logger.info(f"   📆 Дней в экспозиции: {days_in_exposition}")
                logger.info(f"   👁️ Всего просмотров: {total_views}")
                logger.info(f"   🔍 Уникальных просмотров (сегодня): {unique_views}")

                return days_in_exposition, total_views, unique_views
                
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики: {e}")
            return None, None, None, None

    def _get_schema(self) -> Dict[str, Any]:
        """
        JSON Schema для структурированной экстракции данных объявления.
        
        Returns:
            Dict с JSON Schema
        """
        return {
            "type": "object",
            "properties": {
                "price": {
                    "type": "integer",
                    "description": "Цена в рублях"
                },
                "title": {
                    "type": "string",
                    "description": "Заголовок объявления"
                },
                "address_full": {
                    "type": "string",
                    "description": "Полный адрес"
                },
                "address_district": {
                    "type": "string",
                    "description": "Район"
                },
                "address_metro_station": {
                    "type": "string",
                    "description": "Ближайшее метро"
                },
                "metro_walk_time": {
                    "type": "integer",
                    "description": "Время пешком до метро в минутах (только число)"
                },
                "address_okrug": {
                    "type": "string",
                    "description": "Округ (ЦАО, САО и т.д)"
                },
                "area": {
                    "type": "number",
                    "description": "Площадь в м²"
                },
                "cian_id": {
                    "type": "string",
                    "description": "ID объявления в Cian"
                },
                "rooms": {
                    "type": "integer",
                    "description": "Количество комнат"
                },
                "floor_current": {
                    "type": "integer",
                    "description": "Текущий этаж"
                },
                "floor_all": {
                    "type": "integer",
                    "description": "Всего этажей"
                },
                "description": {
                    "type": "string",
                    "description": "Описание объявления"
                },
                "price_per_m2": {
                    "type": "integer",
                    "description": "Цена за м²"
                },
                "construction_year": {
                    "type": "integer",
                    "description": "Год постройки"
                },
                "renovation": {
                    "type": "string",
                    "description": "Статус ремонта"
                },
                "housing_type": {
                    "type": "string",
                    "description": "Тип жилья"
                },
                "price_history": {
                    "type": "array",
                    "description": "История изменения цены (если есть раздел 'История цены')",
                    "items": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "Дата изменения (например: '10 мар 2026')"
                            },
                            "price": {
                                "type": "integer",
                                "description": "Цена в рублях на эту дату"
                            },
                            "change_amount": {
                                "type": "integer",
                                "description": "На сколько изменилась цена по сравнению с предыдущей (может быть отрицательным для decrease). Для первой записи = 0."
                            },
                            "change_type": {
                                "type": "string",
                                "enum": ["initial", "decrease", "increase"],
                                "description": "Тип изменения цены: 'initial' (первая публикация), 'decrease' (цена снизилась - зелёная стрелка вниз), 'increase' (цена повысилась - красная стрелка вверх). Если текущая цена МЕНЬШЕ предыдущей - это decrease. Если текущая цена БОЛЬШЕ предыдущей - это increase."
                            }
                        },
                        "required": ["date", "price", "change_amount", "change_type"]
                    }
                }
            },
            "required": ["price", "area", "cian_id"]
        }

    async def parse_async(self, url: str) -> ParsedAdData:
        """
        Парсит одно объявление через Firecrawl API.
        
        Args:
            url: URL объявления на cian.ru
            
        Returns:
            ParsedAdData с извлеченными данными
            
        Raises:
            ValueError: Если ошибка при парсинге или извлечении данных
        """
        logger.info(f"🔍 Начинаю парсинг: {url}")
        
        # Получаем куки
        cookies_str = await self._get_cookies()
        
        # Если куки пустые - проверяем статус Cookie Manager
        if not cookies_str:
            logger.warning("⚠️ Cookies are empty, checking Cookie Manager status...")
            await self._check_and_trigger_recovery()
            raise ValueError("Cookies are empty. Recovery triggered. Please retry later.")

        # Формируем Payload для Firecrawl API
        payload = {
            "url": url,
            "formats": [
                "html",  # Обработанный HTML (может содержать creationDate)
                "rawHtml",  # Оригинальный HTML со скриптами
                {
                    "type": "json",
                    "schema": self._get_schema()
                }
            ],
            "waitFor": 0,
            "headers": {
                "Cookie": cookies_str
            } if cookies_str else {}
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.post(
                    self.firecrawl_api_url,
                    json=payload,
                    headers=headers
                )

            if response.status_code != 200:
                raise ValueError(
                    f"Firecrawl API error: {response.status_code} - {response.text[:200]}"
                )

            result = response.json()

            # Проверяем успех и наличие данных
            if not result.get("success"):
                logger.warning(f"Firecrawl returned success=false for {url}")
                raise ValueError("Firecrawl API returned success=false")

            if "data" not in result:
                logger.warning(f"No data in Firecrawl response for {url}")
                raise ValueError("No data in Firecrawl response")
            
            data_obj = result["data"]
            
            # Извлекаем HTML для получения creationDate
            # Сначала пробуем rawHtml (там точно есть скрипты), потом html
            creation_date = None
            for html_key in ["rawHtml", "html"]:
                html_content = data_obj.get(html_key, "")
                if html_content:
                    creation_date = self._extract_creation_date_from_html(html_content)
                    if creation_date:
                        break
            
            # Проверяем наличие JSON данных
            if "json" not in data_obj:
                # Если данных нет, может быть проблема с сессией
                logger.warning(f"No JSON data extracted for {url}, checking authentication...")
                
                # Проверяем авторизацию
                is_auth = await self._check_authentication(cookies_str)
                if not is_auth:
                    logger.error("❌ Authentication failed! Triggering recovery...")
                    await self._check_and_trigger_recovery()
                    raise ValueError("Authentication failed. Recovery triggered.")
                
                raise ValueError("No JSON data extracted")

            extracted_data = data_obj["json"]
            extracted_data["url"] = url
            
            # Получаем cian_id для запроса статистики
            cian_id = extracted_data.get("cian_id")
            
            if cian_id and creation_date:
                # Вычитаем 1 день из creationDate для API
                # API ожидает дату ДО первого дня публикации
                creation_date_obj = datetime.strptime(creation_date, "%Y-%m-%d")
                api_date = (creation_date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info(f"📅 Отправляем в API дату: {api_date} (creationDate - 1 день)")
                
                # Получаем статистику просмотров
                days_in_exposition, total_views, unique_views = await self._get_statistics(
                    cian_id, api_date, cookies_str
                )
                
                # Обновляем данные
                extracted_data["publish_date"] = creation_date
                extracted_data["days_in_exposition"] = days_in_exposition
                extracted_data["total_views"] = total_views
                extracted_data["unique_views"] = unique_views
            elif not cian_id:
                logger.warning("⚠️ cian_id не найден, пропускаем получение статистики")
                extracted_data["publish_date"] = None
                extracted_data["days_in_exposition"] = None
                extracted_data["total_views"] = None
                extracted_data["unique_views"] = None
            elif not creation_date:
                logger.warning("⚠️ creationDate не найден в HTML, пропускаем получение статистики")
                extracted_data["publish_date"] = None
                extracted_data["days_in_exposition"] = None
                extracted_data["total_views"] = None
                extracted_data["unique_views"] = None

            # Преобразуем плоский JSON в вложенные Pydantic модели
            normalized = self._normalize_data(extracted_data)
            parsed = ParsedAdData(**normalized)

            logger.info(f"✅ Успешно спарсил: {url} | Цена: {parsed.price:,} руб | Площадь: {parsed.area} м²")
            return parsed

        except Exception as e:
            logger.error(f"Parse error for {url}: {e}")
            raise

    def _normalize_data(self, flat_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Преобразует плоский JSON (от Firecrawl) в структурированный формат
        для Pydantic моделей с вложенными объектами.
        
        Args:
            flat_data: Плоский словарь от Firecrawl
            
        Returns:
            Нормализованный словарь для ParsedAdData
        """
        result = {}
        address_data = {}
        floor_data = {}

        for key, value in flat_data.items():
            if key.startswith("address_"):
                # "address_full" -> {"full": ...}
                address_key = key.replace("address_", "")
                if value:  # Пропускаем пустые значения
                    address_data[address_key] = value
            elif key.startswith("floor_"):
                # "floor_current" -> {"current": ...}
                floor_key = key.replace("floor_", "")
                if value is not None:  # 0 - валидное значение для этажа
                    floor_data[floor_key] = value
            else:
                # Остальные поля берем как есть
                if value is not None:
                    result[key] = value

        # Добавляем вложенные объекты если они не пустые
        if address_data:
            result["address"] = address_data
        if floor_data:
            result["floor_info"] = floor_data

        # Нормализуем историю цен если она есть: гарантируем порядок newest->oldest
        # и корректно вычисляем change_amount (abs diff) и change_type.
        ph = flat_data.get("price_history")
        if ph and isinstance(ph, list):
            parsed = []
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
                                'янв':1,'фев':2,'мар':3,'апр':4,'май':5,'мая':5,'июн':6,
                                'июл':7,'авг':8,'сен':9,'окт':10,'ноя':11,'дек':12
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
                parsed.append({
                    "orig_index": idx,
                    "date_str": d_str,
                    "date": parsed_date.date() if parsed_date else None,
                    "price": int(p) if isinstance(p, (int, float)) else None,
                    "raw": entry,
                })

            # Filter out entries without price
            parsed = [e for e in parsed if e.get("price") is not None]

            if parsed:
                # determine if list is newest->oldest or oldest->newest by inspecting parsed dates
                dated = [e for e in parsed if e.get("date") is not None]
                order_desc = True
                if dated and len(dated) >= 2:
                    dates = [e["date"] for e in dated]
                    # if first date < second date -> ascending -> not desc
                    if dates[0] < dates[1]:
                        order_desc = False

                # build list in newest->oldest order for computing changes
                if order_desc:
                    seq = parsed
                else:
                    seq = list(reversed(parsed))

                # compute changes: for each entry in seq (newest->oldest) compare to next (older)
                out = []
                for i, item in enumerate(seq):
                    cur_price = item["price"]
                    if i + 1 < len(seq):
                        prev_price = seq[i+1]["price"]
                        diff = abs(cur_price - prev_price)
                        change_type = "decrease" if cur_price < prev_price else ("increase" if cur_price > prev_price else "initial")
                        change_amount = diff
                    else:
                        change_type = "initial"
                        change_amount = 0

                    out.append({
                        "date": item.get("date_str") or (item.get("date").strftime("%Y-%m-%d") if item.get("date") else None),
                        "price": cur_price,
                        "change_amount": change_amount,
                        "change_type": change_type,
                    })

                # ensure output order is newest->oldest (as in UI)
                result["price_history"] = out

        return result
