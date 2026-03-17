"""
services.parser_cian.parser - AdParser for Firecrawl integration

Парсер объявлений недвижимости с использованием Firecrawl API.
Получает куки из microservice cookie_manager, который работает внутри Docker.
"""

import os
import logging
import httpx
import asyncio
from typing import Dict, Any
from models import ParsedAdData

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
                                "description": "На сколько изменилась цена по сравнению с предыдущей (0 для первой публикации)"
                            },
                            "change_type": {
                                "type": "string",
                                "enum": ["initial", "decrease", "increase"],
                                "description": "Тип: 'initial' (первая цена), 'decrease' (снижение), 'increase' (повышение)"
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
                {
                    "type": "json",
                    "schema": self._get_schema()
                }
            ],
            "waitFor": 0,
            "onlyMainContent": True,
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

            if "data" not in result or "json" not in result.get("data", {}):
                # Если данных нет, может быть проблема с сессией
                logger.warning(f"No JSON data extracted for {url}, checking authentication...")
                
                # Проверяем авторизацию
                is_auth = await self._check_authentication(cookies_str)
                if not is_auth:
                    logger.error("❌ Authentication failed! Triggering recovery...")
                    await self._check_and_trigger_recovery()
                    raise ValueError("Authentication failed. Recovery triggered.")
                
                raise ValueError("No JSON data extracted")

            extracted_data = result["data"]["json"]
            extracted_data["url"] = url
            
            # Устанавливаем значения "soon" для полей, которые не парсим через Firecrawl
            extracted_data["publish_date"] = "soon"
            extracted_data["days_in_exposition"] = "soon"
            extracted_data["total_views"] = "soon"
            extracted_data["unique_views"] = "soon"

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

        return result
