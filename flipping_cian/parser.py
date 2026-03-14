import logging
from firecrawl import FirecrawlApp
from config import settings
from models import ParsedAdData
import asyncio

logger = logging.getLogger(__name__)


class AdParser:
    def __init__(self):
        self.app = FirecrawlApp(api_key=settings.firecrawl_api_key)

    def parse_sync(self, url: str) -> ParsedAdData:
        """Synchronously parse an ad using Firecrawl"""
        try:
            logger.info(f"Parsing URL via Firecrawl: {url}")
            data = self.app.scrape(
                url,
                only_main_content=False,
                formats=[
                    {
                        "type": "json",
                        "schema": ParsedAdData.model_json_schema(),
                        "prompt": (
                            "Извлеките данные об объекте недвижимости строго в соответствии со схемой и описаниями полей. "
                            "ОЧЕНЬ ВАЖНО: \n"
                            "1. Цена (price, price_per_m2) и площадь (area) должны быть ТОЛЬКО голыми числами (например 20000000 или 45.5), без пробелов, запятых, слов 'руб' или 'м2'.\n"
                            "2. Округ (okrug) должен быть строго одной из аббревиатур: ЦАО, САО, СВАО, ВАО, ЮВАО, ЮАО, ЮЗАО, ЗАО, СЗАО, ЗелАО, Троицкий, Новомосковский.\n"
                            "3. Этажи (current и all) должны быть только целыми числами.\n"
                            "4. Дата (publish_date) только в формате YYYY-MM-DD (например 2023-10-25).\n"
                            "5. Идентификатор (cian_id) только строка из цифр.\n"
                            "Не добавляйте ничего лишнего. Если данных для поля нет или они конфликтуют с правилами, верните null."
                        ),
                    }
                ],
            )

            # Firecrawl v2 SDK
            extracted_json = {}
            if hasattr(data, "json") and data.json:
                extracted_json = data.json
            elif isinstance(data, dict) and "json" in data:
                extracted_json = data["json"]

            # Add URL
            extracted_json["url"] = url

            # Parse into Pydantic model
            parsed_model = ParsedAdData(**extracted_json)
            return parsed_model

        except Exception as e:
            logger.error(f"Failed to parse {url}: {e}")
            raise

    async def parse_async(self, url: str) -> ParsedAdData:
        """Asynchronously parse an ad using Firecrawl by running sync call in executor"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.parse_sync, url)
