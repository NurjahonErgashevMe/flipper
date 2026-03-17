import asyncio
import os
import sys
import logging
import json
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

# Добавляем папку сервиса в пути поиска Python, 
# чтобы внутренние импорты парсера (например, from models import ...) работали без ошибок
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'services', 'parser_cian'))

from services.parser_cian.parser import AdParser

# Загружаем переменные из .env
load_dotenv()

async def test_single_ad():
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    # Подключаемся к менеджеру кук (он запущен в Docker и доступен на 8000 порту локалхоста)
    logger.info("🔌 Подключаемся к Cookie Manager...")
    parser = AdParser(cookie_manager_url="http://localhost:8000")
    
    url = "https://www.cian.ru/sale/flat/326100259/"
    logger.info(f"🧪 Тестируем парсинг через Cloud Firecrawl API")
    logger.info(f"📍 URL: {url}")
    
    try:
        data = await parser.parse_async(url)
        
        # Сохраняем данные в data.json
        output_file = "data.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data.model_dump(), f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Данные сохранены в {output_file}")
        
        logger.info("\n" + "="*60)
        logger.info("✅ УСПЕШНО СПАРСИЛИ ДАННЫЕ!")
        logger.info("="*60)
        logger.info(f"💰 Цена: {data.price:,} руб.")
        logger.info(f"📐 Площадь: {data.area} м²")
        logger.info(f"🚇 Метро: {data.address.metro_station if data.address else 'Нет данных'}")
        logger.info(f"🏠 Комнат: {data.rooms if data.rooms else 'Нет данных'}")
        logger.info(f"🆔 ID на Циан: {data.cian_id}")
        logger.info("="*60)
    except Exception as e:
        logger.error(f"\n❌ ПРОВАЛ ПАРСИНГА: {type(e).__name__}: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_single_ad())