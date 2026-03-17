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

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'services', 'parser_cian'))

from services.parser_cian.parser import AdParser

load_dotenv()

async def test_price_history():
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    parser = AdParser(cookie_manager_url="http://localhost:8000")
    
    # Тестируем два URL
    test_urls = [
        {
            "url": "https://www.cian.ru/sale/flat/327607745/",
            "name": "С историей цен"
        },
        {
            "url": "https://www.cian.ru/sale/flat/326100259/",
            "name": "Без истории цен (или другой)"
        }
    ]
    
    for test_case in test_urls:
        url = test_case["url"]
        name = test_case["name"]
        
        logger.info("\n" + "="*80)
        logger.info(f"🧪 Тестируем: {name}")
        logger.info(f"📍 URL: {url}")
        logger.info("="*80)
        
        try:
            data = await parser.parse_async(url)
            
            # Сохраняем данные
            filename = f"price_history_test_{test_case['url'].split('/')[-2]}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data.model_dump(), f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Данные сохранены в {filename}")
            
            # Проверяем историю цен
            logger.info("\n📊 РЕЗУЛЬТАТ:")
            logger.info(f"💰 Текущая цена: {data.price:,} руб.")
            
            if data.price_history:
                logger.info(f"📈 История цен найдена: {len(data.price_history)} записей")
                logger.info("\n🕐 История изменений:")
                for i, entry in enumerate(data.price_history, 1):
                    change_emoji = {
                        "initial": "🆕",
                        "decrease": "📉",
                        "increase": "📈"
                    }.get(entry.change_type, "❓")
                    
                    change_str = ""
                    if entry.change_amount != 0:
                        sign = "+" if entry.change_type == "increase" else "-"
                        change_str = f" ({sign}{abs(entry.change_amount):,} руб.)"
                    
                    logger.info(
                        f"  {i}. {entry.date}: {entry.price:,} руб. "
                        f"{change_emoji} {entry.change_type}{change_str}"
                    )
            else:
                logger.info("ℹ️ История цен отсутствует (цена не менялась)")
            
            logger.info("✅ Парсинг успешен!")
            
        except Exception as e:
            logger.error(f"❌ ПРОВАЛ: {type(e).__name__}: {e}", exc_info=True)
        
        logger.info("="*80 + "\n")

if __name__ == "__main__":
    asyncio.run(test_price_history())
