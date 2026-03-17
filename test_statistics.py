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

async def test_statistics():
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    logger.info("🔌 Подключаемся к Cookie Manager...")
    parser = AdParser(cookie_manager_url="http://localhost:8000")
    
    # Тестовый URL с историей цен
    url = "https://www.cian.ru/sale/flat/327607745/"
    
    logger.info("\n" + "="*80)
    logger.info("🧪 Тестируем полный парсинг с получением статистики")
    logger.info(f"📍 URL: {url}")
    logger.info("="*80 + "\n")
    
    try:
        data = await parser.parse_async(url)
        
        # Сохраняем данные
        output_file = "test_statistics_result.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data.model_dump(), f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Данные сохранены в {output_file}")
        
        logger.info("\n" + "="*80)
        logger.info("✅ РЕЗУЛЬТАТ ПАРСИНГА")
        logger.info("="*80)
        
        # Основная информация
        logger.info(f"\n📋 Основная информация:")
        logger.info(f"  🆔 Cian ID: {data.cian_id}")
        logger.info(f"  💰 Цена: {data.price:,} руб." if data.price else "  💰 Цена: Не указана")
        logger.info(f"  📐 Площадь: {data.area} м²" if data.area else "  📐 Площадь: Не указана")
        logger.info(f"  🏠 Комнат: {data.rooms}" if data.rooms else "  🏠 Комнат: Не указано")
        
        # Статистика
        logger.info(f"\n📊 Статистика:")
        logger.info(f"  📅 Дата публикации: {data.publish_date if data.publish_date else 'Не получена'}")
        logger.info(f"  📆 Дней в экспозиции: {data.days_in_exposition if data.days_in_exposition is not None else 'Не получено'}")
        logger.info(f"  👁️ Всего просмотров: {data.total_views if data.total_views is not None else 'Не получено'}")
        logger.info(f"  🔍 Уникальных просмотров (сегодня): {data.unique_views if data.unique_views is not None else 'Не получено'}")
        
        # История цен
        if data.price_history:
            logger.info(f"\n📈 История цен ({len(data.price_history)} записей):")
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
            logger.info("\nℹ️ История цен отсутствует")
        
        logger.info("\n" + "="*80)
        logger.info("✅ ТЕСТ ЗАВЕРШЕН УСПЕШНО!")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"\n❌ ПРОВАЛ ПАРСИНГА: {type(e).__name__}: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_statistics())
