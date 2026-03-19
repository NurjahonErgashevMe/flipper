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
from services.parser_cian.models import parse_to_sheets_row
from packages.flipper_core.sheets import SheetsManager

# Загружаем переменные из .env
load_dotenv()

async def test_single_ad():
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    # Инициализация Google Sheets Manager
    sheets_manager = None
    try:
        logger.info("📊 Инициализация Google Sheets Manager...")
        sheets_manager = SheetsManager()
        logger.info("✅ Google Sheets Manager готов")
    except (ValueError, FileNotFoundError) as e:
        logger.warning(f"⚠️ Google Sheets не настроен: {e}")

    # Подключаемся к менеджеру кук (он запущен в Docker и доступен на 8000 порту локалхоста)
    logger.info("🔌 Подключаемся к Cookie Manager...")
    parser = AdParser(cookie_manager_url="http://localhost:8000")
    
    # url = "https://www.cian.ru/sale/flat/326100259/"
    url = "https://www.cian.ru/sale/flat/326002860/"
    logger.info(f"🧪 Тестируем полный парсинг (Firecrawl + Статистика + История цен)")
    logger.info(f"📍 URL: {url}")
    
    try:
        data = await parser.parse_async(url)
        
        # Сохраняем данные в data.json
        output_file = "data.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data.model_dump(mode='json'), f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Данные сохранены в {output_file}")
        
        logger.info("\n" + "="*80)
        logger.info("✅ УСПЕШНО СПАРСИЛИ ДАННЫЕ!")
        logger.info("="*80)
        
        # Основная информация
        logger.info(f"\n📋 Основная информация:")
        logger.info(f"  🆔 Cian ID: {data.cian_id}")
        logger.info(f"  💰 Цена: {data.price:,} руб." if data.price else "  💰 Цена: Не указана")
        logger.info(f"  📐 Площадь: {data.area} м²" if data.area else "  📐 Площадь: Не указана")
        logger.info(f"  🏠 Комнат: {data.rooms}" if data.rooms else "  🏠 Комнат: Не указано")
        logger.info(f"  🚇 Метро: {data.address.metro_station if data.address and data.address.metro_station else 'Нет данных'}")
        logger.info(f"  🚶 До метро: {data.metro_walk_time} мин" if data.metro_walk_time else "  🚶 До метро: Не указано")
        logger.info(f"  🏢 Этаж: {data.floor_info.current}/{data.floor_info.all}" if data.floor_info and data.floor_info.current and data.floor_info.all else "  🏢 Этаж: Не указано")
        
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
            logger.info("\nℹ️ История цен отсутствует (цена не менялась)")
        
        # Сохранение в Google Sheets
        if sheets_manager:
            logger.info("\n💾 Сохранение в Google Sheets...")
            try:
                row = parse_to_sheets_row(data)
                success = sheets_manager.write_row("PARSED", row)
                if success:
                    logger.info("✅ Данные успешно записаны в Google Sheets (таб PARSED)")
                else:
                    logger.error("❌ Не удалось записать данные в Google Sheets")
            except Exception as e:
                logger.error(f"❌ Ошибка при записи в Google Sheets: {e}", exc_info=True)
        
        logger.info("\n" + "="*80)
        logger.info("✅ ТЕСТ ЗАВЕРШЕН УСПЕШНО!")
        logger.info("="*80)
    except Exception as e:
        logger.error(f"\n❌ ПРОВАЛ ПАРСИНГА: {type(e).__name__}: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_single_ad())