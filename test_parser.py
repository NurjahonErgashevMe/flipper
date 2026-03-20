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
# чтобы внутренние импорты парсера работали без ошибок
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'services', 'parser_cian'))

from services.parser_cian.parser import AdParser
from services.parser_cian.models import parse_to_sheets_row
from packages.flipper_core.sheets import SheetsManager
from services.parser_cian.queue_manager import check_signals, send_telegram_notification

# Загружаем переменные из .env
load_dotenv()

async def test_ads():
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    # Инициализация Google Sheets Manager
    sheets_manager = None
    try:
        logger.info("📊 Инициализация Google Sheets Manager...")
        creds_path = "credentials.json" if os.path.exists("credentials.json") else "/app/credentials.json"
        sheets_manager = SheetsManager(credentials_path=creds_path)
        logger.info("✅ Google Sheets Manager готов")
    except (ValueError, FileNotFoundError) as e:
        logger.warning(f"⚠️ Google Sheets не настроен: {e}")

    # Подключаемся к менеджеру кук
    logger.info("🔌 Подключаемся к Cookie Manager...")
    parser = AdParser(cookie_manager_url="http://localhost:8000")
    
    urls_to_test = [
        "https://www.cian.ru/sale/flat/326100259/", # Активное
        "https://www.cian.ru/sale/flat/326002860/", # SOLD
    ]
    
    for url in urls_to_test:
        logger.info("\n" + "="*80)
        logger.info(f"🧪 Тестируем парсинг URL: {url}")
        logger.info("="*80)
        
        try:
            data = await parser.parse_async(url)
            
            # Сохраняем данные в json
            output_file = f"data_{data.cian_id}.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data.model_dump(mode='json'), f, ensure_ascii=False, indent=2)
            
            # Основная информация
            logger.info(f"\n📋 Основная информация:")
            logger.info(f"  🆔 Cian ID: {data.cian_id}")
            logger.info(f"  💰 Цена: {data.price:,} руб." if data.price else "  💰 Цена: Не указана")
            logger.info(f"  🟢 Статус активности: {'АКТИВНО' if data.is_active else 'SOLD (СНЯТО)'}")
            
            # Оценка Signals_Parser
            price_history_dicts = [h.model_dump(mode='json') for h in data.price_history] if data.price_history else []
            is_signal = check_signals(price_history_dicts)
            logger.info(f"\n🚦 Подходит для Signals_Parser: {'✅ ДА' if is_signal else '❌ НЕТ'}")
            
            # Сохранение в Google Sheets
            if sheets_manager:
                if not data.is_active:
                    tab_name = "SOLD"
                    logger.info(f"💾 Пишем в таб {tab_name}...")
                    row = parse_to_sheets_row(data)
                    await asyncio.to_thread(sheets_manager.write_row, tab_name, row, insert_at_top=True)
                else:
                    row = parse_to_sheets_row(data)
                    # Offers_Parser (Голубой если > 50 охват)
                    off_bg = {"red": 0.8, "green": 0.9, "blue": 1.0} if (data.unique_views and data.unique_views >= 50) else None
                    logger.info("💾 Пишем в Offers_Parser...")
                    await asyncio.to_thread(sheets_manager.write_row, "Offers_Parser", row, insert_at_top=True, bg_color=off_bg)
                    if off_bg:
                        msg = f"🌟 <b>Offers_Parser Match! (TEST)</b>\nУникальных просмотров сегодня: {data.unique_views}\nЦена: {data.price} руб.\nСсылка: <a href='{url}'>{url}</a>"
                        await send_telegram_notification(msg)
                    
                    # Signals_Parser (Желтый)
                    if is_signal:
                        sig_bg = {"red": 1.0, "green": 0.9, "blue": 0.7}
                        logger.info("💾 Сигнал обнаружен! Пишем в Signals_Parser...")
                        await asyncio.to_thread(sheets_manager.write_row, "Signals_Parser", row, insert_at_top=True, bg_color=sig_bg)
                        msg = f"🚦 <b>Signals_Parser Match! (TEST)</b>\nСработало условие по снижению цены.\nЦена: {data.price} руб.\nСсылка: <a href='{url}'>{url}</a>"
                        await send_telegram_notification(msg)
            
        except Exception as e:
            logger.error(f"\n❌ ПРОВАЛ ПАРСИНГА для {url}: {e}", exc_info=True)

    # ТЕСТ МОКОВОГО ОБЪЯВЛЕНИЯ ИЗ signaled_data.json
    logger.info("\n" + "="*80)
    logger.info(f"🧪 Тестируем МОК-ОБЪЯВЛЕНИЕ ИЗ signaled_data.json")
    logger.info("="*80)
    try:
        from services.parser_cian.models import ParsedAdData
        from services.parser_cian.config import settings
        
        filename = "signaled_data.json"
        if not os.path.exists(filename):
            logger.error(f"❌ Файл {filename} не найден!")
        else:
            with open(filename, "r", encoding="utf-8") as f:
                json_data = json.load(f)
            
            # Валидируем данные через модель
            mock_data = ParsedAdData.model_validate(json_data)
            
            logger.info(f"📋 Данные загружены для ID: {mock_data.cian_id} ({filename})")
            logger.info(f"💰 Цена: {mock_data.price:,} руб.")
            logger.info(f"👁️ Уникальные просмотры: {mock_data.unique_views}")
            
            # Проверка сигналов
            price_history_dicts = [h.model_dump(mode='json') for h in mock_data.price_history] if mock_data.price_history else []
            is_signal = check_signals(price_history_dicts)
            logger.info(f"🚦 Подходит для Signals_Parser: {'✅ ДА' if is_signal else '❌ НЕТ'}")
            
            if sheets_manager:
                row_mock = parse_to_sheets_row(mock_data)
                
                # Offers_Parser
                off_bg = settings.sheet_highlight_color if (mock_data.unique_views and mock_data.unique_views >= settings.min_unique_views) else None
                logger.info(f"💾 Пишем МОК в Offers_Parser (Highlights: {'ДА' if off_bg else 'НЕТ'})...")
                await asyncio.to_thread(sheets_manager.find_and_update_row, "Offers_Parser", row_mock, id_value=mock_data.cian_id, id_column_index=20, bg_color=off_bg)
                if off_bg:
                    msg = f"🌟 <b>Offers_Parser Match! (MOCK)</b>\nУникальных просмотров сегодня: {mock_data.unique_views}\nЦена: {mock_data.price} руб."
                    await send_telegram_notification(msg)
                
                # Signals_Parser
                if is_signal:
                    sig_bg = settings.sheet_highlight_color
                    logger.info("💾 Пишем МОК в Signals_Parser...")
                    await asyncio.to_thread(sheets_manager.find_and_update_row, "Signals_Parser", row_mock, id_value=mock_data.cian_id, id_column_index=20, bg_color=sig_bg)
                    msg = f"🚦 <b>Signals_Parser Match! (MOCK)</b>\nСработало условие по снижению цены.\nЦена: {mock_data.price} руб."
                    await send_telegram_notification(msg)
                
                logger.info("✅ Тест мок-данных успешно завершен")
                
    except Exception as e:
        logger.error(f"❌ Ошибка при тесте мок-данных: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_ads())