"""
Тестовый скрипт для проверки работы QueueManager с РЕАЛЬНЫМИ Google Sheets.
ВНИМАНИЕ: Данные будут записаны в реальную таблицу!
"""

import asyncio
import os
import sys
import logging
from dotenv import load_dotenv

# Добавляем папки в пути поиска Python
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'services', 'parser_cian'))
sys.path.append(os.path.join(current_dir, 'packages'))

from services.parser_cian.parser import AdParser
from services.parser_cian.queue_manager import QueueManager
from packages.flipper_core.sheets import SheetsManager

# Загружаем переменные из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


async def test_queue_with_real_sheets():
    """
    Тестирует работу QueueManager с РЕАЛЬНЫМИ Google Sheets.
    """
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return
    
    if not os.getenv("SPREADSHEET_ID"):
        logger.error("❌ Ошибка: SPREADSHEET_ID не установлен в .env")
        return

    # Список тестовых URLs (можете изменить на свои)
    test_urls = [
        "https://www.cian.ru/sale/flat/325133590/",
        "https://www.cian.ru/sale/flat/326100259/",
        "https://www.cian.ru/sale/flat/325230006/",
    ]

    logger.info("=" * 80)
    logger.info("🧪 ТЕСТИРОВАНИЕ С РЕАЛЬНЫМИ GOOGLE SHEETS")
    logger.info("=" * 80)
    logger.warning("⚠️  ВНИМАНИЕ: Данные будут записаны в реальную таблицу!")
    logger.info("=" * 80)
    logger.info(f"📋 Количество URLs: {len(test_urls)}")
    logger.info(f"👷 Количество воркеров: 2")
    logger.info(f"📊 Spreadsheet ID: {os.getenv('SPREADSHEET_ID')}")
    logger.info("=" * 80)

    # Инициализируем компоненты
    parser = AdParser(cookie_manager_url="http://localhost:8000")
    
    # РЕАЛЬНЫЙ SheetsManager
    sheets_manager = SheetsManager(
        spreadsheet_id=os.getenv("SPREADSHEET_ID"),
        credentials_path=os.getenv("CREDENTIALS_PATH", "./credentials.json"),
    )
    
    # Создаем QueueManager с 2 воркерами
    queue_manager = QueueManager(
        parser=parser,
        sheets_manager=sheets_manager,
        concurrency=2,
    )

    try:
        # Запускаем обработку
        stats = await queue_manager.run(test_urls)
        
        # Выводим итоговую статистику
        logger.info("\n" + "=" * 80)
        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА")
        logger.info("=" * 80)
        logger.info(f"✅ Успешно обработано: {stats['processed']}/{stats['total']}")
        logger.info(f"❌ Ошибок: {stats['errors']}")
        logger.info(f"📈 Процент успеха: {stats['success_rate']}%")
        logger.info("=" * 80)
        logger.info("🔗 Проверьте результаты в Google Sheets:")
        logger.info(f"   https://docs.google.com/spreadsheets/d/{os.getenv('SPREADSHEET_ID')}")
        logger.info("=" * 80)
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Прервано пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {type(e).__name__}: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_queue_with_real_sheets())
