"""
Тестовый скрипт для проверки работы QueueManager с несколькими воркерами.
Демонстрирует параллельный парсинг нескольких объявлений.
"""

import asyncio
import os
import sys
import logging
from dotenv import load_dotenv

# Добавляем папку сервиса в пути поиска Python
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'services', 'parser_cian'))

from services.parser_cian.parser import AdParser
from services.parser_cian.queue_manager import QueueManager

# Загружаем переменные из .env
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


# Mock SheetsManager для тестирования без реального Google Sheets
class MockSheetsManager:
    """Заглушка для SheetsManager, чтобы не писать в реальные Google Sheets"""
    
    def write_row(self, tab_name: str, row: list) -> bool:
        """Имитирует запись в Google Sheets"""
        logger.info(f"📝 [MockSheets] Записываю в таб '{tab_name}': {len(row)} колонок")
        # Имитируем задержку записи
        import time
        time.sleep(0.5)
        return True


async def test_queue_with_workers():
    """
    Тестирует работу QueueManager с несколькими воркерами.
    """
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    # Список тестовых URLs
    test_urls = [
        "https://www.cian.ru/sale/flat/325133590/",
        "https://www.cian.ru/sale/flat/326100259/",
        "https://www.cian.ru/sale/flat/325230006/",
    ]

    logger.info("=" * 80)
    logger.info("🧪 ТЕСТИРОВАНИЕ QUEUE MANAGER С НЕСКОЛЬКИМИ ВОРКЕРАМИ")
    logger.info("=" * 80)
    logger.info(f"📋 Количество URLs: {len(test_urls)}")
    logger.info(f"👷 Количество воркеров: 2")
    logger.info("=" * 80)

    # Инициализируем компоненты
    parser = AdParser(cookie_manager_url="http://localhost:8000")
    mock_sheets = MockSheetsManager()
    
    # Создаем QueueManager с 2 воркерами
    queue_manager = QueueManager(
        parser=parser,
        sheets_manager=mock_sheets,
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
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Прервано пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {type(e).__name__}: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_queue_with_workers())
