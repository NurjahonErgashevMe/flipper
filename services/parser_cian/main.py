"""
services.parser_cian.main - Entry point orchestrator

Оркестратор для сервиса парсинга Cian.
Управляет полным циклом: чтение URLs -> парсинг -> сохранение в Sheets.
"""

import asyncio
import logging
import sys

# Общие пакеты (Docker их найдет благодаря PYTHONPATH="/app")
from packages.flipper_core.sheets import SheetsManager
from packages.flipper_core.utils import log_section

# Локальные модули сервиса (БЕЗ ТОЧЕК в начале)
from config import settings, validate_config
from parser import AdParser
from queue_manager import QueueManager
from search_parser import extract_batch_from_searches

# Настройка логирования
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


async def main():
    """
    Главная функция оркестратора.
    """
    log_section("Starting Parser Cian Service")
    
    try:
        # === STEP 1: Validate Configuration ===
        log_section("Step 1: Configuration Validation")
        validate_config()

        # === STEP 2: Initialize Components ===
        log_section("Step 2: Initializing Components")
        
        logger.info("Initializing Google Sheets Manager...")
        sheets_manager = SheetsManager(
            spreadsheet_id=settings.spreadsheet_id,
            credentials_path=settings.credentials_path,
        )
        logger.info("✓ Sheets Manager initialized")

        logger.info("Initializing Firecrawl Parser...")
        parser = AdParser(cookie_manager_url=settings.cookie_manager_url)
        logger.info("✓ Parser initialized")

        logger.info(f"Initializing Queue Manager (concurrency: {settings.parser_concurrency})...")
        queue_manager = QueueManager(
            parser=parser,
            sheets_manager=sheets_manager,
            concurrency=settings.parser_concurrency,
        )
        logger.info("✓ Queue Manager initialized")

        # === STEP 3: Read URLs from Google Sheets ===
        log_section("Step 3: Reading URLs from Google Sheets")
        
        logger.info("Reading URLs from 'FILTERS' tab...")
        search_urls = sheets_manager.get_urls(tab_name="FILTERS", column="A")
        
        if not search_urls:
            logger.warning("No URLs found in FILTERS tab")
            logger.info("Exiting gracefully")
            return
        
        logger.info(f"✓ Found {len(search_urls)} URLs to process")

        # === STEP 4: Parse URLs ===
        log_section("Step 4: Processing URLs")
        
        logger.info(f"Starting asynchronous parsing of {len(search_urls)} URLs...")
        stats = await queue_manager.run(search_urls)

        # === STEP 5: Report Results ===
        log_section("Step 5: Execution Summary")
        
        logger.info(f"Total URLs processed:  {stats['total']}")
        logger.info(f"Successfully parsed:   {stats['processed']}")
        logger.info(f"Errors encountered:    {stats['errors']}")
        logger.info(f"Success rate:          {stats['success_rate']}%")
        
        logger.info("✓ Parser Cian Service completed successfully")

    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        logger.error(f"File Not Found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Critical error: {type(e).__name__}: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)