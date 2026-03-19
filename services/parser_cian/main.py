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

# Локальные модули сервиса
from services.parser_cian.config import settings, validate_config
from services.parser_cian.parser import AdParser
from services.parser_cian.queue_manager import QueueManager
from services.parser_cian.search_parser import extract_batch_from_searches

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
        sheets_manager = SheetsManager()
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
        
        logger.info("Reading search URLs from 'FILTERS' tab...")
        search_urls = sheets_manager.get_urls(tab_name="FILTERS", column="A")
        
        if not search_urls:
            logger.warning("No search URLs found in FILTERS tab")
            logger.info("Exiting gracefully")
            return
        
        logger.info(f"✓ Found {len(search_urls)} search URLs (categories)")

        # === STEP 4: Extract Ad URLs from Search Pages ===
        log_section("Step 4: Extracting Ad URLs from Search Pages")
        
        logger.info(f"Extracting ad URLs from {len(search_urls)} search pages...")
        all_ad_urls = extract_batch_from_searches(
            search_urls=search_urls,
            location="Москва",
            max_urls_per_search=1,
            http_proxy=settings.http_proxy if settings.http_proxy else None
        )
        
        if not all_ad_urls:
            logger.warning("No ad URLs extracted from any category")
            logger.info("Exiting gracefully")
            return
        
        logger.info(f"✓ Total ad URLs to parse: {len(all_ad_urls)}")

        # === STEP 5: Parse Ad URLs ===
        log_section("Step 5: Parsing Individual Ads")
        
        logger.info(f"Starting asynchronous parsing of {len(all_ad_urls)} ad URLs...")
        stats = await queue_manager.run(all_ad_urls)

        # === STEP 6: Report Results ===
        log_section("Step 6: Execution Summary")
        
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