"""
services.parser_cian.main - Entry point orchestrator

Оркестратор для сервиса парсинга Cian.
Управляет полным циклом: чтение URLs -> SQLite БД -> парсинг -> сохранение в БД и Sheets.
"""

import asyncio
import logging
import sys

# Общие пакеты
from packages.flipper_core.sheets import SheetsManager
from packages.flipper_core.utils import log_section

# Локальные модули сервиса
from services.parser_cian.config import settings, validate_config
from services.parser_cian.parser import AdParser
from services.parser_cian.queue_manager import QueueManager
from services.parser_cian.search_parser import extract_batch_from_searches
from services.parser_cian.db.repository import DatabaseRepository

# Настройка логирования
logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


async def main():
    """Главная функция оркестратора."""
    log_section("Starting Parser Cian Service")
    
    try:
        # === STEP 1: Validate Configuration ===
        log_section("Step 1: Configuration Validation")
        validate_config()

        # === STEP 2: Initialize Components ===
        log_section("Step 2: Initializing Components")
        
        logger.info("Initializing SQLite Database...")
        import os
        os.makedirs("data", exist_ok=True)
        db_repo = DatabaseRepository(db_path="data/parser_cian.db")
        await db_repo.init_db()
        logger.info("✓ Database initialized")

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
            db_repo=db_repo,
            concurrency=settings.parser_concurrency,
        )
        logger.info("✓ Queue Manager initialized")

        # === STEP 3: Read URLs from Google Sheets and Sync to DB ===
        log_section("Step 3: Syncing Fitlers with Database")
        
        logger.info("Reading search URLs from 'FILTERS' tab...")
        search_urls = sheets_manager.get_urls(tab_name="FILTERS", column="A")
        
        if search_urls:
            await db_repo.add_filters(search_urls)
            logger.info(f"✓ Synced {len(search_urls)} search URLs to DB")
            
        saved_filters = await db_repo.get_all_filters()
        if not saved_filters:
            logger.warning("No search URLs found in DB or FILTERS tab")
            logger.info("Exiting gracefully")
            return

        active_search_urls = [f["url"] for f in saved_filters]
        logger.info(f"✓ Found {len(active_search_urls)} active search URLs in DB")

        # === STEP 4: Extract Ad URLs from Search Pages ===
        log_section("Step 4: Extracting Ad URLs from Search Pages")
        
        logger.info(f"Extracting ad URLs from {len(active_search_urls)} search pages...")
        all_new_ad_urls = extract_batch_from_searches(
            search_urls=active_search_urls,
            location="Москва",
            max_pages=50,  # Идем до 50 страниц вглубь
            http_proxy=settings.http_proxy if settings.http_proxy else None
        )
        
        if all_new_ad_urls:
            await db_repo.add_ad_urls(all_new_ad_urls)
            logger.info(f"✓ Synced {len(all_new_ad_urls)} newly extracted ad URLs to DB")

        # Fetch all active ads from DB to parse
        active_ad_urls = await db_repo.get_all_active_ads()
        
        if not active_ad_urls:
            logger.warning("No active ad URLs found in DB for parsing")
            logger.info("Exiting gracefully")
            return
            
        logger.info(f"✓ Total active ad URLs from DB ready to parse: {len(active_ad_urls)}")

        # === STEP 5: Parse Ad URLs ===
        log_section("Step 5: Parsing Individual Ads")
        
        logger.info(f"Starting asynchronous parsing of {len(active_ad_urls)} ad URLs...")
        stats = await queue_manager.run(active_ad_urls)

        # === STEP 6: Report Results ===
        log_section("Step 6: Execution Summary")
        
        logger.info(f"Total URLs processed:  {stats['total']}")
        logger.info(f"Successfully parsed:   {stats['processed']}")
        logger.info(f"Errors encountered:    {stats['errors']}")
        logger.info(f"Success rate:          {stats['success_rate']}%")
        
        logger.info("✓ Parser Cian Service completed successfully")

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