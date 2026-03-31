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


def _configure_logging() -> None:
    """Консоль + опционально файл (ротация ~10 МБ, 5 архивов)."""
    from logging.handlers import RotatingFileHandler

    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    level_name = str(settings.log_level or "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    log_path = (settings.parser_cian_log_file or "").strip()
    if log_path:
        import os

        log_dir = os.path.dirname(log_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        fh = RotatingFileHandler(
            log_path,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        handlers.append(fh)
    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)


_configure_logging()

logger = logging.getLogger(__name__)


import argparse


async def main(args):
    """Главная функция оркестратора."""
    log_section(
        f"Starting Parser Cian Service (mode={args.mode}, "
        f"skip_links={args.skip_links}, only_links={args.only_links})"
    )

    try:
        # === STEP 1: Validate Configuration ===
        log_section("Step 1: Configuration Validation")
        validate_config()

        # === STEP 2: Initialize Components ===
        log_section("Step 2: Initializing Components")

        import os
        os.makedirs("data", exist_ok=True)

        logger.info("Initializing SQLite Database...")
        db_repo = DatabaseRepository(db_path="data/parser_cian.db")
        await db_repo.init_db()
        logger.info("✓ Database initialized")

        logger.info("Initializing Google Sheets Manager...")
        sheets_manager = SheetsManager()
        logger.info("✓ Sheets Manager initialized")

        source = args.mode  # "regular" или "avans"

        newly_extracted_count = 0

        if args.skip_links:
            # --skip-links: пропускаем сбор ссылок, берём из БД то что уже есть
            log_section(f"Step 3-4: SKIPPED (--skip-links), loading from DB (source={source})")
            active_ad_urls = await db_repo.get_all_active_ads(source=source)
        else:
            # === STEP 3: Setup Search URLs ===
            log_section(f"Step 3: Setup Search URLs ({args.mode} mode)")

            if args.mode == "regular":
                logger.info("Reading search URLs from 'FILTERS' tab...")
                raw_urls = sheets_manager.get_urls(tab_name="FILTERS", column="A")
                search_urls = [
                    str(u).strip()
                    for u in (raw_urls or [])
                    if u is not None and str(u).strip()
                ]
                if not search_urls:
                    logger.warning("No search URLs in FILTERS tab (column A)")
                    logger.info("Exiting gracefully")
                    return

                await db_repo.sync_filters_exact(search_urls)
                logger.info(
                    f"✓ FILTERS synced to DB (exactly {len(search_urls)} URLs from sheet)"
                )
                active_search_urls = search_urls
                max_pages_limit = settings.regular_search_max_pages
            else:
                logger.info("Using Avans search URL from config...")
                active_search_urls = [settings.avans_search_url]
                max_pages_limit = settings.avans_max_pages

            logger.info(f"✓ Found {len(active_search_urls)} active search URLs")

            # === STEP 4: Extract Ad URLs from Search Pages ===
            log_section("Step 4: Extracting Ad URLs from Search Pages")

            logger.info(f"Extracting ad URLs from {len(active_search_urls)} search pages (max {max_pages_limit} pages deep)...")

            all_new_ad_urls = extract_batch_from_searches(
                search_urls=active_search_urls,
                location="Москва",
                max_pages=max_pages_limit,
                http_proxy=None,
                duplicate_streak_to_stop=settings.search_duplicate_streak_stop,
            )

            deduped_ad_urls = list(dict.fromkeys(all_new_ad_urls or []))
            newly_extracted_count = len(deduped_ad_urls)

            await db_repo.replace_active_ad_urls(deduped_ad_urls, source=source)
            logger.info(
                f"✓ Active ad list replaced with {newly_extracted_count} URLs from current "
                f"search pages (source={source})"
            )

            active_ad_urls = await db_repo.get_all_active_ads(source=source)

        if args.only_links:
            log_section("Step 5: SKIPPED (--only-links)")
            logger.info(
                f"Собрано ссылок с поисковых страниц (этот запуск): {newly_extracted_count}; "
                f"всего активных в БД (source={source}): {len(active_ad_urls)}"
            )
            logger.info("✓ Parser Cian Service completed (--only-links)")
            return

        logger.info("Initializing Firecrawl Parser...")
        parser = AdParser(
            cookie_manager_url=settings.cookie_manager_url,
            firecrawl_base_url=settings.firecrawl_base_url,
            firecrawl_api_key=settings.firecrawl_api_key,
        )
        logger.info("✓ Parser initialized")

        logger.info(f"Initializing Queue Manager (concurrency={settings.parser_concurrency}, mode={args.mode})...")
        queue_manager = QueueManager(
            parser=parser,
            sheets_manager=sheets_manager,
            db_repo=db_repo,
            concurrency=settings.parser_concurrency,
            mode=args.mode,
        )
        logger.info("✓ Queue Manager initialized")

        if not active_ad_urls:
            logger.warning(f"No active ad URLs found in DB for source={source}")
            logger.info("Exiting gracefully")
            return

        logger.info(f"✓ Total active ad URLs ready to parse: {len(active_ad_urls)} (source={source})")

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
    parser = argparse.ArgumentParser(description="Parser Cian Service")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["regular", "avans"],
        default="regular",
        help="Режим работы: regular (фильтры из Sheets) или avans (статичная ссылка)",
    )
    link_stage = parser.add_mutually_exclusive_group()
    link_stage.add_argument(
        "--skip-links",
        action="store_true",
        default=False,
        help="Пропустить сбор ссылок (шаги 3-4), парсить только то что уже есть в БД",
    )
    link_stage.add_argument(
        "--only-links",
        action="store_true",
        default=False,
        help="Только собрать ссылки с поисковых страниц в БД (шаги 3-4), без парсинга карточек",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)