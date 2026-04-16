"""
services.parser_cian.main - Entry point orchestrator

Оркестратор для сервиса парсинга Cian.
Управляет полным циклом: чтение URLs -> PostgreSQL -> парсинг -> сохранение в БД и Sheets.
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
from services.parser_cian.search_parser import extract_urls_by_searches
from services.parser_cian.db.repository import DatabaseRepository


def _configure_logging() -> None:
    """Консоль + опционально файл (ротация ~10 МБ, 5 архивов)."""
    from logging.handlers import RotatingFileHandler

    # Windows консоль часто CP1251/CP866 — эмодзи/✓ вызывают UnicodeEncodeError в логгере.
    # Делаем поток "непадающим": неподдерживаемые символы будут экранироваться.
    try:
        sys.stdout.reconfigure(errors="backslashreplace")  # py>=3.7
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(errors="backslashreplace")
    except Exception:
        pass

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
    # Backward compatibility: старое имя режима regular -> offers
    if getattr(args, "mode", None) == "regular":
        args.mode = "offers"

    log_section(
        f"Starting Parser Cian Service (mode={args.mode}, "
        f"skip_links={args.skip_links}, only_links={args.only_links}, "
        f"unparsed_only={getattr(args, 'unparsed_only', False)}, "
        f"unparsed_links={getattr(args, 'unparsed_links', False)})"
    )

    try:
        # === STEP 1: Validate Configuration ===
        log_section("Step 1: Configuration Validation")
        validate_config()

        # === STEP 2: Initialize Components ===
        log_section("Step 2: Initializing Components")

        import os
        os.makedirs("data/logs", exist_ok=True)

        logger.info("Initializing PostgreSQL Database...")
        db_repo = DatabaseRepository(database_url=settings.database_url)
        await db_repo.init_db()
        logger.info("Database initialized")

        if args.reset_sold:
            cleared = await db_repo.clear_sold_ads()
            logger.info(f"--reset-sold: очищено {cleared} записей из cian_sold_ads")

        logger.info("Initializing Google Sheets Manager...")
        sheets_manager = SheetsManager()
        logger.info("✓ Sheets Manager initialized")

        source = args.mode  # "offers" или "avans"

        newly_extracted_count = 0

        if args.skip_links:
            # --skip-links: пропускаем сбор ссылок, берём из БД то что уже есть
            log_section(f"Step 3-4: SKIPPED (--skip-links), loading from DB (source={source})")
            active_ad_urls = await db_repo.get_all_active_ads(source=source)
        else:
            # === STEP 3: Setup Search URLs ===
            log_section(f"Step 3: Setup Search URLs ({args.mode} mode)")

            if args.mode == "offers":
                logger.info("Reading search URLs from 'FILTERS' tab...")
                filters_table = sheets_manager.get_filters_table(
                    tab_name="FILTERS", max_columns="ZZ"
                )
                filter_rows = filters_table.get("rows") or []
                search_urls = [str(r.get("url") or "").strip() for r in filter_rows]
                search_urls = [u for u in search_urls if u]
                if not search_urls:
                    logger.warning("No search URLs in FILTERS tab (column A)")
                    logger.info("Exiting gracefully")
                    return

                db_filters = []
                for r in filter_rows:
                    u = (r.get("url") or "").strip()
                    if not u:
                        continue
                    meta = {
                        "sheet_row": r.get("row_index"),
                        "a_display": r.get("a_display") or "",
                        "cells": r.get("cells") or [],
                        "headers": filters_table.get("headers") or [],
                    }
                    db_filters.append({"url": u, "meta": meta})

                await db_repo.sync_filters_exact(db_filters)
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

            proxy_urls = settings.proxy_urls_for_search()
            if proxy_urls:
                logger.info("Используются прокси из файла (%s), %s шт.", settings.proxies_file, len(proxy_urls))

            urls_by_search = extract_urls_by_searches(
                search_urls=active_search_urls,
                location="Москва",
                max_pages=max_pages_limit,
                http_proxy=None,
                proxy_urls=proxy_urls or None,
                duplicate_streak_to_stop=settings.search_duplicate_streak_stop,
            )

            all_new_ad_urls = []
            for urls in (urls_by_search or {}).values():
                all_new_ad_urls.extend(urls or [])

            deduped_ad_urls = list(dict.fromkeys(all_new_ad_urls or []))
            newly_extracted_count = len(deduped_ad_urls)

            new_count = await db_repo.merge_active_ad_urls(deduped_ad_urls, source=source)
            logger.info(
                f"✓ Merged {newly_extracted_count} URLs from search: "
                f"{new_count} new, {newly_extracted_count - new_count} already in DB "
                f"(source={source})"
            )

            # Проставляем filter_id для объявлений (чтобы тащить данные FILTERS в Offers_Parser/Signals_Parser)
            if args.mode == "offers" and urls_by_search:
                filters_db = await db_repo.get_all_filters()
                by_url = {str(f.get("url") or "").strip(): f for f in (filters_db or [])}
                total_linked = 0
                for search_url, ad_urls in urls_by_search.items():
                    f = by_url.get(str(search_url).strip())
                    if not f:
                        continue
                    fid = f.get("id")
                    if not fid:
                        continue
                    updated = await db_repo.assign_filter_to_ads(ad_urls or [], fid, source=source)
                    total_linked += int(updated or 0)
                if total_linked:
                    logger.info(f"✓ Linked ads to FILTERS (assigned filter_id): {total_linked}")

            active_ad_urls = await db_repo.get_all_active_ads(source=source)

        signals_tab_urls: list[str] = []
        if getattr(args, "unparsed_links", False) and args.mode != "offers":
            logger.warning("--unparsed-links: флаг работает только с --mode offers, игнорирую")
        if getattr(args, "unparsed_links", False) and args.mode == "offers":
            log_section("Signals_Parser → merge в БД (source=offers)")
            signals_tab_urls = sheets_manager.get_urls(tab_name="Signals_Parser", column="A")
            signals_tab_urls = list(
                dict.fromkeys([str(u).strip() for u in signals_tab_urls if str(u).strip()])
            )
            added_sig = await db_repo.merge_active_ad_urls(signals_tab_urls, source="offers")
            logger.info(
                "✓ Signals_Parser: %s уникальных URL, в БД добавлено новых строк: %s",
                len(signals_tab_urls),
                added_sig,
            )

        if args.only_links:
            log_section("Step 5: SKIPPED (--only-links)")
            logger.info(
                f"Собрано ссылок с поисковых страниц (этот запуск): {newly_extracted_count}; "
                f"всего активных в БД (source={source}): {len(active_ad_urls)}"
            )
            if getattr(args, "unparsed_links", False) and args.mode == "offers":
                logger.info(
                    "Signals_Parser: в БД смержено %s URL; к следующему парсингу с --unparsed-links "
                    "будут выбраны только is_parsed=false из этого списка.",
                    len(signals_tab_urls),
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

        if getattr(args, "unparsed_links", False) and args.mode == "offers":
            active_ad_urls = await db_repo.get_unparsed_active_ads_in_urls(
                signals_tab_urls, source=source
            )
            logger.info(
                f"✓ --unparsed-links: к парсингу только URL из Signals_Parser с is_parsed=false: "
                f"{len(active_ad_urls)} (source={source})"
            )
        elif getattr(args, "unparsed_only", False):
            active_ad_urls = await db_repo.get_unparsed_active_ads(source=source)
            logger.info(
                f"✓ --unparsed-only: к парсингу только объявления без is_parsed в БД: "
                f"{len(active_ad_urls)} (source={source})"
            )
        else:
            logger.info(f"✓ Total active ad URLs ready to parse: {len(active_ad_urls)} (source={source})")

        if not active_ad_urls:
            logger.warning(
                f"Нечего парсить (source={source}). "
                f"Если указан --unparsed-only / --unparsed-links — список на парсинг пуст."
            )
            logger.info("Exiting gracefully")
            return

        # === STEP 5: Parse Ad URLs ===
        log_section("Step 5: Parsing Individual Ads")

        logger.info(f"Starting asynchronous parsing of {len(active_ad_urls)} ad URLs...")
        stats = await queue_manager.run(active_ad_urls)

        # === STEP 6: Cleanup Stale Ads ===
        log_section("Step 6: Cleanup Stale Ads")

        if args.mode == "avans":
            stale_removed = 0
            logger.info(
                "Avans mode: пропускаем remove_stale_active_ads "
                "(объявления живут до снятия или внесения аванса)"
            )
        else:
            stale_removed = 0
            if settings.cleanup_stale_active_ads:
                stale_removed = await db_repo.remove_stale_active_ads(
                    source=source, max_age_days=settings.ad_max_age_days
                )
            else:
                logger.info(
                    "Cleanup stale active ads: отключено (CLEANUP_STALE_ACTIVE_ADS=false). "
                    "Отслеживаем объявления до снятия с публикации."
                )
        remaining = await db_repo.get_all_active_ads(source=source)
        logger.info(
            f"Stale ads removed: {stale_removed} (older than {settings.ad_max_age_days} days). "
            f"Active ads remaining in DB: {len(remaining)}"
        )

        # === STEP 7: Report Results ===
        log_section("Step 7: Execution Summary")

        logger.info(f"Total URLs processed:  {stats['total']}")
        logger.info(f"Successfully parsed:   {stats['processed']}")
        logger.info(f"Errors encountered:    {stats['errors']}")
        logger.info(f"Success rate:          {stats['success_rate']}%")
        logger.info(f"Stale ads cleaned up:  {stale_removed}")

        logger.info("✓ Parser Cian Service completed successfully")

    except Exception as e:
        logger.error(f"Critical error: {type(e).__name__}: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parser Cian Service")
    parser.add_argument(
        "--mode",
        type=str,
        choices=["offers", "avans"],
        default="offers",
        help="Режим работы: offers (фильтры из Sheets) или avans (статичная ссылка). "
        "AliasЖ offers",
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
    parser.add_argument(
        "--reset-sold",
        action="store_true",
        default=False,
        help="Очистить cian_sold_ads перед запуском (вернуть все объявления в пайплайн)",
    )
    parser.add_argument(
        "--unparsed-only",
        action="store_true",
        default=False,
        help="Парсить только объявления с is_parsed=false в БД (остальные шаги без изменений)",
    )
    parser.add_argument(
        "--unparsed-links",
        action="store_true",
        default=False,
        help="Только mode=offers: читает URL из вкладки Signals_Parser (кол. A), мержит в БД, "
        "парсит пересечение с is_parsed=false (догон Offers → актуализация Signals)",
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