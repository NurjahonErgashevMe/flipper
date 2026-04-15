"""Main entry point for category counter service."""

import os
import sys
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from packages.flipper_core.sheets import SheetsManager
from services.category_counter.parser import CategoryCounterParser

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def main():
    """Run category counter parser."""
    load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

    for key in ("DECODO_AUTH_TOKEN", "DECODO_SCRAPER_URL", "DECODO_MAX_RETRIES"):
        os.environ.pop(key, None)

    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    rel = (os.environ.get("CIAN_PROXIES_FILE") or "data/proxies.txt").strip()
    proxy_path = rel if os.path.isabs(rel) else os.path.normpath(os.path.join(root, rel))

    from services.parser_cian.proxy_loader import load_proxy_urls

    proxy_urls = load_proxy_urls(proxy_path) if os.path.isfile(proxy_path) else []
    http_single = (os.environ.get("HTTP_PROXY") or "").strip() or None

    logger.info(
        "Starting category counter (file %s → %s прокси; HTTP_PROXY=%s)",
        proxy_path,
        len(proxy_urls),
        "да" if http_single else "нет",
    )
    parser = CategoryCounterParser(
        http_proxy=http_single if not proxy_urls else None,
        proxy_urls=proxy_urls if proxy_urls else None,
    )
    
    # Parse all categories
    logger.info("Parsing all categories...")
    results = parser.parse_all_categories()
    
    if not results:
        logger.error("Failed to parse categories")
        sys.exit(1)
    
    logger.info(f"Successfully parsed {len(results)} categories")
    
    # Initialize Sheets Manager (auto-reads from env)
    logger.info("Initializing Google Sheets Manager...")
    sheets_manager = SheetsManager()
    
    # Write to Google Sheets
    logger.info("Writing results to Google Sheets...")
    success = write_to_sheets(sheets_manager, results)
    
    if success:
        logger.info("✓ Successfully wrote results to Google Sheets")
    else:
        logger.error("Failed to write results to Google Sheets")
        sys.exit(1)


def write_to_sheets(sheets_manager: SheetsManager, results: list) -> bool:
    """Write category counter results to Google Sheets."""
    SHEET_NAME = "Balans"
    EQUILIBRIUM_VALUE = 150000
    
    try:
        # Get current date and time in MSK timezone
        msk_tz = ZoneInfo("Europe/Moscow")
        now_msk = datetime.now(msk_tz)
        
        # Format: DD.MM.YYYY HH:MM:SS
        datetime_str = now_msk.strftime("%d.%m.%Y %H:%M:%S")
        
        logger.info(f"Current MSK time: {datetime_str}")
        
        # Prepare row data
        # A: Date/Time, B: Вторичка Москва, C: Первичка Москва, D: Первичка МО, E: Вторичка МО, F: Всего, G: Точка равновесия
        
        # Create a dict for easy lookup by category name
        results_dict = {r["name"]: r["count"] for r in results}
        
        # Build the row in correct order
        row = [
            datetime_str,  # A: Date and time
            results_dict.get("Вторичка Москва", 0),  # B
            results_dict.get("Первичка Москва", 0),  # C
            results_dict.get("Первичка МО", 0),      # D
            results_dict.get("Вторичка МО", 0),      # E
            "=SUM(B{},C{},D{},E{})",  # F: placeholder; ниже подставляется номер строки
            EQUILIBRIUM_VALUE                         # G: Точка равновесия
        ]
        
        # Find the next empty row (after header)
        range_name = f"{SHEET_NAME}!A:A"
        values = sheets_manager.read_range(range_name)
        
        # Find next row (skip header in row 1)
        next_row = len(values) + 1 if values else 2
        
        logger.info(f"Writing to row {next_row}")
        
        # Update the sum formula with correct row number
        row[5] = f"=SUM(B{next_row}:E{next_row})"
        
        # Write the row
        range_to_write = f"{SHEET_NAME}!A{next_row}:G{next_row}"
        logger.info(f"Range: {range_to_write}")
        logger.info(f"Data: {row}")
        
        success = sheets_manager.update_range(range_to_write, [row])
        
        if success:
            logger.info(f"Successfully wrote data to row {next_row}")
            logger.info(f"  Date/Time: {datetime_str}")
            logger.info(f"  Вторичка Москва: {results_dict.get('Вторичка Москва', 0):,}")
            logger.info(f"  Первичка Москва: {results_dict.get('Первичка Москва', 0):,}")
            logger.info(f"  Первичка МО: {results_dict.get('Первичка МО', 0):,}")
            logger.info(f"  Вторичка МО: {results_dict.get('Вторичка МО', 0):,}")
        
        return success
        
    except Exception as e:
        logger.error(f"Error writing to Google Sheets: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    main()
