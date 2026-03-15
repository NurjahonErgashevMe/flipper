import asyncio
import logging
import sys

from config import change_ip, settings
from sheets import SheetsManager
from parser import AdParser
from queue_manager import QueueManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


async def main():
    logger.info("Starting flipping_cian...")
    try:
        # Initialize Google Sheets Manager
        sheets_manager = SheetsManager(
            spreadsheet_id=settings.spreadsheet_id,
            credentials_path=settings.credentials_path,
        )

        # Initialize Parser
        parser = AdParser()

        # Initialize Queue Manager (concurrency 2 to avoid aggressive rate limits)
        queue_manager = QueueManager(
            sheets_manager=sheets_manager, parser=parser, concurrency=2
        )

        # 1. Fetch search URLs from Google Sheets (URLS tab)
        logger.info("Fetching search URLs from Google Sheets...")
        search_urls = sheets_manager.get_urls()
        logger.info(f"Found {len(search_urls)} search URLs to process.")

        if not search_urls:
            logger.info("Finished flipping_cian execution. No URLs found.")
            return

        # 2. Extract top 3 individual ad URLs from each search URL using CianParser
        import sys
        import os

        # Add cianparser to path to allow importing it properly
        sys.path.insert(
            0, os.path.abspath(os.path.join(os.path.dirname(__file__), "cianparser"))
        )
        import cianparser
        import re
        from cianparser.flat.list import FlatListPageParser

        all_ad_urls = []
        for i, search_url in enumerate(search_urls, 1):
            logger.info(f"--- Processing search URL {i}/{len(search_urls)} ---")
            change_ip()

            try:
                parser = cianparser.CianParser(
                    location="Москва", proxies=[settings.http_proxy]
                )

                deal_type = "sale" if "sale" in search_url else "rent"

                # Use cianparser's native FlatListPageParser instead of BeautifulSoup
                parser.__parser__ = FlatListPageParser(
                    session=parser.__session__,
                    accommodation_type="flat",
                    deal_type=deal_type,
                    rent_period_type=None,
                    location_name="Москва",
                    with_saving_csv=False,
                    with_extra_data=False,
                    additional_settings={"start_page": 1, "end_page": 1},
                )

                # Append or replace page parameter format for cianparser
                if "&p=" in search_url or "?p=" in search_url:
                    url_format = re.sub(r"([&?])p=\d+", r"\g<1>p={}", search_url)
                else:
                    separator = "&" if "?" in search_url else "?"
                    url_format = search_url + f"{separator}p={{}}"

                # Start the native parser
                parser.__run__(url_format)

                # Retrieve parsed data
                parsed_data = parser.__parser__.result
                extracted_links = [item["url"] for item in parsed_data if "url" in item]

                top_3 = extracted_links[:3]
                logger.info(
                    f"Found {len(extracted_links)} ad links, taking top {len(top_3)}: {top_3}"
                )
                all_ad_urls.extend(top_3)

            except Exception as e:
                logger.error(f"Failed to extract ad URLs from {search_url}: {e}")

        # 3. Add individual Ad URLs to Queue and Process with Firecrawl
        if all_ad_urls:
            logger.info(
                f"Total individual Ad URLs to parse via Firecrawl: {len(all_ad_urls)}"
            )
            await queue_manager.run(all_ad_urls)

        logger.info("Finished flipping_cian execution.")

    except Exception as e:
        logger.error(f"Critical error in main flow: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(main())
