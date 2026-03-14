import asyncio
import logging
from typing import List

from parser import AdParser
from sheets import SheetsManager

logger = logging.getLogger(__name__)


class QueueManager:
    def __init__(
        self, sheets_manager: SheetsManager, parser: AdParser, concurrency: int = 2
    ):
        self.sheets_manager = sheets_manager
        self.parser = parser
        self.concurrency = concurrency
        self.queue = asyncio.Queue()

    async def worker(self, worker_id: int):
        while True:
            url = await self.queue.get()
            try:
                logger.info(f"Worker {worker_id} processing {url}")
                # Parse the URL
                parsed_data = await self.parser.parse_async(url)

                # Convert to row
                row = parsed_data.to_row()

                # Save to sheets (running sync IO in executor)
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(
                    None, self.sheets_manager.write_parsed_row, row
                )

                if success:
                    logger.info(f"Successfully processed and saved {url}")
                else:
                    logger.error(f"Failed to save {url} to Google Sheets")
            except Exception as e:
                logger.error(f"Worker {worker_id} error processing {url}: {e}")
            finally:
                self.queue.task_done()

    async def run(self, urls: List[str]):
        """Add URLs to the queue and run workers to process them"""
        if not urls:
            logger.info("No URLs to process.")
            return

        for url in urls:
            await self.queue.put(url)

        # Create worker tasks
        tasks = []
        for i in range(self.concurrency):
            task = asyncio.create_task(self.worker(i))
            tasks.append(task)

        # Wait until the queue is fully processed
        await self.queue.join()

        # Cancel worker tasks
        for task in tasks:
            task.cancel()

        # Wait until all workers are cancelled
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("All URLs processed successfully.")
