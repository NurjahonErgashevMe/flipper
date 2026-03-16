"""
services.parser_cian.queue_manager - Asynchronous queue management

Управляет асинхронной очередью URL для парсинга с ограничением concurrency.
Каждый Worker: парсит URL → конвертирует в строку → записывает в Sheets.
"""

import asyncio
import logging
from typing import List, Callable, Optional

from parser import AdParser
from models import ParsedAdData, parse_to_sheets_row

logger = logging.getLogger(__name__)


class QueueManager:
    """
    Менеджер асинхронной очереди для парсинга URLs.
    
    Особенности:
    - Ограниченный concurrency для защиты от rate limits
    - Callback функция для обработки распарсенных данных
    - Graceful shutdown с отменой всех задач
    
    Example:
        ```python
        queue_manager = QueueManager(parser, sheets_manager, concurrency=2)
        await queue_manager.run(urls)
        ```
    """

    def __init__(
        self,
        parser: AdParser,
        sheets_manager,  # SheetsManager
        on_data_parsed: Optional[Callable[[ParsedAdData], None]] = None,
        concurrency: int = 2,
    ):
        """
        Args:
            parser: Экземпляр AdParser для парсинга URLs
            sheets_manager: Экземпляр SheetsManager для сохранения данных
            on_data_parsed: Optional callback функция при успешном парсинге
            concurrency: Максимальное количество одновременных workers
                Рекомендуется 2 для защиты от rate limits Cian
        """
        self.parser = parser
        self.sheets_manager = sheets_manager
        self.on_data_parsed = on_data_parsed
        self.concurrency = concurrency
        self.queue: asyncio.Queue = asyncio.Queue()
        
        # Статистика
        self.processed_count = 0
        self.error_count = 0
        
        logger.info(f"QueueManager initialized with concurrency={concurrency}")

    async def worker(self, worker_id: int) -> None:
        """
        Worker процесс, обрабатывающий URLs из очереди.
        
        Работает в бесконечном цикле, беря URLs из очереди:
        1. Парсит URL через AdParser
        2. Преобразует данные в строку для Sheets через адаптер
        3. Сохраняет в Google Sheets
        4. Вызывает callback если установлен
        
        Args:
            worker_id: Идентификатор worker процесса для логирования
        """
        while True:
            url = await self.queue.get()
            
            try:
                logger.info(f"[Worker {worker_id}] Processing: {url}")

                # 1. Парсим данные
                parsed_data: ParsedAdData = await self.parser.parse_async(url)

                # 2. Преобразуем в строку для Google Sheets
                row = parse_to_sheets_row(parsed_data)

                # 3. Сохраняем в Sheets (I/O в отдельном потоке)
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(
                    None, 
                    self.sheets_manager.write_row, 
                    "RESULTS",  # Tab name
                    row
                )

                if success:
                    logger.info(f"[Worker {worker_id}] ✓ Saved {url}")
                    self.processed_count += 1
                    
                    # Вызываем callback если есть
                    if self.on_data_parsed:
                        self.on_data_parsed(parsed_data)
                else:
                    logger.error(f"[Worker {worker_id}] Failed to save {url} to Sheets")
                    self.error_count += 1

            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error processing {url}: {type(e).__name__}: {e}")
                self.error_count += 1

            finally:
                self.queue.task_done()

    async def run(self, urls: List[str]) -> dict:
        """
        Запускает обработку списка URLs асинхронно с ограничением concurrency.
        
        Процесс:
        1. Добавляет все URLs в очередь
        2. Запускает N workers (где N = concurrency)
        3. Workers обрабатывают очередь параллельно
        4. Ждет пока все tasks завершатся (queue.join())
        5. Останавливает workers
        
        Args:
            urls: Список URLs для парсинга
            
        Returns:
            dict со статистикой:
                {
                    "total": int,
                    "processed": int,
                    "errors": int,
                    "success_rate": float
                }
        """
        if not urls:
            logger.info("No URLs to process")
            return {
                "total": 0,
                "processed": 0,
                "errors": 0,
                "success_rate": 0.0
            }

        logger.info(f"Starting queue processing: {len(urls)} URLs, {self.concurrency} workers")

        # Добавляем все URLs в очередь
        for url in urls:
            await self.queue.put(url)

        # Запускаем workers
        tasks = [
            asyncio.create_task(self.worker(i)) 
            for i in range(self.concurrency)
        ]

        try:
            # Ждем пока все URLs будут обработаны
            await self.queue.join()
            logger.info("Queue processing completed")

        except asyncio.CancelledError:
            logger.warning("Queue processing cancelled")
            raise

        finally:
            # Останавливаем workers
            for task in tasks:
                task.cancel()

            # Ждем завершения всех tasks (с обработкой CancelledError)
            await asyncio.gather(*tasks, return_exceptions=True)

            logger.info("Workers stopped")

        # Возвращаем статистику
        total = len(urls)
        success_rate = (self.processed_count / total * 100) if total > 0 else 0.0

        stats = {
            "total": total,
            "processed": self.processed_count,
            "errors": self.error_count,
            "success_rate": round(success_rate, 2)
        }

        logger.info(f"Queue statistics: {stats}")
        return stats
