"""
services.parser_cian.queue_manager - Asynchronous queue management

Управляет асинхронной очередью URL для парсинга с ограничением concurrency.
Каждый Worker: парсит URL → обновляет SQLite БД → пишет в нужные табы Sheets с цветом.
"""

import asyncio
import logging
from typing import List, Callable, Optional, Dict, Any
from datetime import datetime

from services.parser_cian.parser import AdParser
from services.parser_cian.models import ParsedAdData, parse_to_sheets_row
from services.parser_cian.db.repository import DatabaseRepository
from services.parser_cian.config import settings

logger = logging.getLogger(__name__)


def check_signals(price_history: List[Dict[str, Any]]) -> bool:
    """Проверяет объявление на условия Signals_Parser:
    - Снижение цены >= 5% (хотя бы одно)
    - Снижений цены >= 3 раз за последние 30 дней
    """
    if not price_history:
        return False
        
    now = datetime.now().date()
    drop_count_30d = 0
    has_large_drop = False
    
    for entry in price_history:
        if entry.get("change_type") == "decrease":
            price = entry.get("price") or 0
            change = entry.get("change_amount") or 0
            # change_amount обычно отрицательный для decrease в моделях
            abs_change = abs(change)
            
            if price > 0:
                prev_price = price + abs_change
                drop_percent = abs_change / prev_price
                if drop_percent >= 0.05:
                    has_large_drop = True
            
            date_str = entry.get("date")
            if date_str:
                try:
                    # Обработка разных форматов даты (Y-m-d или d m Y)
                    if '-' in date_str:
                        entry_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    else:
                        # Если дата в формате "19 мар 2026", нам нужно ее распарсить или привести к стандарту
                        # Для простоты в моке используем ISO, а для парсера CIAN он возвращает ISO
                        continue 
                        
                    if (now - entry_date).days <= 30:
                        drop_count_30d += 1
                except ValueError:
                    pass
                    
    return has_large_drop and (drop_count_30d >= 3)


class QueueManager:
    """
    Менеджер асинхронной очереди для парсинга URLs с трекингом в БД.
    """

    def __init__(
        self,
        parser: AdParser,
        sheets_manager,
        db_repo: DatabaseRepository,
        on_data_parsed: Optional[Callable[[ParsedAdData], None]] = None,
        concurrency: int = 2,
    ):
        self.parser = parser
        self.sheets_manager = sheets_manager
        self.db_repo = db_repo
        self.on_data_parsed = on_data_parsed
        self.concurrency = concurrency
        self.queue: asyncio.Queue = asyncio.Queue()
        
        self.processed_count = 0
        self.error_count = 0
        self._sheets_lock = asyncio.Lock()
        
        logger.info(f"QueueManager initialized with concurrency={concurrency}")

    async def worker(self, worker_id: int) -> None:
        """
        Worker процесс:
        1. Парсит URL
        2. Если продано -> SQLite (sold) + Sheets (SOLD)
        3. Если активно -> SQLite (active) + Sheets (Offers_Parser, Signals_Parser) с цветами.
        """
        while True:
            url = await self.queue.get()
            
            try:
                logger.info(f"🔧 [Worker-{worker_id}] Взял из очереди: {url}")

                # 1. Парсим данные
                parsed_data: ParsedAdData = await self.parser.parse_async(url)
                
                # Подготавливаем данные для БД и Sheets
                parsed_dict = parsed_data.model_dump(mode='json')  
                row = parse_to_sheets_row(parsed_data)
                
                is_active = parsed_data.is_active

                # 2. Обработка результатов
                if is_active is False:
                    # ОБЪЯВЛЕНИЕ ПРОДАНО
                    logger.info(f"💾 [Worker-{worker_id}] Объявление снято. Перемещаю в SOLD: {url}")
                    
                    # Пишем в БД
                    publish_date = parsed_data.publish_date or ""
                    await self.db_repo.move_to_sold(url, parsed_dict, publish_date)
                    
                    # Пишем в Sheets "SOLD"
                    loop = asyncio.get_event_loop()
                    async with self._sheets_lock:
                        success = await loop.run_in_executor(
                            None,
                            lambda: self.sheets_manager.find_and_update_row(
                                "SOLD", row, id_value=parsed_data.cian_id, id_column_index=20
                            )
                        )
                    
                else:
                    # ОБЪЯВЛЕНИЕ АКТИВНО (или None при ошибке извлечения статуса - считаем активным)
                    logger.info(f"💾 [Worker-{worker_id}] Обновляю активное объявление в БД и Sheets: {url}")
                    
                    # Обновляем БД
                    await self.db_repo.update_active_ad(url, parsed_dict)
                    
                    # Определяем цвета:
                    # Offers_Parser: цвет выделения если unique_views >= min_unique_views
                    offers_color = None
                    if parsed_data.unique_views and parsed_data.unique_views >= settings.min_unique_views:
                        offers_color = settings.sheet_highlight_color
                        
                    # Signals_Parser: цвет выделения если check_signals == True
                    signals_color = None
                    if check_signals(parsed_dict.get("price_history", [])):
                        signals_color = settings.sheet_highlight_color
                        
                    # Пишем в Sheets "Offers_Parser"
                    loop = asyncio.get_event_loop()
                    async with self._sheets_lock:
                        success1 = await loop.run_in_executor(
                            None,
                            lambda: self.sheets_manager.find_and_update_row(
                                "Offers_Parser", row, id_value=parsed_data.cian_id, id_column_index=20, bg_color=offers_color
                            )
                        )
                        
                        # Пишем в Sheets "Signals_Parser"
                        success2 = await loop.run_in_executor(
                            None,
                            lambda: self.sheets_manager.find_and_update_row(
                                "Signals_Parser", row, id_value=parsed_data.cian_id, id_column_index=20, bg_color=signals_color
                            )
                        )
                    
                    success = success1 or success2

                if success:
                    logger.info(f"✅ [Worker-{worker_id}] Успешно обработано: {url} | ID: {parsed_data.cian_id}")
                    self.processed_count += 1
                    
                    if self.on_data_parsed:
                        self.on_data_parsed(parsed_data)
                else:
                    logger.error(f"❌ [Worker-{worker_id}] Ошибка при сохранении в Sheets: {url}")
                    self.error_count += 1

            except Exception as e:
                logger.error(f"❌ [Worker-{worker_id}] Ошибка при обработке {url}: {type(e).__name__}: {e}")
                self.error_count += 1

            finally:
                self.queue.task_done()

    async def run(self, urls: List[str]) -> dict:
        if not urls:
            logger.info("No URLs to process")
            return {"total": 0, "processed": 0, "errors": 0, "success_rate": 0.0}

        logger.info(f"🚀 Запускаю обработку очереди: {len(urls)} URLs, {self.concurrency} воркеров")

        for url in urls:
            await self.queue.put(url)
        
        tasks = [asyncio.create_task(self.worker(i)) for i in range(self.concurrency)]

        try:
            await self.queue.join()
            logger.info("✅ Обработка очереди завершена")
        except asyncio.CancelledError:
            logger.warning("⚠️ Обработка очереди отменена")
            raise
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        total = len(urls)
        success_rate = (self.processed_count / total * 100) if total > 0 else 0.0

        return {
            "total": total,
            "processed": self.processed_count,
            "errors": self.error_count,
            "success_rate": round(success_rate, 2)
        }
