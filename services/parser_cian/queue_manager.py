"""
services.parser_cian.queue_manager - Asynchronous queue management

Управляет асинхронной очередью URL для парсинга с ограничением concurrency.
Каждый Worker: парсит URL → обновляет SQLite БД → пишет в нужные табы Sheets с цветом.
"""

import asyncio
import json
import logging
import os
import httpx
from typing import List, Callable, Optional, Dict, Any
from datetime import datetime

from services.parser_cian.parser import AdParser
from services.parser_cian.models import ParsedAdData, parse_to_sheets_row
from services.parser_cian.db.repository import DatabaseRepository
from services.parser_cian.config import settings

PARSED_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "parsed_data")
os.makedirs(PARSED_DATA_DIR, exist_ok=True)

logger = logging.getLogger(__name__)


def check_signals(price_history: List[Dict[str, Any]]) -> str:
    """Проверяет объявление на условия Signals_Parser (OR логика).

    Returns:
        Строка-причина для колонки reason в Offers_Parser:
        - ""                                — не сработало
        - "max_drop>=5.0%"                  — есть снижение >= 5%
        - "drops>=3"                        — >= 3 снижений за 30 дней
        - "drops>=3 AND max_drop>=5.0%"     — оба критерия
    """
    if not price_history:
        return ""

    now = datetime.now().date()
    drop_count_30d = 0
    max_drop_pct = 0.0

    _MONTHS_RU = {
        "янв": 1, "фев": 2, "мар": 3, "апр": 4,
        "май": 5, "мая": 5, "июн": 6, "июл": 7,
        "авг": 8, "сен": 9, "окт": 10, "ноя": 11, "дек": 12,
    }

    def _parse_date(date_str: str):
        if not date_str:
            return None
        date_str = date_str.strip()
        if "-" in date_str:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                return None
        parts = date_str.split()
        if len(parts) >= 3:
            try:
                day = int(parts[0])
                mon = _MONTHS_RU.get(parts[1].lower()[:3])
                year = int(parts[2])
                if mon:
                    return datetime(year, mon, day).date()
            except (ValueError, IndexError):
                pass
        return None

    for entry in price_history:
        if entry.get("change_type") != "decrease":
            continue

        price = entry.get("price") or 0
        change = entry.get("change_amount") or 0
        abs_change = abs(change)

        if price > 0 and abs_change > 0:
            prev_price = price + abs_change
            drop_pct = abs_change / prev_price * 100
            if drop_pct > max_drop_pct:
                max_drop_pct = drop_pct

        entry_date = _parse_date(entry.get("date"))
        if entry_date and (now - entry_date).days <= 30:
            drop_count_30d += 1

    has_large_drop = max_drop_pct >= 5.0
    has_many_drops = drop_count_30d >= 3

    if has_many_drops and has_large_drop:
        return f"drops>={drop_count_30d} AND max_drop>={max_drop_pct:.1f}%"
    if has_large_drop:
        return f"max_drop>={max_drop_pct:.1f}%"
    if has_many_drops:
        return f"drops>={drop_count_30d}"
    return ""


async def send_telegram_notification(message: str) -> None:
    """Отправляет уведомление в Telegram, если настроены токен и ID чата."""
    token = settings.tg_bot_token
    chat_id = settings.tg_chat_id
    
    if not token or not chat_id:
        return
        
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=10.0)
            if resp.status_code != 200:
                logger.warning(f"Telegram API warning: {resp.text}")
    except Exception as e:
        logger.error(f"Error sending Telegram notification: {e}")


def _save_parsed_json(parsed_dict: Dict[str, Any], cian_id: Optional[str]) -> None:
    """Сохраняет спарсенные данные в parsed_data/data_{cian_id}.json."""
    fname = f"data_{cian_id or 'unknown'}.json"
    path = os.path.join(PARSED_DATA_DIR, fname)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(parsed_dict, f, ensure_ascii=False, indent=2)
        logger.debug(f"Saved parsed data to {path}")
    except Exception as e:
        logger.warning(f"Failed to save parsed JSON to {path}: {e}")


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
        mode: str = "regular",
    ):
        self.parser = parser
        self.sheets_manager = sheets_manager
        self.db_repo = db_repo
        self.on_data_parsed = on_data_parsed
        self.concurrency = concurrency
        self.mode = mode
        self.queue: asyncio.Queue = asyncio.Queue()
        
        self.processed_count = 0
        self.error_count = 0
        self._sheets_lock = asyncio.Lock()
        
        logger.info(f"QueueManager initialized with concurrency={concurrency}")

    DEACTIVATED_COLOR = settings.sheet_deactivated_color

    async def worker(self, worker_id: int) -> None:
        """
        Worker процесс:
        1. Парсит URL
        2. Обновляет БД (sold / active)
        3. Размещает строку в нужные табы Sheets по критериям.

        Критерии попадания в табы:
        - Avans (mode=avans): unique_views >= 200.
          Снятые с публикации, подходящие под критерий → Avans + SOLD, цвет #B5D6A8.
        - Offers_Parser (mode=regular): все объявления; цвет при unique_views >= 200.
        - Signals_Parser (mode=regular): снижение >= 5% ИЛИ >= 3 снижений за 30 дней.
          Снятые с публикации, подходящие под критерий → соответствующий таб + SOLD, цвет #B5D6A8.
        """
        while True:
            url = await self.queue.get()

            try:
                logger.info(f"🔧 [Worker-{worker_id}] Взял из очереди: {url}")

                parsed_data: ParsedAdData = await self.parser.parse_async(url)

                parsed_dict = parsed_data.model_dump(mode="json")
                is_active = parsed_data.is_active

                # Вычисляем reason (signals) для колонки в Offers_Parser
                signal_reason = check_signals(parsed_dict.get("price_history", []))
                signals_match = bool(signal_reason)

                row = parse_to_sheets_row(parsed_data, reason=signal_reason)

                # Сохраняем JSON в parsed_data/
                _save_parsed_json(parsed_dict, parsed_data.cian_id)

                loop = asyncio.get_event_loop()
                success = False

                if is_active is False:
                    publish_date = parsed_data.publish_date or ""
                    await self.db_repo.move_to_sold(url, parsed_dict, publish_date)
                else:
                    await self.db_repo.update_active_ad(url, parsed_dict)

                if self.mode == "avans":
                    meets_avans = (
                        parsed_data.unique_views is not None
                        and parsed_data.unique_views >= settings.min_unique_views
                    )

                    if meets_avans:
                        avans_color = self.DEACTIVATED_COLOR if is_active is False else settings.sheet_highlight_color
                        logger.info(
                            f"💾 [Worker-{worker_id}] Avans match (views={parsed_data.unique_views}, "
                            f"active={is_active}): {url}"
                        )
                        async with self._sheets_lock:
                            success = await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.find_and_update_row(
                                    "Avans", row, id_value=parsed_data.cian_id,
                                    id_column_index=20, bg_color=avans_color,
                                ),
                            )
                        msg = (
                            f"🌟 <b>Avans Match!</b>\n\n"
                            f"Уникальных просмотров сегодня: {parsed_data.unique_views}\n"
                            f"Цена: {parsed_data.price} руб.\n"
                            f"Активно: {'Да' if is_active else 'Нет (снято)'}\n"
                            f"Ссылка: <a href='{url}'>{url}</a>"
                        )
                        asyncio.create_task(send_telegram_notification(msg))
                    else:
                        logger.info(
                            f"⏭️ [Worker-{worker_id}] Avans skip (views={parsed_data.unique_views}): {url}"
                        )
                        success = True

                    if is_active is False:
                        async with self._sheets_lock:
                            await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.find_and_update_row(
                                    "SOLD", row, id_value=parsed_data.cian_id, id_column_index=20,
                                ),
                            )

                else:
                    # ── mode = regular ──
                    views_match = (
                        parsed_data.unique_views is not None
                        and parsed_data.unique_views >= settings.min_unique_views
                    )

                    if is_active is False:
                        async with self._sheets_lock:
                            await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.find_and_update_row(
                                    "SOLD", row, id_value=parsed_data.cian_id, id_column_index=20,
                                ),
                            )

                        offers_color = self.DEACTIVATED_COLOR if views_match else None
                        signals_color = self.DEACTIVATED_COLOR if signals_match else None

                        async with self._sheets_lock:
                            success = await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.sync_offers_and_signals(
                                    row,
                                    str(parsed_data.cian_id),
                                    20,
                                    offers_color,
                                    signals_match,
                                    signals_color or settings.sheet_highlight_color,
                                ),
                            )

                        if views_match or signals_match:
                            logger.info(
                                f"💾 [Worker-{worker_id}] Снято, но подходит "
                                f"(views={views_match}, signals={signal_reason}): {url}"
                            )

                    else:
                        offers_color = settings.sheet_highlight_color if views_match else None

                        if views_match:
                            msg = (
                                f"🌟 <b>Offers_Parser Match!</b>\n\n"
                                f"Уникальных просмотров сегодня: {parsed_data.unique_views}\n"
                                f"Цена: {parsed_data.price} руб.\n"
                                f"Ссылка: <a href='{url}'>{url}</a>"
                            )
                            asyncio.create_task(send_telegram_notification(msg))

                        if signals_match:
                            msg_sig = (
                                f"🚦 <b>Signals_Parser Match!</b>\n\n"
                                f"Reason: {signal_reason}\n"
                                f"Цена: {parsed_data.price} руб.\n"
                                f"Ссылка: <a href='{url}'>{url}</a>"
                            )
                            asyncio.create_task(send_telegram_notification(msg_sig))

                        async with self._sheets_lock:
                            success = await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.sync_offers_and_signals(
                                    row,
                                    str(parsed_data.cian_id),
                                    20,
                                    offers_color,
                                    signals_match,
                                    settings.sheet_highlight_color,
                                ),
                            )

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
