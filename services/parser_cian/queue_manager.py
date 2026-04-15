"""
services.parser_cian.queue_manager - Asynchronous queue management

Manages async URL queue for parsing with concurrency limits.
Worker: parse URL -> update DB -> write to Sheets tabs with color coding.
"""

import asyncio
import logging
import re
from typing import List, Optional, Dict, Any
from datetime import datetime, date

import httpx

from services.parser_cian.parser import AdParser
from services.parser_cian.models import ParsedAdData, parse_to_sheets_row
from services.parser_cian.db.repository import DatabaseRepository
from services.parser_cian.config import settings

logger = logging.getLogger(__name__)

_AVANS_POS_PATTERNS = [
    re.compile(r"за\s+объект\s+(?:уже\s+)?внес[её]н\s+(?:аванс|задаток)", re.I),
    re.compile(r"за\s+квартир[ау]\s+(?:уже\s+)?внес[её]н\s+(?:аванс|задаток)", re.I),
    re.compile(r"внес[её]н\s+(?:аванс|задаток)", re.I),
    re.compile(r"принят\s+задаток", re.I),
    re.compile(r"получен\s+аванс", re.I),
    re.compile(r"квартир[ау]\s+забронирован[ао]", re.I),
    re.compile(r"объект\s+забронирован", re.I),
    re.compile(r"обеспечительн\w*\s+плат[её]ж\s+(?:внес[её]н|получен)", re.I),
]

_AVANS_NEG_PATTERNS = [
    re.compile(r"(?:аванс|задаток)\s+не\s+(?:бер[еу]|нужен|требуетс[яь])", re.I),
    re.compile(r"без\s+(?:аванса|задатка)", re.I),
]

_cookie_alert_sent = False


# ---------------------------------------------------------------------------
# Signal detection
# ---------------------------------------------------------------------------

def check_signals(price_history: List[Dict[str, Any]]) -> str:
    """Check ad for Signals_Parser criteria (OR logic).

    Returns reason string (empty = no match).
    """
    if not price_history:
        return ""

    for e in price_history:
        if e.get("change_type") == "increase":
            try:
                if int(e.get("change_amount") or 0) != 0:
                    return ""
            except Exception:
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


# ---------------------------------------------------------------------------
# Telegram notifications
# ---------------------------------------------------------------------------

async def send_telegram_notification(message: str) -> None:
    token = settings.tg_bot_token
    chat_id = settings.tg_chat_id
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=10.0)
            if resp.status_code != 200:
                logger.warning("Telegram API: %s", resp.text)
    except Exception as e:
        logger.error("Telegram send error: %s", e)


def _build_tg_message(parsed: ParsedAdData, event: str, reason: str = "") -> str:
    price_str = f"{parsed.price / 1_000_000:.1f} млн" if parsed.price else "? руб."
    area_str = f"{parsed.area} м\u00b2" if parsed.area else ""
    district = parsed.address.district if parsed.address else ""
    parts = [p for p in [price_str, area_str, district] if p]
    info_line = " | ".join(parts)
    lines = [f"<b>{event}</b>", info_line]
    if reason:
        lines.append(reason)
    lines.append(parsed.url or "")
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_recently_published(publish_date_str: Optional[str], max_days: int) -> bool:
    if not publish_date_str:
        return False
    try:
        pub = datetime.strptime(publish_date_str.strip(), "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        return False
    return (date.today() - pub).days <= max_days


def _is_within_days(
    publish_date_str: Optional[str], days_in_exposition: Optional[int], max_days: int
) -> bool:
    if days_in_exposition is not None:
        try:
            return int(days_in_exposition) <= max_days
        except (TypeError, ValueError):
            pass
    return _is_recently_published(publish_date_str, max_days)


def _is_avans_deposit(parsed: ParsedAdData) -> bool:
    if parsed.has_avans_deposit is not None:
        return parsed.has_avans_deposit
    haystack = " ".join(
        s for s in (parsed.title or "", parsed.description or "") if s
    ).lower()
    if any(p.search(haystack) for p in _AVANS_NEG_PATTERNS):
        return False
    return any(p.search(haystack) for p in _AVANS_POS_PATTERNS)


# ---------------------------------------------------------------------------
# QueueManager
# ---------------------------------------------------------------------------

class QueueManager:
    DEACTIVATED_COLOR = settings.sheet_deactivated_color
    HIGHLIGHT_COLOR = settings.sheet_highlight_color

    def __init__(
        self,
        parser: AdParser,
        sheets_manager,
        db_repo: DatabaseRepository,
        concurrency: int = 2,
        mode: str = "offers",
    ):
        self.parser = parser
        self.sheets_manager = sheets_manager
        self.db_repo = db_repo
        self.concurrency = concurrency
        self.mode = mode
        self.queue: asyncio.Queue = asyncio.Queue()
        self.processed_count = 0
        self.error_count = 0
        self._sheets_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Worker
    # ------------------------------------------------------------------

    async def worker(self, worker_id: int) -> None:
        global _cookie_alert_sent
        while True:
            url = await self.queue.get()
            try:
                await self._process_url(worker_id, url)
            except ValueError as exc:
                err_msg = str(exc).lower()
                if "cookie" in err_msg or "authentication" in err_msg:
                    logger.error("[Worker-%s] Cookies invalid for %s: %s", worker_id, url, exc)
                    if not _cookie_alert_sent:
                        _cookie_alert_sent = True
                        asyncio.create_task(send_telegram_notification(
                            "<b>Куки слетели</b>\nЗапущен recovery. Парсинг приостановлен до восстановления."
                        ))
                else:
                    logger.error("[Worker-%s] ValueError for %s: %s", worker_id, url, exc)
                self.error_count += 1
            except Exception as exc:
                logger.error("[Worker-%s] Error processing %s: %s: %s", worker_id, url, type(exc).__name__, exc)
                self.error_count += 1
            finally:
                self.queue.task_done()

    async def _process_url(self, worker_id: int, url: str) -> None:
        logger.info("[Worker-%s] Processing: %s", worker_id, url)

        parsed_data, parsed_dict = await self._parse_with_retry(worker_id, url)
        if parsed_data is None:
            return

        signal_reason = check_signals(parsed_dict.get("price_history", []))
        signals_match = bool(signal_reason)
        row = parse_to_sheets_row(parsed_data, reason=signal_reason)
        loop = asyncio.get_event_loop()

        sold_tab = (
            settings.sheet_tab_avans_sold if self.mode == "avans"
            else settings.sheet_tab_sold
        )

        if self.mode == "avans":
            success = await self._handle_avans(worker_id, url, parsed_data, parsed_dict, row, sold_tab)
        else:
            success = await self._handle_offers(
                worker_id, url, parsed_data, parsed_dict, row, sold_tab,
                signals_match, signal_reason, loop,
            )

        if success:
            logger.info("[Worker-%s] OK: %s (ID: %s)", worker_id, url, parsed_data.cian_id)
            self.processed_count += 1
        else:
            logger.error("[Worker-%s] Sheets write failed: %s", worker_id, url)
            self.error_count += 1

    # ------------------------------------------------------------------
    # Parse with retry (empty-data protection)
    # ------------------------------------------------------------------

    async def _parse_with_retry(self, worker_id: int, url: str):
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            parsed_data = await self.parser.parse_async(url)
            parsed_dict = parsed_data.model_dump(mode="json")

            cian_id_ok = bool((parsed_data.cian_id or "").strip())
            price_ok = isinstance(parsed_data.price, int) and parsed_data.price > 0
            area_ok = parsed_data.area is not None and float(parsed_data.area) > 0

            if cian_id_ok and (price_ok or area_ok):
                return parsed_data, parsed_dict

            if attempt < max_attempts:
                logger.warning(
                    "[Worker-%s] Empty data (attempt %s/%s) for %s: cian_id=%s price=%s area=%s",
                    worker_id, attempt, max_attempts, url,
                    parsed_data.cian_id, parsed_data.price, parsed_data.area,
                )
                await asyncio.sleep(1.5 * attempt)
            else:
                logger.error(
                    "[Worker-%s] Empty data after %s attempts for %s — removing from DB",
                    worker_id, max_attempts, url,
                )
                await self.db_repo.delete_active_ad(url)

        return None, None

    # ------------------------------------------------------------------
    # Mode: avans
    # ------------------------------------------------------------------

    async def _handle_avans(self, worker_id, url, parsed_data, parsed_dict, row, sold_tab) -> bool:
        has_avans = _is_avans_deposit(parsed_data)
        within_week = _is_within_days(
            parsed_data.publish_date, parsed_data.days_in_exposition,
            settings.sold_max_age_days,
        )
        publish_date = parsed_data.publish_date or ""
        loop = asyncio.get_event_loop()

        if parsed_data.is_active is False:
            await self.db_repo.move_to_sold(url, parsed_dict, publish_date)
            async with self._sheets_lock:
                await loop.run_in_executor(None, lambda: self.sheets_manager.delete_row_by_id(
                    settings.sheet_tab_avans, id_value=parsed_data.cian_id, id_column_index=20,
                ))
            logger.info("[Worker-%s] Deactivated -> removed from Avans + DB: %s", worker_id, url)
            return True

        if has_avans:
            await self.db_repo.move_to_sold(url, parsed_dict, publish_date)
            if within_week:
                async with self._sheets_lock:
                    await loop.run_in_executor(None, lambda: self.sheets_manager.find_and_update_row(
                        settings.sheet_tab_avans_sold, row,
                        id_value=parsed_data.cian_id, id_column_index=20,
                    ))
                asyncio.create_task(send_telegram_notification(
                    _build_tg_message(parsed_data, "Аванс внесён", f"Дней в каталоге: {parsed_data.days_in_exposition or '?'}")
                ))
            async with self._sheets_lock:
                await loop.run_in_executor(None, lambda: self.sheets_manager.delete_row_by_id(
                    settings.sheet_tab_avans, id_value=parsed_data.cian_id, id_column_index=20,
                ))
            logger.info("[Worker-%s] Avans deposit -> Avans_Prodano + removed from Avans: %s", worker_id, url)
            return True

        # Active, no deposit — keep tracking
        await self.db_repo.update_active_ad(url, parsed_dict)
        async with self._sheets_lock:
            ok = await loop.run_in_executor(None, lambda: self.sheets_manager.find_and_update_row(
                settings.sheet_tab_avans, row,
                id_value=parsed_data.cian_id, id_column_index=20,
            ))
        return bool(ok)

    # ------------------------------------------------------------------
    # Mode: offers
    # ------------------------------------------------------------------

    async def _handle_offers(
        self, worker_id, url, parsed_data, parsed_dict, row, sold_tab,
        signals_match, signal_reason, loop,
    ) -> bool:
        is_active = parsed_data.is_active
        publish_date = parsed_data.publish_date or ""

        if is_active is False:
            await self.db_repo.move_to_sold(url, parsed_dict, publish_date)
        else:
            await self.db_repo.update_active_ad(url, parsed_dict)

        views_match = (
            parsed_data.unique_views is not None
            and parsed_data.unique_views >= settings.min_unique_views
        )
        within_week = _is_within_days(
            parsed_data.publish_date, parsed_data.days_in_exposition,
            settings.sold_max_age_days,
        )

        if is_active is False:
            if within_week:
                async with self._sheets_lock:
                    await loop.run_in_executor(None, lambda: self.sheets_manager.find_and_update_row(
                        sold_tab, row, id_value=parsed_data.cian_id, id_column_index=20,
                    ))
                asyncio.create_task(send_telegram_notification(
                    _build_tg_message(parsed_data, "Продано", f"Дней в каталоге: {parsed_data.days_in_exposition or '?'}")
                ))

            async with self._sheets_lock:
                result = await loop.run_in_executor(None, lambda: self.sheets_manager.sync_offers_and_signals(
                    row, str(parsed_data.cian_id), 20,
                    self.DEACTIVATED_COLOR, signals_match, self.DEACTIVATED_COLOR,
                    True, True,
                ))
            return bool(result.get("offers_ok"))

        # Active ad
        highlight_ok = views_match and (parsed_data.is_active is True)
        offers_color = self.HIGHLIGHT_COLOR if highlight_ok else None
        signals_color = self.HIGHLIGHT_COLOR if highlight_ok else None

        async with self._sheets_lock:
            result = await loop.run_in_executor(None, lambda: self.sheets_manager.sync_offers_and_signals(
                row, str(parsed_data.cian_id), 20,
                offers_color, signals_match, signals_color, True,
            ))

        if result.get("signal_added"):
            asyncio.create_task(send_telegram_notification(
                _build_tg_message(parsed_data, "Signal", signal_reason)
            ))
        if result.get("signal_removed"):
            asyncio.create_task(send_telegram_notification(
                _build_tg_message(parsed_data, "Signal удалён", "Критерии больше не выполняются")
            ))

        return bool(result.get("offers_ok"))

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------

    async def run(self, urls: List[str]) -> dict:
        if not urls:
            logger.info("No URLs to process")
            return {"total": 0, "processed": 0, "errors": 0, "success_rate": 0.0}

        logger.info("Starting queue: %s URLs, %s workers", len(urls), self.concurrency)

        for url in urls:
            await self.queue.put(url)

        tasks = [asyncio.create_task(self.worker(i)) for i in range(self.concurrency)]

        try:
            await self.queue.join()
            logger.info("Queue processing complete")
        except asyncio.CancelledError:
            logger.warning("Queue processing cancelled")
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
            "success_rate": round(success_rate, 2),
        }
