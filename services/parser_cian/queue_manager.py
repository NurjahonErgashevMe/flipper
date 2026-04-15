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
import re
from typing import List, Callable, Optional, Dict, Any
from datetime import datetime, date

from services.parser_cian.parser import AdParser
from services.parser_cian.models import ParsedAdData, parse_to_sheets_row
from services.parser_cian.db.repository import DatabaseRepository
from services.parser_cian.config import settings

PARSED_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "parsed_data")
os.makedirs(PARSED_DATA_DIR, exist_ok=True)

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

    # Анти-условие: если в истории есть повышение цены, то сигнал не подходит.
    # Иначе объявления с «дерганной» динамикой (рост→падения) будут ложно проходить по drops>=3/max_drop.
    for e in price_history:
        if e.get("change_type") == "increase":
            try:
                if int(e.get("change_amount") or 0) != 0:
                    return ""
            except Exception:
                # если тип increase есть, но change_amount не число — считаем что рост был
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


def _is_recently_published(publish_date_str: Optional[str], max_days: int) -> bool:
    """True если от publish_date до сегодня прошло не больше max_days дней."""
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
    """True если объявление "младше" max_days дней.

    Приоритет: days_in_exposition (0 = сегодня, 7 = ровно неделя).
    Fallback: publish_date (YYYY-MM-DD).
    """
    if days_in_exposition is not None:
        try:
            return int(days_in_exposition) <= max_days
        except (TypeError, ValueError):
            pass
    return _is_recently_published(publish_date_str, max_days)


def _is_avans_deposit(parsed: "ParsedAdData") -> bool:
    """True если LLM определила, что за объект внесён аванс/задаток.

    Приоритет: AI-поле has_avans_deposit (из Firecrawl schema).
    Fallback: keyword search по description/title.
    """
    if parsed.has_avans_deposit is not None:
        return parsed.has_avans_deposit

    haystack = " ".join(
        s for s in (parsed.title or "", parsed.description or "") if s
    ).lower()
    if any(p.search(haystack) for p in _AVANS_NEG_PATTERNS):
        return False
    return any(p.search(haystack) for p in _AVANS_POS_PATTERNS)


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
        mode: str = "offers",
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
    HIGHLIGHT_COLOR = settings.sheet_highlight_color

    async def worker(self, worker_id: int) -> None:
        """
        Worker процесс:
        1. Парсит URL
        2. Обновляет БД (sold / active)
        3. Размещает строку в нужные табы Sheets по критериям.

        Критерии попадания в табы:

        mode=avans:
        - Активное объявление без аванса/задатка → upsert в «Аванс» (без цвета), остаётся в БД.
        - Аванс/задаток внесён (is_active=True):
          - если объявление младше недели (days_in_exposition ≤ sold_max_age_days)
            → upsert в «Аванс_Продано», удалить из «Аванс», удалить из БД (перестать отслеживать);
          - иначе → удалить из «Аванс» + удалить из БД (без записи в «Аванс_Продано»).
        - Снято с публикации (is_active=False) → удалить из «Аванс» + удалить из БД.
        - «Продано» заполняется только в mode=offers.

        mode=offers:
        - Всегда: upsert в «Offers_Parser».
        - Подсветка в «Offers_Parser»: unique_views ≥ min_unique_views (200+) → sheet_highlight_color.
        - Signals_Parser: снижение ≥ 5% ИЛИ ≥ 3 снижений за 30 дней; строка не удаляется,
          если критерий позже перестал выполняться — продолжаем обновлять данные до снятия с публикации.
        - Снято с публикации:
          - «Продано»: только если объявление младше недели (days_in_exposition ≤ 7 / publish_date ≤ sold_max_age_days).
          - «Offers_Parser» и «Signals_Parser»: сероватый фон (sheet_deactivated_color), строки не удаляем.
          - В БД: удаляем (перестаём отслеживать).
        """
        while True:
            url = await self.queue.get()

            try:
                logger.info(f"🔧 [Worker-{worker_id}] Взял из очереди: {url}")
                # Иногда Firecrawl/страница возвращают «битую» карточку (captcha/ошибка рендера):
                # cian_id=None, price=0, area=0.0, без creationDate и т.п.
                # В этих случаях делаем 2–3 повтора. Если всё ещё пусто — удаляем из БД и не отслеживаем.
                parsed_data: ParsedAdData | None = None
                parsed_dict: Dict[str, Any] | None = None

                max_attempts = 3  # 1 + 2 повтора
                abandoned_parse = False
                for attempt in range(1, max_attempts + 1):
                    parsed_data = await self.parser.parse_async(url)
                    parsed_dict = parsed_data.model_dump(mode="json")

                    cian_id_ok = bool((parsed_data.cian_id or "").strip())
                    price_ok = isinstance(parsed_data.price, int) and parsed_data.price > 0
                    area_ok = (parsed_data.area is not None) and float(parsed_data.area) > 0

                    # Базовый критерий валидности: cian_id должен быть распознан.
                    # Иначе любая запись в Sheets бессмысленна (ID=null ломает поиск/апдейты).
                    if cian_id_ok and (price_ok or area_ok):
                        break

                    if attempt < max_attempts:
                        logger.warning(
                            "⚠️ [Worker-%s] Пустые данные (attempt %s/%s) для %s: cian_id=%s price=%s area=%s. Повтор...",
                            worker_id,
                            attempt,
                            max_attempts,
                            url,
                            parsed_data.cian_id,
                            parsed_data.price,
                            parsed_data.area,
                        )
                        await asyncio.sleep(1.5 * attempt)
                    else:
                        logger.error(
                            "❌ [Worker-%s] Пустые данные после %s попыток для %s: cian_id=%s price=%s area=%s. Удаляю из БД.",
                            worker_id,
                            max_attempts,
                            url,
                            parsed_data.cian_id,
                            parsed_data.price,
                            parsed_data.area,
                        )
                        await self.db_repo.delete_active_ad(url)
                        abandoned_parse = True
                        break

                if abandoned_parse:
                    continue

                # Доп. защита: даже если price/area заполнены, но cian_id не распознан — не пишем в Sheets вообще.
                if not parsed_data or not (parsed_data.cian_id or "").strip():
                    logger.error(
                        "❌ [Worker-%s] cian_id отсутствует после парсинга %s. Удаляю из БД, пропускаю Sheets.",
                        worker_id,
                        url,
                    )
                    await self.db_repo.delete_active_ad(url)
                    continue

                is_active = parsed_data.is_active

                # Вычисляем reason (signals) для колонки в Offers_Parser
                signal_reason = check_signals(parsed_dict.get("price_history", []))
                signals_match = bool(signal_reason)

                # Строка таблицы: A..V — данные, W — reason (без хвоста из FILTERS).
                row = parse_to_sheets_row(parsed_data, reason=signal_reason)

                # Сохраняем JSON в parsed_data/
                _save_parsed_json(parsed_dict, parsed_data.cian_id)

                loop = asyncio.get_event_loop()
                success = False

                sold_tab = (
                    settings.sheet_tab_avans_sold
                    if self.mode == "avans"
                    else settings.sheet_tab_sold
                )

                if self.mode == "avans":
                    has_avans = _is_avans_deposit(parsed_data)
                    within_week = _is_within_days(
                        parsed_data.publish_date,
                        parsed_data.days_in_exposition,
                        settings.sold_max_age_days,
                    )

                    publish_date = parsed_data.publish_date or ""

                    if is_active is False:
                        # Снято с публикации: не пишем в «Аванс_Продано». Удаляем из «Аванс» и перестаём отслеживать.
                        await self.db_repo.move_to_sold(url, parsed_dict, publish_date)
                        async with self._sheets_lock:
                            avans_del_ok = await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.delete_row_by_id(
                                    settings.sheet_tab_avans,
                                    id_value=parsed_data.cian_id,
                                    id_column_index=20,
                                ),
                            )
                        success = bool(avans_del_ok)
                        logger.info(
                            f"🗑️ [Worker-{worker_id}] Снято → удалено из «{settings.sheet_tab_avans}» + из БД: {url}"
                        )

                    elif has_avans:
                        # Аванс/задаток внесён (и объявление активно) → фиксируем в «Аванс_Продано» (только если ≤ недели),
                        # удаляем из «Аванс» и перестаём отслеживать.
                        await self.db_repo.move_to_sold(url, parsed_dict, publish_date)

                        avans_sold_ok = True
                        if within_week:
                            async with self._sheets_lock:
                                avans_sold_ok = await loop.run_in_executor(
                                    None,
                                    lambda: self.sheets_manager.find_and_update_row(
                                        settings.sheet_tab_avans_sold,
                                        row,
                                        id_value=parsed_data.cian_id,
                                        id_column_index=20,
                                    ),
                                )

                        async with self._sheets_lock:
                            avans_del_ok = await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.delete_row_by_id(
                                    settings.sheet_tab_avans,
                                    id_value=parsed_data.cian_id,
                                    id_column_index=20,
                                ),
                            )

                        success = bool(avans_sold_ok) and bool(avans_del_ok)

                        if within_week:
                            logger.info(
                                f"📌 [Worker-{worker_id}] Аванс → {settings.sheet_tab_avans_sold} + удалено из «{settings.sheet_tab_avans}» + из БД "
                                f"(≤{settings.sold_max_age_days}д): {url}"
                            )
                        else:
                            logger.info(
                                f"⏭️ [Worker-{worker_id}] Аванс_Продано skip (старше {settings.sold_max_age_days}д), "
                                f"но удалено из «{settings.sheet_tab_avans}» + из БД: {url}"
                            )

                    else:
                        # Активное, аванс не внесён → обновляем «Аванс» и продолжаем отслеживание
                        await self.db_repo.update_active_ad(url, parsed_dict)
                        async with self._sheets_lock:
                            avans_ok = await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.find_and_update_row(
                                    settings.sheet_tab_avans,
                                    row,
                                    id_value=parsed_data.cian_id,
                                    id_column_index=20,
                                ),
                            )
                        success = bool(avans_ok)
                        logger.info(
                            f"💾 [Worker-{worker_id}] {settings.sheet_tab_avans} "
                            f"(views={parsed_data.unique_views}): {url}"
                        )

                else:
                    # ── mode = offers ──
                    if is_active is False:
                        publish_date = parsed_data.publish_date or ""
                        await self.db_repo.move_to_sold(url, parsed_dict, publish_date)
                    else:
                        await self.db_repo.update_active_ad(url, parsed_dict)

                    views_match = (
                        parsed_data.unique_views is not None
                        and parsed_data.unique_views >= settings.min_unique_views
                    )
                    within_week = _is_within_days(
                        parsed_data.publish_date,
                        parsed_data.days_in_exposition,
                        settings.sold_max_age_days,
                    )

                    if is_active is False:
                        if within_week:
                            async with self._sheets_lock:
                                await loop.run_in_executor(
                                    None,
                                    lambda: self.sheets_manager.find_and_update_row(
                                        sold_tab, row, id_value=parsed_data.cian_id, id_column_index=20,
                                    ),
                                )
                            logger.info(
                                f"📌 [Worker-{worker_id}] → {sold_tab}: "
                                f"снято + ≤{settings.sold_max_age_days}д: {url}"
                            )
                        else:
                            logger.info(
                                f"⏭️ [Worker-{worker_id}] {sold_tab} skip "
                                f"(days_in_exposition={parsed_data.days_in_exposition}, "
                                f"publish_date={parsed_data.publish_date}): {url}"
                            )

                        offers_color = self.DEACTIVATED_COLOR
                        # При снятии объявления в Signals_Parser (если строка уже есть) тоже делаем серой,
                        # даже если сейчас signals_match не вычислился (не удаляем строку).
                        signals_color = self.DEACTIVATED_COLOR

                        async with self._sheets_lock:
                            success = await loop.run_in_executor(
                                None,
                                lambda: self.sheets_manager.sync_offers_and_signals(
                                    row,
                                    str(parsed_data.cian_id),
                                    20,
                                    offers_color,
                                    signals_match,
                                    signals_color,
                                    True,  # Offers_Parser: всегда upsert
                                    True,  # deactivated: меняем только цвет, не значения
                                ),
                            )

                        if views_match or signals_match:
                            logger.info(
                                f"💾 [Worker-{worker_id}] Снято, но подходит "
                                f"(views={views_match}, signals={signal_reason}): {url}"
                            )

                    else:
                        # Зелёная подсветка только для реально активных карточек (is_active=True).
                        # Иначе при is_active=None/False не перетираем серым через этот путь — но и не даём «ложный» зелёный.
                        highlight_ok = views_match and (parsed_data.is_active is True)
                        offers_color = self.HIGHLIGHT_COLOR if highlight_ok else None
                        signals_color = self.HIGHLIGHT_COLOR if highlight_ok else None

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
                                    signals_color,
                                    True,  # Offers_Parser: всегда upsert
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
