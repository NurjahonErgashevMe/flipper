"""
test_parser.py — тестирование реального парсинга объявлений Cian через AdParser
с проверкой критериев распределения по табам (Аванс, Offers_Parser, Signals_Parser).

Результаты: JSON-файлы в parsed_data/ + сводка + вердикт по критериям.
"""

import asyncio
import os
import sys
import json
import logging

from dotenv import load_dotenv

load_dotenv()

_DOCKER_TO_LOCAL = {
    "FIRECRAWL_BASE_URL": ("flippercrawl-api-1", "localhost"),
    "COOKIE_MANAGER_URL": ("cookie_manager", "localhost"),
}
for _env_key, (_docker_host, _local_host) in _DOCKER_TO_LOCAL.items():
    _val = os.environ.get(_env_key, "")
    if _docker_host in _val:
        os.environ[_env_key] = _val.replace(_docker_host, _local_host)

from services.parser_cian.parser import AdParser  # noqa: E402
from services.parser_cian.models import ParsedAdData, parse_to_sheets_row  # noqa: E402
from services.parser_cian.queue_manager import check_signals, _is_avans_deposit  # noqa: E402
from services.parser_cian.config import settings  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

CONCURRENCY = 20
PARSED_DATA_DIR = os.path.join(os.path.dirname(__file__), "parsed_data")
os.makedirs(PARSED_DATA_DIR, exist_ok=True)


def evaluate_criteria(parsed: ParsedAdData) -> dict:
    """Проверяет, под какие критерии попадает объявление."""
    parsed_dict = parsed.model_dump(mode="json")
    price_history = parsed_dict.get("price_history", [])

    offers_match = (
        parsed.unique_views is not None
        and parsed.unique_views >= settings.min_unique_views
    )
    signal_reason = check_signals(price_history)
    has_avans = _is_avans_deposit(parsed)

    return {
        "offers_match": offers_match,
        "signal_reason": signal_reason,
        "signals_match": bool(signal_reason),
        "has_avans": has_avans,
        "has_avans_ai": parsed.has_avans_deposit,
        "is_active": parsed.is_active,
        "unique_views": parsed.unique_views,
    }


def dump_result(parsed: ParsedAdData) -> None:
    """Сохраняет результат в parsed_data/ и логирует сводку с критериями."""
    cian_id = parsed.cian_id or "unknown"
    output_file = os.path.join(PARSED_DATA_DIR, f"data_{cian_id}.json")

    data_dict = parsed.model_dump(mode="json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=2)

    ph = parsed.price_history
    ph_info = f"{len(ph)} записей" if ph else "нет"

    criteria = evaluate_criteria(parsed)
    reason = criteria["signal_reason"]
    sheets_row = parse_to_sheets_row(parsed, reason=reason)

    logger.info(
        f"OK {cian_id}: price={parsed.price}, area={parsed.area}, "
        f"rooms={parsed.rooms}, views={parsed.total_views}/{parsed.unique_views}, "
        f"active={parsed.is_active}, price_history={ph_info}"
    )

    tab_dest = []
    if criteria["has_avans"]:
        tab_dest.append("Аванс_Продано")
    if criteria["offers_match"]:
        c = "#D9D9D9 (снято)" if not criteria["is_active"] else ""
        tab_dest.append(f"Offers_Parser{' ' + c if c else ''}")
    if criteria["signals_match"]:
        c = "#D9D9D9 (снято)" if not criteria["is_active"] else ""
        tab_dest.append(f"Signals_Parser{' ' + c if c else ''}")
    if not criteria["is_active"]:
        tab_dest.append("Продано")

    logger.info(
        f"  КРИТЕРИИ {cian_id}: "
        f"has_avans={criteria['has_avans']}, "
        f"offers={criteria['offers_match']} (views={criteria['unique_views']}), "
        f"signals={criteria['signals_match']} reason=[{reason}], "
        f"active={criteria['is_active']}"
    )
    logger.info(f"  ТАБЫ: {', '.join(tab_dest) if tab_dest else '(нет критериев)'}")
    logger.info(f"  Sheets row ({len(sheets_row)} cols): {sheets_row}")


async def worker(
    queue: asyncio.Queue,
    parser: AdParser,
    worker_id: int,
    stats: dict,
):
    while True:
        url = await queue.get()
        try:
            logger.info(f"[Worker-{worker_id}] Парсинг: {url}")
            parsed = await parser.parse_async(url)
            dump_result(parsed)
            stats["ok"] += 1
            stats["results"].append(parsed)
        except Exception as e:
            logger.error(f"[Worker-{worker_id}] FAIL {url}: {e}")
            stats["fail"] += 1
        finally:
            queue.task_done()


async def test_ads():
    urls_to_test = [
        "https://www.cian.ru/sale/flat/327273828/",
        "https://www.cian.ru/sale/flat/328663696/",
        "https://www.cian.ru/sale/flat/297346236/",
    ]

    parser = AdParser(
        cookie_manager_url=os.getenv("COOKIE_MANAGER_URL", "http://localhost:8000"),
        firecrawl_base_url=os.getenv("FIRECRAWL_BASE_URL", "http://localhost:3002"),
        firecrawl_api_key=os.getenv("FIRECRAWL_API_KEY", "test-key"),
    )

    logger.info(
        f"URL-ов: {len(urls_to_test)}, воркеров: {min(CONCURRENCY, len(urls_to_test))}"
    )

    queue: asyncio.Queue[str] = asyncio.Queue()
    for u in urls_to_test:
        queue.put_nowait(u)

    stats = {"ok": 0, "fail": 0, "results": []}
    num_workers = min(CONCURRENCY, len(urls_to_test))

    workers = [
        asyncio.create_task(worker(queue, parser, i, stats)) for i in range(num_workers)
    ]
    await queue.join()
    for w in workers:
        w.cancel()

    logger.info("=" * 60)
    logger.info(
        f"Готово: OK={stats['ok']}, FAIL={stats['fail']}, "
        f"всего={stats['ok'] + stats['fail']}"
    )

    logger.info("\n" + "=" * 110)
    logger.info("СВОДНАЯ ТАБЛИЦА КРИТЕРИЕВ")
    logger.info("=" * 110)
    logger.info(
        f"{'ID':<12} {'Views':>6} {'Active':>7} {'AI_Avans':>9} "
        f"{'Result':>7} {'Offers':>7} {'Signal':>7} {'Reason':<35}"
    )
    logger.info("-" * 110)

    avans_sold_count = 0
    for parsed in stats["results"]:
        c = evaluate_criteria(parsed)
        cid = parsed.cian_id or "?"
        if c["has_avans"]:
            avans_sold_count += 1
        ai_val = c.get("has_avans_ai")
        ai_str = "TRUE" if ai_val is True else ("FALSE" if ai_val is False else "N/A")
        logger.info(
            f"{cid:<12} {str(c['unique_views'] or '-'):>6} "
            f"{'Y' if c['is_active'] else 'N':>7} "
            f"{ai_str:>9} "
            f"{'YES' if c['has_avans'] else '-':>7} "
            f"{'YES' if c['offers_match'] else '-':>7} "
            f"{'YES' if c['signals_match'] else '-':>7} "
            f"{c['signal_reason'] or '-':<35}"
        )

    logger.info("=" * 110)
    logger.info(
        f"Аванс_Продано: {avans_sold_count} из {len(stats['results'])} "
        f"(AI определила внесение аванса/задатка)"
    )


if __name__ == "__main__":
    asyncio.run(test_ads())
