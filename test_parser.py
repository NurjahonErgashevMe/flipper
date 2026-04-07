"""
test_parser.py — тестирование реального парсинга объявлений Cian через AdParser.

Использует настоящий AdParser из services/parser_cian/parser.py.
Результаты: JSON-файлы data_{cian_id}.json + сводка по каждому объявлению.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

CONCURRENCY = 20


def dump_result(parsed: ParsedAdData) -> None:
    """Сохраняет результат в JSON и логирует сводку."""
    cian_id = parsed.cian_id or "unknown"
    output_file = f"data_{cian_id}.json"

    data_dict = parsed.model_dump(mode="json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data_dict, f, ensure_ascii=False, indent=2)

    ph = parsed.price_history
    ph_info = f"{len(ph)} записей" if ph else "нет"
    sheets_row = parse_to_sheets_row(parsed)

    logger.info(
        f"OK {cian_id}: price={parsed.price}, area={parsed.area}, "
        f"rooms={parsed.rooms}, housing_type={parsed.housing_type}, "
        f"building_type={parsed.building_type}, "
        f"district={parsed.address.district if parsed.address else None}, "
        f"okrug={parsed.address.okrug if parsed.address else None}, "
        f"renovation={parsed.renovation}, "
        f"views={parsed.total_views}/{parsed.unique_views}, "
        f"active={parsed.is_active}, price_history={ph_info}"
    )
    logger.info(f"Sheets row ({len(sheets_row)} cols): {sheets_row}")


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
        except Exception as e:
            logger.error(f"[Worker-{worker_id}] FAIL {url}: {e}")
            stats["fail"] += 1
        finally:
            queue.task_done()


async def test_ads():
    urls_to_test = [
        "https://www.cian.ru/sale/flat/325586286/",
        "https://www.cian.ru/sale/flat/325586286/",
        "https://www.cian.ru/sale/flat/326556052/",
        "https://www.cian.ru/sale/flat/327286191/",
        "https://www.cian.ru/sale/flat/327883327/",
        "https://www.cian.ru/sale/flat/324563821/",
        "https://www.cian.ru/sale/flat/322163051/",
        "https://www.cian.ru/sale/flat/317574947/",
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

    stats = {"ok": 0, "fail": 0}
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


if __name__ == "__main__":
    asyncio.run(test_ads())
