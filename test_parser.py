import asyncio
import os
import sys
import logging
import json
import httpx
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, "services", "parser_cian"))

load_dotenv()

_DOCKER_TO_LOCAL = {
    "FIRECRAWL_BASE_URL": ("flippercrawl-api-1", "localhost"),
    "COOKIE_MANAGER_URL": ("cookie_manager", "localhost"),
}
for _env_key, (_docker_host, _local_host) in _DOCKER_TO_LOCAL.items():
    _val = os.environ.get(_env_key, "")
    if _docker_host in _val:
        os.environ[_env_key] = _val.replace(_docker_host, _local_host)

FIRECRAWL_URL = (
    os.getenv("FIRECRAWL_BASE_URL", "http://localhost:3002").rstrip("/") + "/v2/scrape"
)
FIRECRAWL_KEY = os.getenv("FIRECRAWL_API_KEY", "test-key")
COOKIE_MANAGER_URL = os.getenv("COOKIE_MANAGER_URL", "http://localhost:8000").rstrip(
    "/"
)

EXCLUDE_TAGS = [
    "svg",
    "img",
    "script",
    "style",
    "footer",
    "header",
    "[data-name='CardSectionNew']",
    "[data-name='OfferCardPageLayoutFooter']",
    "[id='adfox-stretch-banner']",
]

JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "cian_id": {
            "type": "string",
            "description": "ID объявления из URL (число в конце /sale/flat/XXXXXX/)",
        },
        "price": {"type": "integer", "description": "Цена в рублях"},
        "price_per_m2": {"type": "integer", "description": "Цена за м²"},
        "title": {"type": "string", "description": "Заголовок объявления"},
        "description": {"type": "string", "description": "Текст описания объявления"},
        "address": {
            "type": "object",
            "properties": {
                "full": {"type": "string", "description": "Полный адрес"},
                "district": {"type": "string", "description": "Район"},
                "metro_station": {
                    "type": "string",
                    "description": "Ближайшая станция метро",
                },
                "okrug": {"type": "string", "description": "Округ (ЦАО, ЮВАО и т.д.)"},
            },
        },
        "area": {"type": "number", "description": "Общая площадь в м²"},
        "rooms": {"type": "integer", "description": "Количество комнат"},
        "housing_type": {
            "type": "string",
            "description": "Тип жилья (Вторичка, Новостройка)",
        },
        "floor_info": {
            "type": "object",
            "properties": {
                "current": {"type": "integer", "description": "Этаж квартиры"},
                "all": {"type": "integer", "description": "Всего этажей в доме"},
            },
        },
        "construction_year": {"type": "integer", "description": "Год постройки дома"},
        "renovation": {"type": "string", "description": "Тип ремонта"},
        "metro_walk_time": {
            "type": "integer",
            "description": "Минут пешком до ближайшего метро",
        },
        "total_views": {
            "type": "integer",
            "description": "Всего просмотров — число ДО запятой в строке 'X просмотров, Y за сегодня'",
        },
        "unique_views": {
            "type": "integer",
            "description": "Просмотров сегодня — число ПОСЛЕ запятой в строке 'X просмотров, Y за сегодня'",
        },
        "is_active": {"type": "boolean", "description": "Активно ли объявление"},
        "price_history": {
            "type": "array",
            "description": "История изменения цены (если есть раздел 'История цены')",
            "items": {
                "type": "object",
                "properties": {
                    "date": {
                        "type": "string",
                        "description": "Дата изменения (например: '10 мар 2026')",
                    },
                    "price": {
                        "type": "integer",
                        "description": "Цена в рублях на эту дату",
                    },
                    "change_amount": {
                        "type": "integer",
                        "description": "На сколько изменилась цена (отрицательное = снижение). 0 для первой записи.",
                    },
                    "change_type": {
                        "type": "string",
                        "enum": ["initial", "decrease", "increase"],
                        "description": "Тип изменения: initial (первая цена), decrease (снижение), increase (повышение)",
                    },
                },
                "required": ["date", "price", "change_amount", "change_type"],
            },
        },
    },
    "required": ["cian_id", "price", "area"],
}

SYSTEM_PROMPT = (
    "Экстрактор объявлений Cian.ru: заполни поля по схеме из markdown; нет данных — null. "
    "Просмотры: в одной строке «X просмотров, Y за сегодня» — X→total_views, Y→unique_views. "
    "is_active: true, если карточка доступна. "
    "price_history: если есть раздел 'История цены' — заполни массив записей с date, price, "
    "change_amount (0 для первой), change_type (initial/decrease/increase). Нет раздела — null."
)


async def get_cookies() -> str:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{COOKIE_MANAGER_URL}/cookies")
            if resp.status_code == 200:
                cookies = resp.json()
                return "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    except Exception as e:
        logger.warning(f"Cookie Manager недоступен: {e}")
    return ""


def build_payload(url: str, cookies_str: str) -> dict:
    payload = {
        "url": url,
        "excludeTags": EXCLUDE_TAGS,
        "formats": [
            "markdown",
            {
                "type": "json",
                "schema": JSON_SCHEMA,
                "systemPrompt": SYSTEM_PROMPT,
            },
        ],
    }
    if cookies_str:
        payload["headers"] = {"Cookie": cookies_str}
    return payload


CONCURRENCY = 20


async def scrape_ad(
    client: httpx.AsyncClient, url: str, cookies_str: str, worker_id: int
) -> dict:
    payload = build_payload(url, cookies_str)
    headers = {
        "Authorization": f"Bearer {FIRECRAWL_KEY}",
        "Content-Type": "application/json",
    }

    logger.info(f"[Worker-{worker_id}] Отправляю: {url}")
    resp = await client.post(FIRECRAWL_URL, json=payload, headers=headers)

    if resp.status_code != 200:
        raise ValueError(f"Firecrawl {resp.status_code}: {resp.text[:300]}")

    result = resp.json()
    if not result.get("success"):
        raise ValueError(
            f"Firecrawl success=false: {json.dumps(result, ensure_ascii=False)[:300]}"
        )

    return result.get("data", {})


def log_result(url: str, data: dict, worker_id: int) -> None:
    cian_id = url.rstrip("/").split("/")[-1]
    output_file = f"data_{cian_id}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    jd = data.get("json") or {}
    md_len = len(data.get("markdown") or "")
    ph = jd.get("price_history")
    ph_info = f"{len(ph)} записей" if ph else "нет"
    logger.info(
        f"[Worker-{worker_id}] OK {cian_id}: "
        f"md={md_len}, price={jd.get('price')}, area={jd.get('area')}, "
        f"rooms={jd.get('rooms')}, views={jd.get('total_views')}/{jd.get('unique_views')}, "
        f"active={jd.get('is_active')}, price_history={ph_info}"
    )


async def worker(
    queue: asyncio.Queue,
    client: httpx.AsyncClient,
    cookies_str: str,
    worker_id: int,
    stats: dict,
):
    while True:
        url = await queue.get()
        try:
            data = await scrape_ad(client, url, cookies_str, worker_id)
            log_result(url, data, worker_id)
            stats["ok"] += 1
        except Exception as e:
            logger.error(f"[Worker-{worker_id}] FAIL {url}: {e}")
            stats["fail"] += 1
        finally:
            queue.task_done()


async def test_ads():
    urls_to_test = [
        "https://www.cian.ru/sale/flat/322869278/",
        "https://www.cian.ru/sale/flat/315780538/",
        "https://www.cian.ru/sale/flat/318732851/",
        "https://www.cian.ru/sale/flat/324878007/",
        "https://www.cian.ru/sale/flat/321180048/",
        "https://www.cian.ru/sale/flat/324638002/",
        "https://www.cian.ru/sale/flat/327524113/",
        "https://www.cian.ru/sale/flat/269645973/",
        "https://www.cian.ru/sale/flat/326240888/",
        "https://www.cian.ru/sale/flat/326545702/",
        "https://www.cian.ru/sale/flat/327625128/",
        "https://www.cian.ru/sale/flat/327585685/",
        "https://www.cian.ru/sale/flat/327845533/",
        "https://www.cian.ru/sale/flat/322231937/",
    ]

    cookies_str = await get_cookies()
    logger.info(f"Куки: {'есть' if cookies_str else 'нет'}")
    logger.info(
        f"URL-ов: {len(urls_to_test)}, воркеров: {min(CONCURRENCY, len(urls_to_test))}"
    )

    queue: asyncio.Queue[str] = asyncio.Queue()
    for u in urls_to_test:
        queue.put_nowait(u)

    stats = {"ok": 0, "fail": 0}
    num_workers = min(CONCURRENCY, len(urls_to_test))

    async with httpx.AsyncClient(timeout=120.0) as client:
        workers = [
            asyncio.create_task(worker(queue, client, cookies_str, i, stats))
            for i in range(num_workers)
        ]
        await queue.join()
        for w in workers:
            w.cancel()

    logger.info("=" * 60)
    logger.info(
        f"Готово: OK={stats['ok']}, FAIL={stats['fail']}, всего={stats['ok'] + stats['fail']}"
    )


if __name__ == "__main__":
    asyncio.run(test_ads())
