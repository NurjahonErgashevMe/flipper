"""
Тест AdParser — парсинг одного объявления Циан с куками и LLM extraction.

Запуск:
    python test_parser.py

Требования:
    - Запущены все Docker контейнеры (docker compose up -d)
    - cookie_manager доступен на http://localhost:8000
    - firecrawl_api доступен на http://localhost:3003
    - В cookies.json есть актуальные куки (или cookie_manager запросит логин)
"""

import asyncio
import logging
import sys
import json
import time
from datetime import datetime

# Добавляем корень проекта в путь
sys.path.insert(0, ".")

from flipping_cian.parser import AdParser
from flipping_cian.models import ParsedAdData

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("test_parser")

# ── Config ────────────────────────────────────────────────────────────────────
TEST_URL = "https://www.cian.ru/sale/flat/312533860/"
FIRECRAWL_URL = "http://localhost:3003"
FIRECRAWL_KEY = "fc-c0b3326bab554a17ae43d2fca45876d7"
COOKIE_MANAGER_URL = "http://localhost:8000"


def print_result(result: ParsedAdData):
    """Красиво выводит результат парсинга."""
    print("\n" + "="*60)
    print("✅ РЕЗУЛЬТАТ ПАРСИНГА")
    print("="*60)

    fields = [
        ("URL",             result.url),
        ("Заголовок",       result.title),
        ("Цена",            f"{result.price:,} ₽" if result.price else None),
        ("Цена за м²",      f"{result.price_per_m2:,} ₽/м²" if result.price_per_m2 else None),
        ("Площадь",         f"{result.area} м²" if result.area else None),
        ("Комнат",          result.rooms),
        ("Этаж",            f"{result.floor_info.current}/{result.floor_info.all}" if result.floor_info else None),
        ("Тип жилья",       result.housing_type),
        ("Ремонт",          result.renovation),
        ("Год постройки",   result.construction_year),
        ("Адрес",           result.address.full if result.address else None),
        ("Район",           result.address.district if result.address else None),
        ("Округ",           result.address.okrug if result.address else None),
        ("Метро",           result.address.metro_station if result.address else None),
        ("До метро",        f"{result.metro_walk_time} мин" if result.metro_walk_time else None),
        ("Дней в экспозиции", result.days_in_exposition),
        ("Просмотров всего", result.total_views),
        ("Просмотров уник.", result.unique_views),
        ("Дата публикации", result.publish_date),
        ("Cian ID",         result.cian_id),
        ("Спарсено",        result.parsed_at),
    ]

    for label, value in fields:
        if value is not None and value != "":
            print(f"  {label:<22}: {value}")

    if result.description:
        desc_preview = result.description[:200] + "..." if len(result.description) > 200 else result.description
        print(f"  {'Описание':<22}: {desc_preview}")

    print("="*60)

    # Считаем заполненность
    total = len(fields)
    filled = sum(1 for _, v in fields if v is not None and v != "")
    print(f"📊 Заполнено полей: {filled}/{total} ({filled/total*100:.0f}%)")
    print("="*60 + "\n")


async def test_parse():
    logger.info(f"Инициализация AdParser...")
    parser = AdParser(
        firecrawl_url=FIRECRAWL_URL,
        firecrawl_key=FIRECRAWL_KEY,
        cookie_manager_url=COOKIE_MANAGER_URL,
    )

    # 1. Проверяем cookie_manager
    logger.info("Проверяем доступность cookie_manager...")
    try:
        cookie_str = parser.get_cookies_str()
        cookie_count = len(cookie_str.split(";")) if cookie_str else 0
        logger.info(f"✅ Куки получены: {cookie_count} штук")
    except Exception as e:
        logger.error(f"❌ Не удалось получить куки: {e}")
        logger.error("Убедитесь что cookie_manager запущен: docker compose up -d cookie_manager")
        return

    # 2. Парсим объявление
    logger.info(f"Парсим: {TEST_URL}")
    start = time.time()

    try:
        result = await parser.parse_async(TEST_URL)
        elapsed = time.time() - start

        logger.info(f"⏱️  Время парсинга: {elapsed:.1f} секунд")
        print_result(result)

        # 3. Проверяем row для Google Sheets
        row = result.to_row()
        print(f"📋 Строка для Google Sheets ({len(row)} колонок):")
        print(f"   {row}\n")

    except Exception as e:
        elapsed = time.time() - start
        logger.error(f"❌ Ошибка парсинга после {elapsed:.1f}с: {e}", exc_info=True)


if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"  ТЕСТ AdParser — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  URL: {TEST_URL}")
    print(f"{'='*60}\n")

    asyncio.run(test_parse())