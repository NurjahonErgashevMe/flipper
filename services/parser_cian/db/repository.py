import asyncio
import logging
from typing import List, Dict, Any, Optional

from sqlalchemy import select, delete
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import OperationalError

from services.parser_cian.db import base as _base
from services.parser_cian.db.base import (
    CianFilter,
    CianActiveAd,
    CianSoldAd,
    init_db as _init_db,
)

logger = logging.getLogger(__name__)

_SQLITE_IO_RETRIES = 5


async def _run_db_write_with_retry(coro_factory):
    """Повтор при временных ошибках SQLite (блокировка, I/O) под параллельными воркерами."""
    last: Optional[Exception] = None
    for attempt in range(_SQLITE_IO_RETRIES):
        try:
            await coro_factory()
            return
        except OperationalError as e:
            last = e
            msg = str(e).lower()
            if "disk i/o" in msg or "locked" in msg or "busy" in msg:
                if attempt < _SQLITE_IO_RETRIES - 1:
                    await asyncio.sleep(0.15 * (2**attempt))
                    continue
            raise
    if last:
        raise last


class DatabaseRepository:
    def __init__(self, db_path: str = "data/parser_cian.db"):
        self.db_path = db_path

    async def init_db(self):
        """Создает таблицы через SQLAlchemy."""
        await _init_db(self.db_path)
        logger.info(f"Database initialized at {self.db_path}")

    # --- Filters ---
    async def add_filters(self, urls: List[str]):
        """Добавляет новые ссылки-фильтры (INSERT OR IGNORE)."""
        async with _base.AsyncSessionLocal() as session:
            for url in urls:
                stmt = sqlite_insert(CianFilter).values(url=url).on_conflict_do_nothing()
                await session.execute(stmt)
            await session.commit()

    async def sync_filters_exact(self, urls: List[str]) -> None:
        """
        Таблица cian_filters = ровно список с листа FILTERS.
        Удаляет строки, которых больше нет в листе; добавляет новые URL.
        """
        normalized: List[str] = []
        seen: set[str] = set()
        for u in urls:
            if not u:
                continue
            s = str(u).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            normalized.append(s)

        async with _base.AsyncSessionLocal() as session:
            if not normalized:
                await session.execute(delete(CianFilter))
            else:
                await session.execute(
                    delete(CianFilter).where(CianFilter.url.not_in(normalized))
                )
            for url in normalized:
                stmt = sqlite_insert(CianFilter).values(url=url).on_conflict_do_nothing()
                await session.execute(stmt)
            await session.commit()

    async def get_all_filters(self) -> List[Dict]:
        """Возвращает все фильтры."""
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(select(CianFilter))
            rows = result.scalars().all()
            return [{"id": r.id, "url": r.url} for r in rows]

    # --- Active Ads ---
    async def add_ad_urls(
        self, urls: List[str], filter_id: Optional[int] = None, source: str = "regular"
    ):
        """Добавляет объявления в таблицу активных (INSERT OR IGNORE)."""
        async with _base.AsyncSessionLocal() as session:
            for url in urls:
                stmt = (
                    sqlite_insert(CianActiveAd)
                    .values(url=url, filter_id=filter_id, source=source)
                    .on_conflict_do_nothing()
                )
                await session.execute(stmt)
            await session.commit()
        logger.debug(f"add_ad_urls: added up to {len(urls)} URLs (source={source})")

    async def replace_active_ad_urls(self, urls: List[str], source: str = "regular") -> None:
        """
        Полностью заменяет активные объявления для данного source (как после обхода FILTERS).
        """
        ordered_unique: List[str] = []
        seen: set[str] = set()
        for u in urls:
            if not u:
                continue
            s = str(u).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            ordered_unique.append(s)

        async with _base.AsyncSessionLocal() as session:
            await session.execute(
                delete(CianActiveAd).where(CianActiveAd.source == source)
            )
            for url in ordered_unique:
                stmt = (
                    sqlite_insert(CianActiveAd)
                    .values(url=url, filter_id=None, source=source)
                    .on_conflict_do_nothing()
                )
                await session.execute(stmt)
            await session.commit()
        logger.info(
            f"replace_active_ad_urls: source={source}, {len(ordered_unique)} URLs"
        )

    async def get_all_active_ads(self, source: Optional[str] = None) -> List[str]:
        """Получает URL активных объявлений, опционально фильтруя по source."""
        async with _base.AsyncSessionLocal() as session:
            q = select(CianActiveAd.url)
            if source:
                q = q.where(CianActiveAd.source == source)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def update_active_ad(self, url: str, parsed_data: Dict[str, Any]):
        """Обновляет данные активного объявления."""

        async def _do():
            async with _base.AsyncSessionLocal() as session:
                result = await session.execute(
                    select(CianActiveAd).where(CianActiveAd.url == url)
                )
                ad = result.scalar_one_or_none()
                if ad:
                    ad.parsed_data = parsed_data
                    ad.is_parsed = True
                    await session.commit()

        await _run_db_write_with_retry(_do)

    async def move_to_sold(self, url: str, parsed_data: Dict[str, Any], publish_date: str):
        """Удаляет объявление из активных и добавляет в проданные."""

        async def _do():
            async with _base.AsyncSessionLocal() as session:
                await session.execute(
                    delete(CianActiveAd).where(CianActiveAd.url == url)
                )
                stmt = (
                    sqlite_insert(CianSoldAd)
                    .values(
                        url=url,
                        parsed_data=parsed_data,
                        publish_date=publish_date,
                    )
                    .on_conflict_do_nothing()
                )
                await session.execute(stmt)
                await session.commit()

        await _run_db_write_with_retry(_do)
        logger.info(f"Moved to sold: {url}")
