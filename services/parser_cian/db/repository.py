import asyncio
import json
import logging
from typing import List, Dict, Any, Optional

from sqlalchemy import select, delete, text
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

    async def sync_filters_exact(self, filters: List[Any]) -> None:
        """
        Таблица cian_filters = ровно список с листа FILTERS.
        Удаляет строки, которых больше нет в листе; добавляет новые URL.

        Поддерживает вход:
        - List[str] (старый формат)
        - List[dict] где dict содержит как минимум {"url": "..."} и опционально {"meta": {...}}
        """
        normalized: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in filters or []:
            url = None
            meta = None
            if isinstance(item, dict):
                url = item.get("url")
                meta = item.get("meta")
            else:
                url = item
                meta = None

            if not url:
                continue
            s = str(url).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            normalized.append({"url": s, "meta": meta})

        async with _base.AsyncSessionLocal() as session:
            if not normalized:
                await session.execute(delete(CianFilter))
            else:
                urls_only = [x["url"] for x in normalized]
                await session.execute(
                    delete(CianFilter).where(CianFilter.url.not_in(urls_only))
                )
            for f in normalized:
                stmt = (
                    sqlite_insert(CianFilter)
                    .values(url=f["url"], meta=f.get("meta"))
                    .on_conflict_do_update(
                        index_elements=["url"],
                        set_={"meta": f.get("meta")},
                    )
                )
                await session.execute(stmt)
            await session.commit()

    async def get_all_filters(self) -> List[Dict]:
        """Возвращает все фильтры."""
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(select(CianFilter))
            rows = result.scalars().all()
            return [{"id": r.id, "url": r.url, "meta": r.meta} for r in rows]

    async def assign_filter_to_ads(
        self, urls: List[str], filter_id: int, source: str = "offers"
    ) -> int:
        """Проставляет filter_id активным объявлениям, где он ещё NULL (не перезаписывает существующий).

        Returns:
            Количество обновлённых строк.
        """
        if not urls or not filter_id:
            return 0

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

        if not ordered_unique:
            return 0

        total_updated = 0
        chunk_size = 400  # <= 999 SQLite vars safe margin

        async with _base.AsyncSessionLocal() as session:
            for i in range(0, len(ordered_unique), chunk_size):
                chunk = ordered_unique[i : i + chunk_size]
                params: Dict[str, Any] = {"fid": int(filter_id), "src": str(source)}
                placeholders: List[str] = []
                for j, url in enumerate(chunk):
                    key = f"u{j}"
                    params[key] = url
                    placeholders.append(f":{key}")

                q = (
                    "UPDATE cian_active_ads "
                    "SET filter_id = :fid "
                    "WHERE source = :src AND filter_id IS NULL "
                    f"AND url IN ({', '.join(placeholders)})"
                )
                res = await session.execute(text(q), params)
                try:
                    total_updated += int(res.rowcount or 0)
                except Exception:
                    pass

            await session.commit()

        return total_updated

    async def get_filter_for_ad_url(self, ad_url: str) -> Optional[Dict[str, Any]]:
        """Возвращает фильтр (url + meta) для активного объявления по его URL."""
        if not ad_url:
            return None
        u = str(ad_url).strip()
        if not u:
            return None
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(
                select(CianFilter.url, CianFilter.meta)
                .join(CianActiveAd, CianActiveAd.filter_id == CianFilter.id)
                .where(CianActiveAd.url == u)
            )
            row = result.first()
            if not row:
                return None
            return {"url": row[0], "meta": row[1]}

    # --- Active Ads ---
    async def add_ad_urls(
        self, urls: List[str], filter_id: Optional[int] = None, source: str = "offers"
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

    async def merge_active_ad_urls(self, urls: List[str], source: str = "offers") -> int:
        """
        Добавляет новые URL к существующим в cian_active_ads (INSERT OR IGNORE).
        Не удаляет существующие записи — объявления остаются в БД до явного удаления
        (move_to_sold или remove_stale_active_ads).

        Пропускает URL-ы, которые уже есть в cian_sold_ads (уже обработаны).

        Returns:
            Количество действительно новых URL, добавленных в БД.
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

        added = 0
        skipped_sold = 0
        async with _base.AsyncSessionLocal() as session:
            existing_result = await session.execute(
                select(CianActiveAd.url).where(CianActiveAd.source == source)
            )
            existing_urls = set(existing_result.scalars().all())

            sold_result = await session.execute(select(CianSoldAd.url))
            sold_urls = set(sold_result.scalars().all())

            for url in ordered_unique:
                if url in existing_urls:
                    continue
                if url in sold_urls:
                    skipped_sold += 1
                    continue
                stmt = (
                    sqlite_insert(CianActiveAd)
                    .values(url=url, filter_id=None, source=source)
                    .on_conflict_do_nothing()
                )
                await session.execute(stmt)
                added += 1
            await session.commit()

        logger.info(
            f"merge_active_ad_urls: source={source}, "
            f"from_search={len(ordered_unique)}, new={added}, "
            f"already_active={len(ordered_unique) - added - skipped_sold}, "
            f"already_sold={skipped_sold}"
        )
        return added

    async def get_all_active_ads(self, source: Optional[str] = None) -> List[str]:
        """Получает URL активных объявлений, опционально фильтруя по source."""
        async with _base.AsyncSessionLocal() as session:
            q = select(CianActiveAd.url)
            if source:
                q = q.where(CianActiveAd.source == source)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def get_unparsed_active_ads(self, source: Optional[str] = None) -> List[str]:
        """Активные объявления, по которым ещё не было успешного парсинга в БД (is_parsed=false)."""
        async with _base.AsyncSessionLocal() as session:
            q = select(CianActiveAd.url).where(CianActiveAd.is_parsed.is_(False))
            if source:
                q = q.where(CianActiveAd.source == source)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def get_unparsed_active_ads_in_urls(
        self, urls: List[str], source: Optional[str] = None
    ) -> List[str]:
        """Активные и ещё не спарсенные URL из переданного списка (пересечение с cian_active_ads)."""
        want = [str(u).strip() for u in (urls or []) if str(u).strip()]
        if not want:
            return []
        async with _base.AsyncSessionLocal() as session:
            q = select(CianActiveAd.url).where(
                CianActiveAd.is_parsed.is_(False),
                CianActiveAd.url.in_(want),
            )
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

    async def remove_stale_active_ads(self, source: str, max_age_days: int = 7) -> int:
        """
        Удаляет из cian_active_ads записи, где publish_date старше max_age_days
        и объявление было успешно спарсено (is_parsed=True, is_active остаётся True).

        Это объявления, которые «отслужили своё» — были активны, но
        выходят за окно мониторинга.

        Чтение через CAST(parsed_data AS BLOB): при битом UTF-8 в TEXT ORM падает
        с «Could not decode to UTF-8»; здесь декодируем вручную.

        Returns:
            Количество удалённых записей.
        """
        from datetime import datetime, timedelta

        cutoff = (datetime.now() - timedelta(days=max_age_days)).strftime("%Y-%m-%d")
        removed = 0

        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(
                text(
                    "SELECT id, CAST(parsed_data AS BLOB) AS raw_pd "
                    "FROM cian_active_ads "
                    "WHERE source = :src AND is_parsed = 1"
                ),
                {"src": source},
            )
            rows = result.fetchall()

            ids_to_delete: List[int] = []

            for row in rows:
                ad_id, raw_pd = row[0], row[1]
                if not raw_pd:
                    continue
                try:
                    s = bytes(raw_pd).decode("utf-8", errors="replace")
                    data = json.loads(s)
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(
                        "remove_stale_active_ads: битый JSON в parsed_data id=%s: %s — сбрасываю поле",
                        ad_id,
                        e,
                    )
                    await session.execute(
                        text(
                            "UPDATE cian_active_ads SET parsed_data = NULL, is_parsed = 0 "
                            "WHERE id = :id"
                        ),
                        {"id": ad_id},
                    )
                    continue

                pd = None
                if isinstance(data, dict):
                    pd = data.get("publish_date")
                if not pd or not isinstance(pd, str):
                    continue
                if pd < cutoff:
                    ids_to_delete.append(ad_id)

            if ids_to_delete:
                await session.execute(
                    delete(CianActiveAd).where(CianActiveAd.id.in_(ids_to_delete))
                )
                removed = len(ids_to_delete)

            await session.commit()

        if removed:
            logger.info(
                f"remove_stale_active_ads: source={source}, "
                f"removed={removed} ads older than {max_age_days} days (cutoff={cutoff})"
            )
        return removed

    async def clear_sold_ads(self) -> int:
        """Удаляет все записи из cian_sold_ads. Используется для сброса при изменении логики."""
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(delete(CianSoldAd))
            count = result.rowcount
            await session.commit()
        logger.info(f"clear_sold_ads: removed {count} records")
        return count

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

    async def delete_active_ad(self, url: str) -> int:
        """Удаляет объявление из cian_active_ads (перестаём отслеживать), без добавления в sold."""

        removed: int = 0

        async def _do():
            nonlocal removed
            async with _base.AsyncSessionLocal() as session:
                result = await session.execute(delete(CianActiveAd).where(CianActiveAd.url == url))
                removed = int(result.rowcount or 0)
                await session.commit()

        await _run_db_write_with_retry(_do)
        if removed:
            logger.info("delete_active_ad: removed url=%s", url)
        return removed
