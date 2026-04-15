import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import select, delete, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError

from services.parser_cian.db import base as _base
from services.parser_cian.db.base import (
    CianFilter,
    CianActiveAd,
    CianSoldAd,
    init_db as _init_db,
)

logger = logging.getLogger(__name__)


class DatabaseRepository:
    def __init__(self, database_url: str):
        self.database_url = database_url

    async def init_db(self):
        await _init_db(self.database_url)
        logger.info("Database initialized (%s)", self.database_url.split("@")[-1])

    # --- Filters ---

    async def add_filters(self, urls: List[str]):
        async with _base.AsyncSessionLocal() as session:
            for url in urls:
                stmt = pg_insert(CianFilter).values(url=url).on_conflict_do_nothing()
                await session.execute(stmt)
            await session.commit()

    async def sync_filters_exact(self, filters: List[Any]) -> None:
        normalized: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for item in filters or []:
            url = item.get("url") if isinstance(item, dict) else item
            meta = item.get("meta") if isinstance(item, dict) else None
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
                    pg_insert(CianFilter)
                    .values(url=f["url"], meta=f.get("meta"))
                    .on_conflict_do_update(
                        index_elements=["url"],
                        set_={"meta": f.get("meta")},
                    )
                )
                await session.execute(stmt)
            await session.commit()

    async def get_all_filters(self) -> List[Dict]:
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(select(CianFilter))
            rows = result.scalars().all()
            return [{"id": r.id, "url": r.url, "meta": r.meta} for r in rows]

    async def assign_filter_to_ads(
        self, urls: List[str], filter_id: int, source: str = "offers"
    ) -> int:
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
        chunk_size = 500

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
        async with _base.AsyncSessionLocal() as session:
            for url in urls:
                stmt = (
                    pg_insert(CianActiveAd)
                    .values(url=url, filter_id=filter_id, source=source)
                    .on_conflict_do_nothing()
                )
                await session.execute(stmt)
            await session.commit()

    async def merge_active_ad_urls(self, urls: List[str], source: str = "offers") -> int:
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
                    pg_insert(CianActiveAd)
                    .values(url=url, filter_id=None, source=source)
                    .on_conflict_do_nothing()
                )
                await session.execute(stmt)
                added += 1
            await session.commit()

        logger.info(
            "merge_active_ad_urls: source=%s, from_search=%s, new=%s, "
            "already_active=%s, already_sold=%s",
            source, len(ordered_unique), added,
            len(ordered_unique) - added - skipped_sold, skipped_sold,
        )
        return added

    async def get_all_active_ads(self, source: Optional[str] = None) -> List[str]:
        async with _base.AsyncSessionLocal() as session:
            q = select(CianActiveAd.url)
            if source:
                q = q.where(CianActiveAd.source == source)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def get_unparsed_active_ads(self, source: Optional[str] = None) -> List[str]:
        async with _base.AsyncSessionLocal() as session:
            q = select(CianActiveAd.url).where(CianActiveAd.is_parsed.is_(False))
            if source:
                q = q.where(CianActiveAd.source == source)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def get_unparsed_active_ads_in_urls(
        self, urls: List[str], source: Optional[str] = None
    ) -> List[str]:
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
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(
                select(CianActiveAd).where(CianActiveAd.url == url)
            )
            ad = result.scalar_one_or_none()
            if ad:
                ad.parsed_data = parsed_data
                ad.is_parsed = True
                await session.commit()

    async def remove_stale_active_ads(self, source: str, max_age_days: int = 7) -> int:
        cutoff = (datetime.now() - timedelta(days=max_age_days)).strftime("%Y-%m-%d")
        removed = 0

        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(
                select(CianActiveAd.id, CianActiveAd.parsed_data).where(
                    CianActiveAd.source == source,
                    CianActiveAd.is_parsed.is_(True),
                )
            )
            rows = result.all()

            ids_to_delete: List[int] = []
            for ad_id, data in rows:
                if not isinstance(data, dict):
                    continue
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
                "remove_stale_active_ads: source=%s, removed=%s (older than %s days, cutoff=%s)",
                source, removed, max_age_days, cutoff,
            )
        return removed

    async def clear_sold_ads(self) -> int:
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(delete(CianSoldAd))
            count = result.rowcount
            await session.commit()
        logger.info("clear_sold_ads: removed %s records", count)
        return count

    async def move_to_sold(self, url: str, parsed_data: Dict[str, Any], publish_date: str):
        async with _base.AsyncSessionLocal() as session:
            await session.execute(
                delete(CianActiveAd).where(CianActiveAd.url == url)
            )
            stmt = (
                pg_insert(CianSoldAd)
                .values(url=url, parsed_data=parsed_data, publish_date=publish_date)
                .on_conflict_do_nothing()
            )
            await session.execute(stmt)
            await session.commit()
        logger.info("Moved to sold: %s", url)

    async def delete_active_ad(self, url: str) -> int:
        async with _base.AsyncSessionLocal() as session:
            result = await session.execute(
                delete(CianActiveAd).where(CianActiveAd.url == url)
            )
            removed = int(result.rowcount or 0)
            await session.commit()
        if removed:
            logger.info("delete_active_ad: removed url=%s", url)
        return removed
