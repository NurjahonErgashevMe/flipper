"""
Миграция данных из SQLite (data/parser_cian.db) в PostgreSQL.

Запускать ОДИН раз при деплое. PostgreSQL должен быть уже запущен.

Использование внутри контейнера parser_cian:
  docker compose run --rm parser_cian python scripts/migrate_sqlite_to_postgres.py

Или с хоста (если установлены зависимости):
  python scripts/migrate_sqlite_to_postgres.py \
      --sqlite data/parser_cian.db \
      --pg "postgresql+asyncpg://flipper:flipper_secret@localhost:5432/flipper"
"""

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text
from services.parser_cian.db import base as db_base

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("migrate")

DEFAULT_SQLITE = "data/parser_cian.db"
DEFAULT_PG = "postgresql+asyncpg://flipper:flipper_secret@app_postgres:5432/flipper"


def read_sqlite(db_path: str) -> dict:
    """Read all data from SQLite into memory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()]
    logger.info("SQLite tables: %s", tables)

    data = {}

    if "cian_filters" in tables:
        rows = conn.execute("SELECT id, url, meta FROM cian_filters").fetchall()
        filters = []
        for r in rows:
            meta = r["meta"]
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except (json.JSONDecodeError, TypeError):
                    pass
            filters.append({"id": r["id"], "url": r["url"], "meta": meta})
        data["cian_filters"] = filters
        logger.info("cian_filters: %d rows", len(filters))

    if "cian_active_ads" in tables:
        rows = conn.execute(
            "SELECT id, url, filter_id, source, parsed_data, is_parsed, "
            "last_updated, added_at FROM cian_active_ads"
        ).fetchall()
        ads = []
        for r in rows:
            pd = r["parsed_data"]
            if isinstance(pd, (str, bytes)):
                try:
                    pd = json.loads(pd)
                except (json.JSONDecodeError, TypeError):
                    pd = None
            ads.append({
                "id": r["id"],
                "url": r["url"],
                "filter_id": r["filter_id"],
                "source": r["source"] or "offers",
                "parsed_data": pd,
                "is_parsed": bool(r["is_parsed"]),
                "last_updated": r["last_updated"],
                "added_at": r["added_at"],
            })
        data["cian_active_ads"] = ads
        logger.info("cian_active_ads: %d rows", len(ads))

    if "cian_sold_ads" in tables:
        rows = conn.execute(
            "SELECT id, url, parsed_data, publish_date, sold_at FROM cian_sold_ads"
        ).fetchall()
        sold = []
        for r in rows:
            pd = r["parsed_data"]
            if isinstance(pd, (str, bytes)):
                try:
                    pd = json.loads(pd)
                except (json.JSONDecodeError, TypeError):
                    pd = None
            sold.append({
                "id": r["id"],
                "url": r["url"],
                "parsed_data": pd,
                "publish_date": r["publish_date"],
                "sold_at": r["sold_at"],
            })
        data["cian_sold_ads"] = sold
        logger.info("cian_sold_ads: %d rows", len(sold))

    conn.close()
    return data


async def write_postgres(pg_url: str, data: dict) -> None:
    """Write data to PostgreSQL, creating tables first."""
    await db_base.init_db(pg_url)
    if db_base.AsyncSessionLocal is None:
        raise RuntimeError("AsyncSessionLocal is not initialized after init_db()")

    async with db_base.AsyncSessionLocal() as session:
        # Check if target tables already have data
        for table in ("cian_filters", "cian_active_ads", "cian_sold_ads"):
            result = await session.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            if count > 0:
                logger.warning("Table %s already has %d rows — will skip duplicates (ON CONFLICT)", table, count)

    # --- cian_filters ---
    filters = data.get("cian_filters", [])
    if filters:
        async with db_base.AsyncSessionLocal() as session:
            for f in filters:
                await session.execute(text(
                    "INSERT INTO cian_filters (id, url, meta) "
                    "VALUES (:id, :url, :meta::jsonb) "
                    "ON CONFLICT (url) DO NOTHING"
                ), {"id": f["id"], "url": f["url"], "meta": json.dumps(f["meta"]) if f["meta"] else None})
            await session.commit()

            # Reset sequence to max id
            await session.execute(text(
                "SELECT setval('cian_filters_id_seq', COALESCE((SELECT MAX(id) FROM cian_filters), 1))"
            ))
            await session.commit()
        logger.info("Inserted cian_filters: %d", len(filters))

    # --- cian_active_ads ---
    ads = data.get("cian_active_ads", [])
    if ads:
        chunk_size = 200
        inserted = 0
        async with db_base.AsyncSessionLocal() as session:
            for i in range(0, len(ads), chunk_size):
                chunk = ads[i:i + chunk_size]
                for ad in chunk:
                    await session.execute(text(
                        "INSERT INTO cian_active_ads "
                        "(id, url, filter_id, source, parsed_data, is_parsed, last_updated, added_at) "
                        "VALUES (:id, :url, :filter_id, :source, :parsed_data::jsonb, :is_parsed, "
                        "CASE WHEN :last_updated IS NOT NULL THEN :last_updated::timestamp ELSE CURRENT_TIMESTAMP END, "
                        "CASE WHEN :added_at IS NOT NULL THEN :added_at::timestamp ELSE CURRENT_TIMESTAMP END) "
                        "ON CONFLICT (url) DO NOTHING"
                    ), {
                        "id": ad["id"],
                        "url": ad["url"],
                        "filter_id": ad["filter_id"],
                        "source": ad["source"],
                        "parsed_data": json.dumps(ad["parsed_data"]) if ad["parsed_data"] else None,
                        "is_parsed": ad["is_parsed"],
                        "last_updated": ad["last_updated"],
                        "added_at": ad["added_at"],
                    })
                    inserted += 1
                await session.commit()
                logger.info("cian_active_ads: %d / %d", min(i + chunk_size, len(ads)), len(ads))

            await session.execute(text(
                "SELECT setval('cian_active_ads_id_seq', COALESCE((SELECT MAX(id) FROM cian_active_ads), 1))"
            ))
            await session.commit()
        logger.info("Inserted cian_active_ads: %d", inserted)

    # --- cian_sold_ads ---
    sold = data.get("cian_sold_ads", [])
    if sold:
        async with db_base.AsyncSessionLocal() as session:
            for s in sold:
                await session.execute(text(
                    "INSERT INTO cian_sold_ads (id, url, parsed_data, publish_date, sold_at) "
                    "VALUES (:id, :url, :parsed_data::jsonb, :publish_date, "
                    "CASE WHEN :sold_at IS NOT NULL THEN :sold_at::timestamp ELSE CURRENT_TIMESTAMP END) "
                    "ON CONFLICT (url) DO NOTHING"
                ), {
                    "id": s["id"],
                    "url": s["url"],
                    "parsed_data": json.dumps(s["parsed_data"]) if s["parsed_data"] else None,
                    "publish_date": s["publish_date"],
                    "sold_at": s["sold_at"],
                })
            await session.commit()

            await session.execute(text(
                "SELECT setval('cian_sold_ads_id_seq', COALESCE((SELECT MAX(id) FROM cian_sold_ads), 1))"
            ))
            await session.commit()
        logger.info("Inserted cian_sold_ads: %d", len(sold))


async def main(args):
    sqlite_path = args.sqlite
    pg_url = args.pg

    if not Path(sqlite_path).exists():
        logger.error("SQLite file not found: %s", sqlite_path)
        sys.exit(1)

    logger.info("=== Migration: SQLite -> PostgreSQL ===")
    logger.info("SQLite: %s", sqlite_path)
    logger.info("PostgreSQL: %s", pg_url.split("@")[-1])

    data = read_sqlite(sqlite_path)

    total = sum(len(v) for v in data.values())
    if total == 0:
        logger.warning("SQLite database is empty — nothing to migrate")
        return

    await write_postgres(pg_url, data)
    logger.info("=== Migration complete: %d total rows ===", total)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sqlite", default=DEFAULT_SQLITE, help="Path to SQLite file")
    ap.add_argument("--pg", default=DEFAULT_PG, help="PostgreSQL async URL")
    asyncio.run(main(ap.parse_args()))
