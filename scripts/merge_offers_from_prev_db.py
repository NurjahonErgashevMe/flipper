#!/usr/bin/env python3
"""
Добавляет в текущую БД активные объявления source=offers (и legacy regular),
которые есть в старой БД (prev), но отсутствуют в целевой.

- filter_id переносится по URL фильтра (id в разных файлах не совпадают).
- URL, уже присутствующие в cian_sold_ads целевой БД, не добавляются.
- Вставленные строки: is_parsed=0, parsed_data=NULL — чтобы прогнать парсер
  с --skip-links --unparsed-only и обновить Sheets.

Запуск из корня репозитория:
  python scripts/merge_offers_from_prev_db.py
  python scripts/merge_offers_from_prev_db.py --prev data/parser_cian_prev.db --target data/parser_cian.db
"""

from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
from datetime import datetime


def _table_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge missing offers from prev SQLite into target.")
    ap.add_argument("--prev", default="data/parser_cian_prev.db", help="Исходная (старая) БД")
    ap.add_argument("--target", default="data/parser_cian.db", help="Текущая БД (куда добавить)")
    ap.add_argument(
        "--no-backup",
        action="store_true",
        help="Не делать копию target перед изменениями",
    )
    args = ap.parse_args()

    prev_path = os.path.abspath(args.prev)
    target_path = os.path.abspath(args.target)

    if not os.path.isfile(prev_path):
        raise SystemExit(f"Нет файла prev: {prev_path}")
    if not os.path.isfile(target_path):
        raise SystemExit(f"Нет файла target: {target_path}")

    prev = sqlite3.connect(f"file:{prev_path}?mode=ro", uri=True)
    tgt = sqlite3.connect(target_path)
    tgt.execute("PRAGMA foreign_keys=ON")

    prev_cols = _table_cols(prev, "cian_active_ads")
    if "source" not in prev_cols:
        raise SystemExit("prev: нет колонки source в cian_active_ads")

    # Карта filter URL -> id в target
    filter_ids_target = {
        str(r[0]).strip(): int(r[1])
        for r in tgt.execute("SELECT url, id FROM cian_filters WHERE url IS NOT NULL")
        if str(r[0]).strip()
    }

    prev_filters = {
        int(r[0]): str(r[1]).strip()
        for r in prev.execute("SELECT id, url FROM cian_filters WHERE url IS NOT NULL")
    }

    active_target = {
        str(r[0]).strip()
        for r in tgt.execute(
            "SELECT url FROM cian_active_ads WHERE source IN ('offers', 'regular')"
        )
    }
    sold_target = {str(r[0]).strip() for r in tgt.execute("SELECT url FROM cian_sold_ads")}

    q = """
        SELECT url, filter_id, source
        FROM cian_active_ads
        WHERE source IN ('offers', 'regular')
    """
    to_insert: list[tuple[str, int | None, str]] = []
    for url, filter_id, source in prev.execute(q):
        u = str(url).strip()
        if not u:
            continue
        if u in active_target or u in sold_target:
            continue
        fid_prev = filter_id
        filter_url = prev_filters.get(int(fid_prev)) if fid_prev is not None else None
        new_fid: int | None = None
        if filter_url:
            new_fid = filter_ids_target.get(filter_url)
        src = "offers" if source == "regular" else source
        to_insert.append((u, new_fid, src))

    if not to_insert:
        print("Нечего добавлять: все URL из prev уже есть в target (active) или в sold.")
        prev.close()
        tgt.close()
        return

    if not args.no_backup:
        bak = f"{target_path}.bak-merge-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        shutil.copy2(target_path, bak)
        print(f"Бэкап: {bak}")

    n = 0
    for u, new_fid, src in to_insert:
        tgt.execute(
            """
            INSERT INTO cian_active_ads (url, filter_id, source, parsed_data, is_parsed)
            VALUES (?, ?, ?, NULL, 0)
            """,
            (u, new_fid, src),
        )
        n += 1
    tgt.commit()

    print(f"Добавлено строк в cian_active_ads: {n} (offers, is_parsed=0, parsed_data=NULL)")
    if any(t[1] is None for t in to_insert):
        print(
            "Часть объявлений без filter_id (фильтр из prev отсутствует в target по URL) — "
            "парсинг всё равно возможен."
        )
    print(
        "Дальше: python -m services.parser_cian.main --mode offers --skip-links --unparsed-only"
    )

    prev.close()
    tgt.close()


if __name__ == "__main__":
    main()
