#!/usr/bin/env python3
"""
Восстановление повреждённого parser_cian.db (malformed / integrity_check).

Копирует данные в новый файл через CAST(... AS BLOB) для JSON-колонок,
чтобы обойти «Could not decode to UTF-8» при чтении.

Использование (из корня репозитория):
  python scripts/recover_parser_cian_db.py
  python scripts/recover_parser_cian_db.py --path data/parser_cian.db

По умолчанию: бэкап в parser_cian.db.broken-TIMESTAMP, новый файл — parser_cian.db.recovered
Замена: --in-place (после проверки PRAGMA integrity_check на новом файле).
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import sys
from datetime import datetime


def _blob_to_json_obj(blob: bytes | None):
    if not blob:
        return None
    s = bytes(blob).decode("utf-8", errors="replace")
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None


def recover(src_path: str, dst_path: str) -> None:
    if os.path.abspath(src_path) == os.path.abspath(dst_path):
        raise SystemExit("src и dst должны различаться")

    if os.path.exists(dst_path):
        raise SystemExit(f"целевой файл уже существует: {dst_path}")

    src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
    dst = sqlite3.connect(dst_path)
    dst.execute("PRAGMA foreign_keys=ON")
    dst.execute("PRAGMA journal_mode=WAL")
    dst.execute("PRAGMA synchronous=NORMAL")

    s = src.cursor()
    d = dst.cursor()

    d.executescript(
        """
        CREATE TABLE cian_filters (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            url VARCHAR NOT NULL UNIQUE
        );
        CREATE TABLE cian_active_ads (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            url VARCHAR NOT NULL UNIQUE,
            filter_id INTEGER,
            source VARCHAR NOT NULL DEFAULT 'offers',
            parsed_data JSON,
            is_parsed BOOLEAN DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(filter_id) REFERENCES cian_filters (id)
        );
        CREATE TABLE cian_sold_ads (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            url VARCHAR NOT NULL UNIQUE,
            parsed_data JSON,
            publish_date VARCHAR,
            sold_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # cian_filters
    for row in s.execute("SELECT id, url FROM cian_filters"):
        d.execute("INSERT INTO cian_filters (id, url) VALUES (?, ?)", row)

    # cian_active_ads — без чтения parsed_data как TEXT
    q = """
        SELECT id, url, filter_id, source,
               CAST(parsed_data AS BLOB), is_parsed, last_updated, added_at
        FROM cian_active_ads
    """
    n_active = 0
    for row in s.execute(q):
        pid, url, fid, source, blob, is_parsed, lu, aa = row
        pd = _blob_to_json_obj(blob)
        d.execute(
            """
            INSERT INTO cian_active_ads
            (id, url, filter_id, source, parsed_data, is_parsed, last_updated, added_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (pid, url, fid, source, json.dumps(pd, ensure_ascii=False) if pd is not None else None, is_parsed, lu, aa),
        )
        n_active += 1

    q_sold = """
        SELECT id, url, CAST(parsed_data AS BLOB), publish_date, sold_at
        FROM cian_sold_ads
    """
    n_sold = 0
    for row in s.execute(q_sold):
        sid, url, blob, pub, sold_at = row
        pd = _blob_to_json_obj(blob)
        d.execute(
            """
            INSERT INTO cian_sold_ads (id, url, parsed_data, publish_date, sold_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (sid, url, json.dumps(pd, ensure_ascii=False) if pd is not None else None, pub, sold_at),
        )
        n_sold += 1

    dst.commit()

    # Синхронизация sqlite_sequence после INSERT с явными id (без дублей)
    try:
        d.execute(
            "DELETE FROM sqlite_sequence WHERE name IN (?, ?, ?)",
            ("cian_filters", "cian_active_ads", "cian_sold_ads"),
        )
    except sqlite3.OperationalError:
        pass
    for table in ("cian_filters", "cian_active_ads", "cian_sold_ads"):
        m = d.execute(f"SELECT MAX(id) FROM {table}").fetchone()[0]
        if m is not None:
            d.execute(
                "INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)",
                (table, m),
            )
    dst.commit()

    src.close()
    dst.close()

    chk = sqlite3.connect(dst_path)
    ok = chk.execute("PRAGMA integrity_check").fetchone()[0]
    chk.close()
    print(f"OK: записано cian_filters + {n_active} active + {n_sold} sold")
    print(f"PRAGMA integrity_check (новый файл): {ok}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="data/parser_cian.db", help="исходная БД")
    ap.add_argument("--out", default="", help="куда сохранить (по умолчанию path + .recovered)")
    ap.add_argument(
        "--in-place",
        action="store_true",
        help="переименовать старую в .broken и положить восстановленную на место path",
    )
    args = ap.parse_args()
    src = os.path.abspath(args.path)
    if not os.path.isfile(src):
        print(f"Нет файла: {src}", file=sys.stderr)
        sys.exit(1)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = f"{src}.broken-{ts}"
    out = args.out or f"{src}.recovered"

    shutil.copy2(src, backup)
    print(f"Бэкап повреждённого файла: {backup}")

    try:
        recover(src, out)
    except Exception as e:
        print(f"Ошибка восстановления: {e}", file=sys.stderr)
        sys.exit(1)

    chk = sqlite3.connect(out).execute("PRAGMA integrity_check").fetchone()[0]
    if chk != "ok":
        print("Новый файл всё ещё не ok — не подменяю.", file=sys.stderr)
        sys.exit(1)

    if args.in_place:
        os.remove(src)
        shutil.move(out, src)
        print(f"Готово: заменено на месте {src}")
    else:
        print(f"Готово: новый файл {out}")
        print(f"Проверьте приложение, затем: переименуйте {out} -> {src} (старый уже в {backup})")


if __name__ == "__main__":
    main()
