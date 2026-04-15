#!/usr/bin/env python3
"""
Печатает JSON body для POST {FIRECRAWL_BASE_URL}/v2/scrape — как в AdParser.parse_async:
excludeTags=EXCLUDE_TAGS, formats: markdown, rawHtml, json(schema=_get_schema(), systemPrompt=SYSTEM_PROMPT).

Запуск из корня репозитория:
  python scripts/dump_firecrawl_scrape_body.py
  python scripts/dump_firecrawl_scrape_body.py "https://www.cian.ru/sale/flat/326002860/" --cookie "name=value; ..."
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.parser_cian.parser import AdParser, EXCLUDE_TAGS, SYSTEM_PROMPT  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "url",
        nargs="?",
        default="https://www.cian.ru/sale/flat/326002860/",
        help="URL карточки Cian",
    )
    ap.add_argument(
        "--cookie",
        default="",
        help="Строка Cookie для Циан (пусто = без headers, как при отсутствии куков)",
    )
    ap.add_argument(
        "-o",
        "--output",
        default="",
        help="Записать JSON в файл (UTF-8); иначе stdout",
    )
    args = ap.parse_args()

    p = AdParser()
    body: dict = {
        "url": args.url.strip(),
        "excludeTags": EXCLUDE_TAGS,
        "formats": [
            "markdown",
            "rawHtml",
            {
                "type": "json",
                "schema": p._get_schema(),
                "systemPrompt": SYSTEM_PROMPT,
            },
        ],
    }
    if args.cookie.strip():
        body["headers"] = {"Cookie": args.cookie.strip()}

    text = json.dumps(body, ensure_ascii=False, indent=2)
    if args.output.strip():
        Path(args.output.strip()).write_text(text, encoding="utf-8")
    else:
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except Exception:
            pass
        print(text)


if __name__ == "__main__":
    main()
