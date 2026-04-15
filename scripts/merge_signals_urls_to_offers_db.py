#!/usr/bin/env python3
"""
Добавляет URL объявлений в cian_active_ads (source=offers), чтобы их подхватил парсер.

Обычный цикл:
  1) (опционально) этот скрипт — merge из Google Sheets Signals_Parser или из файла
  2) парсер:
     python -m services.parser_cian.main --mode offers --skip-links --unparsed-links

Из корня репозитория.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Резервный список (если нет --from-sheet и нет --file)
DEFAULT_SIGNALS_URLS = """
https://www.cian.ru/sale/flat/321099393/
https://www.cian.ru/sale/flat/324994927/
https://www.cian.ru/sale/flat/327851272/
https://www.cian.ru/sale/flat/326473705/
https://www.cian.ru/sale/flat/323398641/
https://www.cian.ru/sale/flat/324957099/
https://www.cian.ru/sale/flat/327784824/
https://www.cian.ru/sale/flat/328247289/
https://www.cian.ru/sale/flat/323526253/
https://www.cian.ru/sale/flat/328331607/
https://www.cian.ru/sale/flat/320499389/
https://www.cian.ru/sale/flat/322231937/
https://www.cian.ru/sale/flat/328827881/
https://www.cian.ru/sale/flat/327496666/
https://www.cian.ru/sale/flat/323615973/
https://www.cian.ru/sale/flat/328157523/
https://www.cian.ru/sale/flat/328199820/
https://www.cian.ru/sale/flat/327413133/
https://www.cian.ru/sale/flat/328696055/
https://www.cian.ru/sale/flat/327618210/
https://www.cian.ru/sale/flat/328622680/
https://www.cian.ru/sale/flat/327025703/
https://www.cian.ru/sale/flat/328026893/
https://www.cian.ru/sale/flat/327615564/
https://www.cian.ru/sale/flat/326669444/
https://www.cian.ru/sale/flat/328288116/
https://www.cian.ru/sale/flat/320539930/
https://www.cian.ru/sale/flat/321500382/
https://www.cian.ru/sale/flat/325871679/
https://www.cian.ru/sale/flat/328493653/
https://www.cian.ru/sale/flat/318562105/
https://www.cian.ru/sale/flat/295103950/
https://www.cian.ru/sale/flat/328361273/
https://www.cian.ru/sale/flat/328243746/
https://www.cian.ru/sale/flat/327649056/
https://www.cian.ru/sale/flat/328213857/
https://www.cian.ru/sale/flat/326533528/
https://www.cian.ru/sale/flat/321082203/
https://www.cian.ru/sale/flat/326329297/
https://www.cian.ru/sale/flat/322075612/
https://www.cian.ru/sale/flat/327875895/
https://www.cian.ru/sale/flat/328028598/
https://www.cian.ru/sale/flat/327239653/
https://www.cian.ru/sale/flat/314742332/
https://www.cian.ru/sale/flat/327671593/
https://www.cian.ru/sale/flat/327974412/
https://www.cian.ru/sale/flat/326565186/
https://www.cian.ru/sale/flat/327930355/
https://www.cian.ru/sale/flat/327848541/
https://www.cian.ru/sale/flat/327183075/
https://www.cian.ru/sale/flat/328250990/
https://www.cian.ru/sale/flat/326775558/
https://www.cian.ru/sale/flat/325960202/
https://www.cian.ru/sale/flat/327688211/
https://www.cian.ru/sale/flat/326737645/
https://www.cian.ru/sale/flat/328215852/
https://www.cian.ru/sale/flat/328182807/
https://www.cian.ru/sale/flat/322564873/
https://www.cian.ru/sale/flat/328217105/
https://www.cian.ru/sale/flat/325974726/
https://www.cian.ru/sale/flat/322366735/
https://www.cian.ru/sale/flat/326707895/
https://www.cian.ru/sale/flat/326006334/
https://www.cian.ru/sale/flat/328097240/
https://www.cian.ru/sale/flat/328291538/
https://www.cian.ru/sale/flat/324893081/
https://www.cian.ru/sale/flat/327973485/
https://www.cian.ru/sale/flat/328324708/
https://www.cian.ru/sale/flat/326096483/
https://www.cian.ru/sale/flat/311299339/
https://www.cian.ru/sale/flat/325982354/
https://www.cian.ru/sale/flat/327699912/
https://www.cian.ru/sale/flat/327054273/
https://www.cian.ru/sale/flat/328608344/
https://www.cian.ru/sale/flat/328225187/
https://www.cian.ru/sale/flat/320352400/
https://www.cian.ru/sale/flat/327836078/
https://www.cian.ru/sale/flat/327785568/
https://www.cian.ru/sale/flat/327394005/
https://www.cian.ru/sale/flat/326088716/
https://www.cian.ru/sale/flat/327381328/
https://www.cian.ru/sale/flat/326352032/
https://www.cian.ru/sale/flat/327688732/
https://www.cian.ru/sale/flat/322183903/
https://www.cian.ru/sale/flat/327206792/
https://www.cian.ru/sale/flat/325525622/
https://www.cian.ru/sale/flat/325882026/
https://www.cian.ru/sale/flat/325263270/
https://www.cian.ru/sale/flat/324507077/
https://www.cian.ru/sale/flat/326737104/
https://www.cian.ru/sale/flat/325642151/
https://www.cian.ru/sale/flat/322883007/
https://www.cian.ru/sale/flat/327273700/
https://www.cian.ru/sale/flat/327948370/
https://www.cian.ru/sale/flat/323353843/
https://www.cian.ru/sale/flat/325886772/
https://www.cian.ru/sale/flat/326039602/
https://www.cian.ru/sale/flat/323625633/
https://www.cian.ru/sale/flat/328286604/
https://www.cian.ru/sale/flat/328329491/
https://www.cian.ru/sale/flat/328694230/
https://www.cian.ru/sale/flat/327086589/
https://www.cian.ru/sale/flat/327757508/
https://www.cian.ru/sale/flat/321919159/
https://www.cian.ru/sale/flat/326776960/
https://www.cian.ru/sale/flat/313108991/
https://www.cian.ru/sale/flat/325433457/
https://www.cian.ru/sale/flat/328481787/
https://www.cian.ru/sale/flat/325385562/
https://www.cian.ru/sale/flat/327534354/
https://www.cian.ru/sale/flat/326531114/
https://www.cian.ru/sale/flat/327258499/
https://www.cian.ru/sale/flat/326747521/
https://www.cian.ru/sale/flat/328257574/
https://www.cian.ru/sale/flat/326747266/
https://www.cian.ru/sale/flat/320994769/
https://www.cian.ru/sale/flat/326247322/
https://www.cian.ru/sale/flat/325313992/
https://www.cian.ru/sale/flat/328079924/
https://www.cian.ru/sale/flat/323203043/
https://www.cian.ru/sale/flat/327692704/
https://www.cian.ru/sale/flat/325985047/
https://www.cian.ru/sale/flat/327134995/
https://www.cian.ru/sale/flat/320702914/
https://www.cian.ru/sale/flat/326068756/
https://www.cian.ru/sale/flat/325244329/
https://www.cian.ru/sale/flat/326565825/
https://www.cian.ru/sale/flat/326898144/
https://www.cian.ru/sale/flat/326788653/
https://www.cian.ru/sale/flat/327154070/
https://www.cian.ru/sale/flat/328289748/
https://www.cian.ru/sale/flat/318237695/
https://www.cian.ru/sale/flat/326124801/
https://www.cian.ru/sale/flat/327989422/
https://www.cian.ru/sale/flat/321397385/
https://www.cian.ru/sale/flat/325349542/
https://www.cian.ru/sale/flat/328813666/
https://www.cian.ru/sale/flat/309025266/
https://www.cian.ru/sale/flat/327814480/
https://www.cian.ru/sale/flat/324446530/
https://www.cian.ru/sale/flat/326565782/
https://www.cian.ru/sale/flat/326249896/
https://www.cian.ru/sale/flat/322493359/
https://www.cian.ru/sale/flat/328496695/
https://www.cian.ru/sale/flat/323674047/
https://www.cian.ru/sale/flat/327635229/
https://www.cian.ru/sale/flat/326410227/
https://www.cian.ru/sale/flat/328217578/
https://www.cian.ru/sale/flat/324360161/
https://www.cian.ru/sale/flat/327394702/
https://www.cian.ru/sale/flat/326131770/
https://www.cian.ru/sale/flat/326161637/
https://www.cian.ru/sale/flat/318158296/
https://www.cian.ru/sale/flat/326896513/
https://www.cian.ru/sale/flat/326986392/
https://www.cian.ru/sale/flat/327087164/
https://www.cian.ru/sale/flat/328135344/
https://www.cian.ru/sale/flat/327551425/
https://www.cian.ru/sale/flat/327617835/
https://www.cian.ru/sale/flat/328225876/
https://www.cian.ru/sale/flat/328066499/
https://www.cian.ru/sale/flat/327507288/
https://www.cian.ru/sale/flat/327942319/
https://www.cian.ru/sale/flat/327842361/
https://www.cian.ru/sale/flat/327802514/
https://www.cian.ru/sale/flat/327998838/
https://www.cian.ru/sale/flat/328331330/
https://www.cian.ru/sale/flat/326629427/
https://www.cian.ru/sale/flat/326522953/
https://www.cian.ru/sale/flat/321514480/
https://www.cian.ru/sale/flat/324542361/
https://www.cian.ru/sale/flat/325800342/
https://www.cian.ru/sale/flat/325600897/
https://www.cian.ru/sale/flat/327775966/
https://www.cian.ru/sale/flat/323475636/
https://www.cian.ru/sale/flat/327988152/
https://www.cian.ru/sale/flat/327975602/
https://www.cian.ru/sale/flat/327510530/
https://www.cian.ru/sale/flat/325884618/
https://www.cian.ru/sale/flat/326527429/
https://www.cian.ru/sale/flat/327626049/
https://www.cian.ru/sale/flat/326189287/
https://www.cian.ru/sale/flat/328532277/
https://www.cian.ru/sale/flat/328363739/
https://www.cian.ru/sale/flat/326534996/
https://www.cian.ru/sale/flat/300352416/
https://www.cian.ru/sale/flat/321837198/
https://www.cian.ru/sale/flat/319848354/
https://www.cian.ru/sale/flat/327723472/
https://www.cian.ru/sale/flat/327885528/
https://www.cian.ru/sale/flat/326950671/
https://www.cian.ru/sale/flat/324616198/
https://www.cian.ru/sale/flat/325678935/
https://www.cian.ru/sale/flat/326520517/
https://www.cian.ru/sale/flat/323802717/
https://www.cian.ru/sale/flat/326041734/
https://www.cian.ru/sale/flat/326490192/
https://www.cian.ru/sale/flat/328428999/
https://www.cian.ru/sale/flat/326567370/
https://www.cian.ru/sale/flat/327666861/
https://www.cian.ru/sale/flat/307773834/
https://www.cian.ru/sale/flat/327305437/
https://www.cian.ru/sale/flat/327879925/
https://www.cian.ru/sale/flat/326529890/
https://www.cian.ru/sale/flat/324702891/
https://www.cian.ru/sale/flat/328430728/
https://www.cian.ru/sale/flat/324942407/
https://www.cian.ru/sale/flat/328296390/
https://www.cian.ru/sale/flat/328226162/
https://www.cian.ru/sale/flat/327803704/
https://www.cian.ru/sale/flat/319982432/
https://www.cian.ru/sale/flat/327704379/
https://www.cian.ru/sale/flat/323868764/
https://www.cian.ru/sale/flat/327319662/
https://www.cian.ru/sale/flat/326969447/
https://www.cian.ru/sale/flat/327821375/
https://www.cian.ru/sale/flat/322505026/
https://www.cian.ru/sale/flat/326231903/
https://www.cian.ru/sale/flat/327864441/
https://www.cian.ru/sale/flat/328061275/
https://www.cian.ru/sale/flat/323833761/
https://www.cian.ru/sale/flat/327652537/
https://www.cian.ru/sale/flat/326829454/
https://www.cian.ru/sale/flat/327617471/
https://www.cian.ru/sale/flat/328107768/
https://www.cian.ru/sale/flat/326522669/
https://www.cian.ru/sale/flat/327312292/
https://www.cian.ru/sale/flat/328436042/
https://www.cian.ru/sale/flat/328101429/
https://www.cian.ru/sale/flat/328048728/
https://www.cian.ru/sale/flat/327734741/
https://www.cian.ru/sale/flat/327643720/
https://www.cian.ru/sale/flat/327333610/
https://www.cian.ru/sale/flat/299106024/
https://www.cian.ru/sale/flat/327784424/
https://www.cian.ru/sale/flat/325766758/
https://www.cian.ru/sale/flat/327810259/
https://www.cian.ru/sale/flat/324682017/
https://www.cian.ru/sale/flat/325966770/
https://www.cian.ru/sale/flat/326593196/
https://www.cian.ru/sale/flat/320823013/
https://www.cian.ru/sale/flat/328305863/
https://www.cian.ru/sale/flat/309617400/
https://www.cian.ru/sale/flat/326837852/
https://www.cian.ru/sale/flat/321953314/
https://www.cian.ru/sale/flat/321844310/
https://www.cian.ru/sale/flat/327921542/
https://www.cian.ru/sale/flat/327695500/
https://www.cian.ru/sale/flat/327096905/
https://www.cian.ru/sale/flat/327848844/
https://www.cian.ru/sale/flat/327847462/
https://www.cian.ru/sale/flat/327754531/
https://www.cian.ru/sale/flat/328289702/
https://www.cian.ru/sale/flat/327804853/
https://www.cian.ru/sale/flat/328421611/
https://www.cian.ru/sale/flat/327537628/
https://www.cian.ru/sale/flat/322720107/
https://www.cian.ru/sale/flat/324890061/
https://www.cian.ru/sale/flat/327282893/
https://www.cian.ru/sale/flat/326497061/
https://www.cian.ru/sale/flat/327630829/
https://www.cian.ru/sale/flat/328247753/
https://www.cian.ru/sale/flat/326136551/
https://www.cian.ru/sale/flat/328466511/
""".strip()


def _parse_urls_blob(blob: str) -> list[str]:
    out: list[str] = []
    for line in blob.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("http"):
            out.append(s)
    return list(dict.fromkeys(out))


def _read_urls_file(path: str) -> list[str]:
    out: list[str] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                out.append(s)
    return list(dict.fromkeys(out))


async def _amain(args: argparse.Namespace) -> None:
    from packages.flipper_core.sheets import SheetsManager
    from services.parser_cian.config import validate_config
    from services.parser_cian.db.repository import DatabaseRepository

    from services.parser_cian.config import settings
    validate_config()
    db_url = args.database_url or settings.database_url
    db = DatabaseRepository(database_url=db_url)
    await db.init_db()

    urls: list[str] = []
    if args.from_sheet:
        sm = SheetsManager()
        urls = sm.get_urls(tab_name="Signals_Parser", column="A")
        urls = list(dict.fromkeys([str(u).strip() for u in urls if str(u).strip()]))
    elif args.file:
        urls = _read_urls_file(args.file)
    else:
        urls = _parse_urls_blob(DEFAULT_SIGNALS_URLS)

    if not urls:
        print("Нет URL для merge.")
        return

    added = await db.merge_active_ad_urls(urls, source="offers")
    print(f"URL в списке: {len(urls)}, новых вставок в cian_active_ads: {added}")
    print("Дальше: python -m services.parser_cian.main --mode offers --skip-links --unparsed-links")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--database-url", default="", help="PostgreSQL URL (default: from settings)")
    ap.add_argument(
        "--from-sheet",
        action="store_true",
        help="Читать URL из вкладки Signals_Parser, колонка A (нужны .env и Google credentials)",
    )
    ap.add_argument("--file", default="", help="Файл: по одному URL на строку")
    args = ap.parse_args()
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
