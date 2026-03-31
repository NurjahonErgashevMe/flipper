import json
import os
import re
from glob import glob


DIR_PATH = os.path.join(os.path.dirname(__file__), "parseddata")

OKRUG_RE = re.compile(r"^[А-ЯЁ]{2,5}$")
BAD_LITERALS = {
    "flat",
    "null",
    "none",
    "nil",
    "nan",
    "n/a",
    "нет данных",
    "не указано",
    "unknown",
    "undefined",
    "-",
}

BUILDING_WORDS = {
    "панельный",
    "кирпичный",
    "монолитный",
    "блочный",
    "монолитно-кирпичный",
    "деревянный",
    "сталинский",
}
HOUSING_WORDS = {"вторичка", "новостройка"}


def _norm(s: object) -> str:
    return (s or "").strip() if isinstance(s, str) else ""


def main() -> int:
    files = sorted(glob(os.path.join(DIR_PATH, "*.json")))
    summary = {
        "files": len(files),
        "json_read_error": 0,
        "missing_building_type": 0,
        "bad_housing_type": 0,
        "bad_building_type": 0,
        "bad_okrug": 0,
        "bad_district": 0,
        "bad_literals": 0,
    }
    issues: list[tuple[str, list[str]]] = []

    for fp in files:
        name = os.path.basename(fp)
        try:
            with open(fp, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception as e:
            summary["json_read_error"] += 1
            issues.append((name, [f"json_read_error: {e}"]))
            continue

        addr = d.get("address") or {}
        housing_type = _norm(d.get("housing_type"))
        building_type = _norm(d.get("building_type"))
        district = _norm(addr.get("district"))
        okrug = _norm(addr.get("okrug"))

        file_issues: list[str] = []

        if not building_type:
            summary["missing_building_type"] += 1
            file_issues.append("missing building_type")

        if housing_type:
            ht = housing_type.lower()
            if ht in BUILDING_WORDS or (
                ("вторич" not in ht) and ("новост" not in ht) and (ht not in HOUSING_WORDS)
            ):
                summary["bad_housing_type"] += 1
                file_issues.append(f"bad housing_type={housing_type!r}")

        if building_type:
            bt = building_type.lower()
            if bt in HOUSING_WORDS or ("вторич" in bt) or ("новост" in bt):
                summary["bad_building_type"] += 1
                file_issues.append(f"bad building_type={building_type!r}")

        if okrug:
            if not OKRUG_RE.match(okrug):
                summary["bad_okrug"] += 1
                file_issues.append(f"bad okrug={okrug!r}")
        else:
            summary["bad_okrug"] += 1
            file_issues.append("missing okrug")

        if district:
            if OKRUG_RE.match(district) or len(district) < 3:
                summary["bad_district"] += 1
                file_issues.append(f"bad district={district!r}")
        else:
            summary["bad_district"] += 1
            file_issues.append("missing district")

        for field_name, val in [
            ("housing_type", d.get("housing_type")),
            ("building_type", d.get("building_type")),
            ("renovation", d.get("renovation")),
            ("address.district", addr.get("district")),
            ("address.okrug", addr.get("okrug")),
        ]:
            if isinstance(val, str) and val.strip().lower() in BAD_LITERALS:
                summary["bad_literals"] += 1
                file_issues.append(f"bad literal {field_name}={val!r}")

        if file_issues:
            issues.append((name, file_issues))

    print("FILES", summary["files"])
    print("SUMMARY", json.dumps(summary, ensure_ascii=False, indent=2))
    print("ISSUES_COUNT", len(issues))
    for name, iss in issues:
        print("-", name, "; ".join(iss))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

