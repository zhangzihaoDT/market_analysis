import argparse
import csv
import re
import sys
from pathlib import Path
import json


MONTH_COL_RE = re.compile(r"^(?P<yy>\d{2}) 年 (?P<m>\d{1,2}) 月销量$")


def parse_int(value: str | None) -> int:
    if value is None:
        return 0
    s = str(value).strip()
    if s == "":
        return 0
    return int(s.replace(",", ""))


def find_month_columns(fieldnames: list[str]) -> dict[tuple[int, int], str]:
    mapping: dict[tuple[int, int], str] = {}
    for c in fieldnames:
        m = MONTH_COL_RE.match(c.strip())
        if not m:
            continue
        year = 2000 + int(m.group("yy"))
        month = int(m.group("m"))
        mapping[(year, month)] = c
    return mapping


def prev_month(year: int, month: int) -> tuple[int, int]:
    if month > 1:
        return year, month - 1
    return year - 1, 12


def safe_div(numer: int, denom: int) -> float | None:
    if denom == 0:
        return None
    return numer / denom


def compute_brand_metrics(input_csv: Path) -> tuple[tuple[int, int], list[dict]]:
    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        if "品牌" not in fieldnames:
            raise RuntimeError("Missing required column: 品牌")

        month_cols = find_month_columns(fieldnames)
        if not month_cols:
            raise RuntimeError("No monthly sales columns found (e.g. '26 年 2 月销量')")

        max_ym = max(month_cols.keys())
        y, m = max_ym
        prev_ym = prev_month(y, m)
        yoy_ym = (y - 1, m)

        ytd_months = [(y, mm) for mm in range(1, m + 1)]
        ytd_prev_months = [(y - 1, mm) for mm in range(1, m + 1)]

        agg: dict[str, dict[str, int]] = {}
        for row in reader:
            brand = (row.get("品牌") or "").strip()
            if brand == "" or brand == "全部":
                continue
            a = agg.get(brand)
            if a is None:
                a = {
                    "max": 0,
                    "prev": 0,
                    "yoy": 0,
                    "ytd": 0,
                    "ytd_prev": 0,
                }
                agg[brand] = a

            max_col = month_cols.get(max_ym)
            if max_col:
                a["max"] += parse_int(row.get(max_col))

            prev_col = month_cols.get(prev_ym)
            if prev_col:
                a["prev"] += parse_int(row.get(prev_col))

            yoy_col = month_cols.get(yoy_ym)
            if yoy_col:
                a["yoy"] += parse_int(row.get(yoy_col))

            for ym in ytd_months:
                c = month_cols.get(ym)
                if c:
                    a["ytd"] += parse_int(row.get(c))

            for ym in ytd_prev_months:
                c = month_cols.get(ym)
                if c:
                    a["ytd_prev"] += parse_int(row.get(c))

    rows: list[dict] = []
    for brand in sorted(agg.keys()):
        a = agg[brand]
        max_sales = a["max"]
        prev_sales = a["prev"]
        yoy_sales = a["yoy"]
        ytd_sales = a["ytd"]
        ytd_prev_sales = a["ytd_prev"]

        mom = safe_div(max_sales - prev_sales, prev_sales)
        yoy = safe_div(max_sales - yoy_sales, yoy_sales)
        ytd_yoy = safe_div(ytd_sales - ytd_prev_sales, ytd_prev_sales)

        rows.append(
            {
                "品牌": brand,
                "max_year": str(y),
                "max_month": str(m),
                "max_年月": f"{y:04d}-{m:02d}",
                "max月销量": str(max_sales),
                "上月销量": str(prev_sales),
                "月环比": "" if mom is None else f"{mom:.6f}",
                "去年同月销量": str(yoy_sales),
                "月同比": "" if yoy is None else f"{yoy:.6f}",
                "累计销量(1~Max月)": str(ytd_sales),
                "去年累计销量(1~Max月)": str(ytd_prev_sales),
                "累计同比(1~Max月)": "" if ytd_yoy is None else f"{ytd_yoy:.6f}",
            }
        )

    return max_ym, rows


def main(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parents[1]
    default_input = repo_root / "data" / "重点关注新能源品牌.csv"
    default_output = repo_root / "out" / "重点关注新能源品牌_品牌指标.csv"

    parser = argparse.ArgumentParser(description="Compute brand KPIs from 重点关注新能源品牌.csv")
    parser.add_argument("--input", default=str(default_input))
    parser.add_argument("--output", default=str(default_output))
    args = parser.parse_args(argv)

    input_csv = Path(args.input)
    output_csv = Path(args.output)
    max_ym, rows = compute_brand_metrics(input_csv)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "品牌",
        "max_year",
        "max_month",
        "max_年月",
        "max月销量",
        "上月销量",
        "月环比",
        "去年同月销量",
        "月同比",
        "累计销量(1~Max月)",
        "去年累计销量(1~Max月)",
        "累计同比(1~Max月)",
    ]
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    y, m = max_ym
    print(f"max 年-月: {y:04d}-{m:02d}")
    print(f"输出: {output_csv}")
    print(f"品牌数: {len(rows)}")

    # 输出 schema JSON
    def infer_dtype(values: list[str]) -> str:
        cleaned = [v for v in values if v.strip() != ""]
        if not cleaned:
            return "empty"
        def is_int(s: str) -> bool:
            t = s.replace(",", "")
            return t.isdigit() or (t.startswith("-") and t[1:].isdigit())
        def is_float(s: str) -> bool:
            t = s.replace(",", "")
            if is_int(s):
                return True
            return bool(re.match(r"^[-+]?\d+(\.\d+)?$", t))
        int_ok = True
        float_ok = True
        for s in cleaned[:5000]:
            if not is_int(s):
                int_ok = False
            if not is_float(s):
                float_ok = False
                break
        if int_ok:
            return "int"
        if float_ok:
            return "float"
        return "string"

    def build_schema(csv_path: Path) -> dict:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            cols = list(reader.fieldnames or [])
            nulls = {c: 0 for c in cols}
            examples = {c: "" for c in cols}
            seen_example = {c: False for c in cols}
            samples: dict[str, list[str]] = {c: [] for c in cols}
            total = 0
            for row in reader:
                total += 1
                for c in cols:
                    v = row.get(c)
                    s = "" if v is None else str(v).strip()
                    if s == "":
                        nulls[c] += 1
                    else:
                        if not seen_example[c]:
                            examples[c] = s
                            seen_example[c] = True
                        if len(samples[c]) < 5000:
                            samples[c].append(s)
        fields = []
        for c in cols:
            fields.append({
                "name": c,
                "example": examples.get(c, ""),
                "dtype": infer_dtype(samples[c]),
                "null_rate": nulls[c] / total if total else 0.0
            })
        return {
            "file": csv_path.name,
            "rows": total,
            "fields": fields
        }

    schema = build_schema(output_csv)
    schema_path = output_csv.with_suffix(".schema.json")
    schema_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"输出Schema: {schema_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
