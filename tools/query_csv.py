import csv
import json
from pathlib import Path


def is_safe_data_path(repo_root: Path, path: Path) -> bool:
    try:
        rp = path.resolve()
    except Exception:
        return False
    allowed_dirs = [(repo_root / "out").resolve(), (repo_root / "data").resolve()]
    for d in allowed_dirs:
        try:
            rp.relative_to(d)
            return True
        except Exception:
            continue
    return False


def coerce_number(value: str) -> float | None:
    s = value.strip()
    if s == "":
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def row_matches_filters(row: dict[str, str], filters: list[dict]) -> bool:
    for f in filters:
        col = str(f.get("col") or "")
        op = str(f.get("op") or "eq")
        raw_val = f.get("value")
        cell = str(row.get(col, "") or "")
        if op == "contains":
            if str(raw_val or "") not in cell:
                return False
            continue
        if op == "in":
            values = raw_val if isinstance(raw_val, list) else []
            values_str = {str(v) for v in values}
            if cell not in values_str:
                return False
            continue

        if op in ("gt", "gte", "lt", "lte"):
            left = coerce_number(cell)
            right = coerce_number(str(raw_val or ""))
            if left is None or right is None:
                return False
            if op == "gt" and not (left > right):
                return False
            if op == "gte" and not (left >= right):
                return False
            if op == "lt" and not (left < right):
                return False
            if op == "lte" and not (left <= right):
                return False
            continue

        if op == "ne":
            if cell == str(raw_val or ""):
                return False
            continue

        if cell != str(raw_val or ""):
            return False
    return True


def query_csv_tool(
    repo_root: Path,
    *,
    path: str,
    select: list[str] | None,
    filters: list[dict] | None,
    order_by: dict | None,
    limit: int,
) -> str:
    p = Path(path)
    if not p.is_absolute():
        p = (repo_root / p).resolve()
    if not is_safe_data_path(repo_root, p):
        return json.dumps({"ok": False, "error": "path not allowed"}, ensure_ascii=False)
    if not p.exists():
        return json.dumps({"ok": False, "error": "file not found"}, ensure_ascii=False)
    if p.is_dir():
        # Return a helpful error with candidate CSVs under this directory
        candidates = [str(x) for x in sorted(p.glob("*.csv"))][:50]
        return json.dumps(
            {
                "ok": False,
                "error": "path is a directory; provide a CSV file path",
                "candidates": candidates,
            },
            ensure_ascii=False,
        )

    filters_list = filters or []
    limit = max(1, min(int(limit or 20), 500))
    rows: list[dict[str, str]] = []
    total_matched = 0
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        all_cols = list(reader.fieldnames or [])
        want_cols = [c for c in (select or []) if c in all_cols] if select else all_cols
        for row in reader:
            if not row_matches_filters(row, filters_list):
                continue
            total_matched += 1
            out_row = {c: str(row.get(c, "") or "") for c in want_cols}
            rows.append(out_row)

    if order_by and isinstance(order_by, dict):
        col = str(order_by.get("col") or "")
        desc = bool(order_by.get("desc"))
        if col:
            def key_fn(r: dict[str, str]):
                v = r.get(col, "")
                n = coerce_number(v)
                return (0, n) if n is not None else (1, v)
            rows.sort(key=key_fn, reverse=desc)

    rows = rows[:limit]
    return json.dumps(
        {
            "ok": True,
            "path": str(p),
            "columns": list(rows[0].keys()) if rows else [],
            "rows": rows,
            "total_matched": total_matched,
        },
        ensure_ascii=False,
    )
