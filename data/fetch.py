import argparse
import csv
import json
import re
import shutil
import subprocess
import sys
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SERVER_URL = "https://tableau-hs.immotors.com"
MOBILE_SERVER_URL = "https://mobile-tableau-hs.immotors.com"


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
        return value[1:-1]
    return value


def load_dotenv(dotenv_path: Path) -> dict:
    if not dotenv_path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = _strip_quotes(value.strip())
        env[key] = value
    return env


def resolve_tabcmd() -> str:
    p = shutil.which("tabcmd")
    if p:
        return p

    candidates = [
        Path("/opt/homebrew/bin/tabcmd"),
        Path("/usr/local/bin/tabcmd"),
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return "tabcmd"


def tabcmd_available() -> bool:
    p = shutil.which("tabcmd")
    if p:
        return True
    return Path("/opt/homebrew/bin/tabcmd").exists() or Path("/usr/local/bin/tabcmd").exists()


def run_command(command: list[str], timeout_s: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )


def tabcmd_login(server: str, token_name: str, token_value: str, timeout_s: int) -> None:
    tabcmd = resolve_tabcmd()
    server = server.split("#")[0] if "#" in server else server
    proc = run_command(
        [tabcmd, "login", "-s", server, "--token-name", token_name, "--token-value", token_value],
        timeout_s=timeout_s,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"tabcmd login failed (server={server}). stderr:\n{proc.stderr.strip()}")


def tabcmd_logout(timeout_s: int) -> None:
    tabcmd = resolve_tabcmd()
    proc = run_command([tabcmd, "logout"], timeout_s=timeout_s)
    if proc.returncode != 0:
        raise RuntimeError(f"tabcmd logout failed. stderr:\n{proc.stderr.strip()}")


def extract_view_info_from_url(view_url: str) -> tuple[str, str]:
    parsed = urlparse(view_url)
    fragment = parsed.fragment.lstrip("/")
    parts = [p for p in fragment.split("/") if p]
    if len(parts) >= 3 and parts[0] == "views":
        return "", f"{parts[1]}/{parts[2]}"
    if len(parts) >= 5 and parts[0] == "site" and parts[2] == "views":
        return parts[1], f"{parts[3]}/{parts[4]}"
    raise ValueError(f"Unrecognized tableau view url: {view_url}")


def export_csv(view_path: str, output_csv: Path, timeout_s: int) -> None:
    tabcmd = resolve_tabcmd()
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    proc = run_command([tabcmd, "export", view_path, "--csv", "-f", str(output_csv)], timeout_s=timeout_s)
    if proc.returncode != 0:
        raise RuntimeError(f"tabcmd export failed (view={view_path}). stderr:\n{proc.stderr.strip()}")


def http_request(
    method: str,
    url: str,
    *,
    auth_token: str | None = None,
    json_body: dict | None = None,
    timeout_s: int = 60,
    accept: str | None = None,
) -> tuple[int, bytes]:
    headers = {}
    if auth_token:
        headers["X-Tableau-Auth"] = auth_token
    if accept:
        headers["Accept"] = accept
    data = None
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        return e.code, body
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error: {e}") from e


def parse_versions_xml(xml_bytes: bytes) -> list[str]:
    root = ET.fromstring(xml_bytes.decode("utf-8", errors="ignore"))
    versions: list[str] = []
    for elem in root.iter():
        if elem.tag.endswith("version") and elem.text:
            versions.append(elem.text.strip())
    return [v for v in versions if re.match(r"^\d+\.\d+$", v)]


def choose_api_version(server: str, timeout_s: int) -> str:
    status, body = http_request("GET", f"{server.rstrip('/')}/api/versions", timeout_s=timeout_s)
    if status != 200:
        return "3.21"

    versions = parse_versions_xml(body)
    if not versions:
        return "3.21"

    def key(v: str) -> tuple[int, int]:
        major, minor = v.split(".", 1)
        return int(major), int(minor)

    return sorted(versions, key=key)[-1]


def tableau_sign_in(
    server: str,
    api_version: str,
    *,
    token_name: str,
    token_value: str,
    site_content_url: str,
    timeout_s: int,
) -> tuple[str, str]:
    payload = {
        "credentials": {
            "personalAccessTokenName": token_name,
            "personalAccessTokenSecret": token_value,
            "site": {"contentUrl": site_content_url},
        }
    }
    url = f"{server.rstrip('/')}/api/{api_version}/auth/signin"
    status, body = http_request("POST", url, json_body=payload, timeout_s=timeout_s, accept="application/json")
    if status != 200:
        raise RuntimeError(f"REST signin failed (status={status}). body:\n{body.decode('utf-8', errors='ignore')}")
    data = json.loads(body.decode("utf-8"))
    creds = data.get("credentials") or {}
    token = creds.get("token")
    site = creds.get("site") or {}
    site_id = site.get("id")
    if not token or not site_id:
        raise RuntimeError("REST signin response missing token/site id")
    return token, site_id


def tableau_sign_out(server: str, api_version: str, auth_token: str, timeout_s: int) -> None:
    url = f"{server.rstrip('/')}/api/{api_version}/auth/signout"
    status, _ = http_request("POST", url, auth_token=auth_token, timeout_s=timeout_s, accept="application/json")
    if status not in (200, 204):
        raise RuntimeError(f"REST signout failed (status={status})")


def tableau_find_workbook_id(
    server: str,
    api_version: str,
    *,
    auth_token: str,
    site_id: str,
    workbook_content_url: str,
    timeout_s: int,
) -> str:
    filter_expr = f"contentUrl:eq:{workbook_content_url}"
    query = quote(filter_expr, safe=":=")
    url = f"{server.rstrip('/')}/api/{api_version}/sites/{site_id}/workbooks?filter={query}"
    status, body = http_request("GET", url, auth_token=auth_token, timeout_s=timeout_s, accept="application/json")
    if status != 200:
        raise RuntimeError(
            f"REST query workbooks failed (status={status}, contentUrl={workbook_content_url}). body:\n"
            f"{body.decode('utf-8', errors='ignore')}"
        )
    data = json.loads(body.decode("utf-8"))
    workbooks = (data.get("workbooks") or {}).get("workbook") or []
    if isinstance(workbooks, dict):
        workbooks = [workbooks]
    for wb in workbooks:
        if isinstance(wb, dict) and wb.get("contentUrl") == workbook_content_url and wb.get("id"):
            return wb["id"]
    if workbooks and isinstance(workbooks[0], dict) and workbooks[0].get("id"):
        return workbooks[0]["id"]
    raise RuntimeError(f"Workbook not found for contentUrl={workbook_content_url}")


def tableau_list_workbook_views(
    server: str,
    api_version: str,
    *,
    auth_token: str,
    site_id: str,
    workbook_id: str,
    timeout_s: int,
) -> list[dict]:
    url = f"{server.rstrip('/')}/api/{api_version}/sites/{site_id}/workbooks/{workbook_id}/views"
    status, body = http_request("GET", url, auth_token=auth_token, timeout_s=timeout_s, accept="application/json")
    if status != 200:
        raise RuntimeError(
            f"REST query workbook views failed (status={status}, workbookId={workbook_id}). body:\n"
            f"{body.decode('utf-8', errors='ignore')}"
        )
    data = json.loads(body.decode("utf-8"))
    views = (data.get("views") or {}).get("view") or []
    if isinstance(views, dict):
        views = [views]
    return [v for v in views if isinstance(v, dict)]


def tableau_find_view_id(
    server: str,
    api_version: str,
    *,
    auth_token: str,
    site_id: str,
    view_content_url: str,
    timeout_s: int,
) -> str:
    candidates: list[str] = [view_content_url]
    if "/" in view_content_url:
        candidates.append(view_content_url.split("/", 1)[-1])

    for candidate in candidates:
        filter_expr = f"contentUrl:eq:{candidate}"
        query = quote(filter_expr, safe=":=")
        url = f"{server.rstrip('/')}/api/{api_version}/sites/{site_id}/views?filter={query}"
        status, body = http_request("GET", url, auth_token=auth_token, timeout_s=timeout_s, accept="application/json")
        if status != 200:
            continue
        data = json.loads(body.decode("utf-8"))
        views = (data.get("views") or {}).get("view") or []
        if isinstance(views, dict):
            views = [views]
        for v in views:
            if isinstance(v, dict) and v.get("id"):
                return v["id"]

    if "/" in view_content_url:
        workbook_url, sheet_url = view_content_url.split("/", 1)
        workbook_id = tableau_find_workbook_id(
            server,
            api_version,
            auth_token=auth_token,
            site_id=site_id,
            workbook_content_url=workbook_url,
            timeout_s=timeout_s,
        )
        views = tableau_list_workbook_views(
            server,
            api_version,
            auth_token=auth_token,
            site_id=site_id,
            workbook_id=workbook_id,
            timeout_s=timeout_s,
        )
        for v in views:
            content_url = v.get("contentUrl") or ""
            if content_url == view_content_url and v.get("id"):
                return v["id"]
            if isinstance(content_url, str) and content_url.split("/")[-1] == sheet_url and v.get("id"):
                return v["id"]

    raise RuntimeError(f"View not found for contentUrl={view_content_url}")


def tableau_download_view_data_csv(
    server: str,
    api_version: str,
    *,
    auth_token: str,
    site_id: str,
    view_id: str,
    timeout_s: int,
) -> bytes:
    url = f"{server.rstrip('/')}/api/{api_version}/sites/{site_id}/views/{view_id}/data?includeAllColumns=true"
    status, body = http_request("GET", url, auth_token=auth_token, timeout_s=timeout_s)
    if status != 200:
        raise RuntimeError(
            f"REST query view data failed (status={status}, viewId={view_id}). body:\n"
            f"{body.decode('utf-8', errors='ignore')}"
        )
    return body


def parse_data_info(md_path: Path, top_n: int) -> list[tuple[str, str]]:
    lines = md_path.read_text(encoding="utf-8").splitlines()
    results: list[tuple[str, str]] = []
    for line in lines:
        if len(results) >= top_n:
            break
        if "http" not in line:
            continue
        m = re.search(r"(https?://\S+)", line)
        if not m:
            continue
        url = m.group(1).strip()
        name = line[: m.start(1)].strip()
        name = re.sub(r"^\s*\d+\s*[、.]\s*", "", name)
        name = name.rstrip("：:").strip()
        if not name:
            name = f"view_{len(results)+1}"
        results.append((name, url))
    return results


def safe_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[\\/:\*\?\"<>\|]+", "_", name)
    name = name.strip("._")
    return name or "tableau_export"


def normalize_numeric_string(value: str) -> str:
    s = value.strip()
    if s == "":
        return s
    s_no_commas = s.replace(",", "")
    if re.match(r"^[-+]?\d+$", s_no_commas):
        return s_no_commas
    if re.match(r"^[-+]?\d+(\.\d+)?$", s_no_commas):
        return s_no_commas
    return s


def pivot_measure_long_to_wide(csv_path: Path) -> bool:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return False
        fieldnames = list(reader.fieldnames)
        if "度量名称" not in fieldnames or "度量值" not in fieldnames:
            return False

        id_cols = [c for c in fieldnames if c not in ("度量名称", "度量值")]
        measures_in_order: list[str] = []
        seen_measures: set[str] = set()
        rows_by_key: dict[tuple[str, ...], dict[str, str]] = {}

        for row in reader:
            key = tuple((row.get(c) or "").strip() for c in id_cols)
            measure_name = (row.get("度量名称") or "").strip()
            measure_value = normalize_numeric_string((row.get("度量值") or "").strip())
            if measure_name and measure_name not in seen_measures:
                measures_in_order.append(measure_name)
                seen_measures.add(measure_name)

            out_row = rows_by_key.get(key)
            if out_row is None:
                out_row = {c: key[i] for i, c in enumerate(id_cols)}
                rows_by_key[key] = out_row

            if measure_name:
                existing = out_row.get(measure_name, "")
                if existing == "" and measure_value != "":
                    out_row[measure_name] = measure_value

        out_fields = id_cols + measures_in_order

    tmp_path = csv_path.with_name(f".{csv_path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
        writer.writeheader()
        for out_row in rows_by_key.values():
            for m in measures_in_order:
                if m not in out_row:
                    out_row[m] = ""
            writer.writerow(out_row)

    tmp_path.replace(csv_path)
    return True


def infer_dtype(values: list[str]) -> str:
    cleaned = [v for v in values if v.strip() != ""]
    if not cleaned:
        return "empty"

    def is_int(s: str) -> bool:
        t = s.replace(",", "")
        return t.isdigit() or (t.startswith("-") and t[1:].isdigit())

    def is_float(s: str) -> bool:
        t = s.replace(",", "")
        if is_int(t):
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


def schema_for_csv(csv_path: Path) -> tuple[int, list[str], dict[str, str], dict[str, str], dict[str, float]]:
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
        dtypes = {c: infer_dtype(samples[c]) for c in cols}
        null_rates = {c: (nulls[c] / total if total else 0.0) for c in cols}
        return total, cols, examples, dtypes, null_rates


def build_schema_markdown(items: list[tuple[str, Path]]) -> str:
    lines: list[str] = []
    lines.append("## 信息模块（Schema）")
    lines.append("")
    for name, csv_path in items:
        total, cols, examples, dtypes, null_rates = schema_for_csv(csv_path)
        lines.append(f"### {name}")
        lines.append(f"- 文件: {csv_path.name}")
        lines.append(f"- 行数: {total}")
        lines.append("| 字段 | value 举例 | dtype | 空值率 |")
        lines.append("|---|---|---|---|")
        for c in cols:
            ex = (examples.get(c) or "").replace("\n", " ")
            if len(ex) > 80:
                ex = ex[:80]
            lines.append(f"| {c} | {ex} | {dtypes.get(c, 'string')} | {null_rates.get(c, 0.0):.2%} |")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def update_data_info_schema(md_path: Path, schema_md: str) -> None:
    text = md_path.read_text(encoding="utf-8")
    pattern = re.compile(r"^## 信息模块（Schema）[\s\S]*\Z", re.MULTILINE)
    if pattern.search(text):
        new_text = pattern.sub(schema_md.rstrip() + "\n", text)
    else:
        sep = "" if text.endswith("\n") else "\n"
        new_text = text + sep + "\n" + schema_md
    md_path.write_text(new_text, encoding="utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Export Tableau views (from data_info.md) as CSV into ./data")
    parser.add_argument("--server", default=DEFAULT_SERVER_URL)
    parser.add_argument("--mobile", action="store_true")
    parser.add_argument("--data-info", default=str(REPO_ROOT / "data" / "data_info.md"))
    parser.add_argument("--top", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-fetch", action="store_true")
    parser.add_argument("--no-transform", action="store_true")
    parser.add_argument("--no-schema", action="store_true")
    args = parser.parse_args(argv)

    dotenv = load_dotenv(REPO_ROOT / ".env")
    token_name = dotenv.get("TABLEAU_TOKEN_NAME", "").strip()
    token_value = dotenv.get("TABLEAU_TOKEN_VALUE", "").strip()
    if not token_name or not token_value:
        print("Missing TABLEAU_TOKEN_NAME / TABLEAU_TOKEN_VALUE in .env", file=sys.stderr)
        return 2

    views = parse_data_info(Path(args.data_info), top_n=args.top)
    if not views:
        print(f"No view urls found in {args.data_info}", file=sys.stderr)
        return 2

    view_infos: list[tuple[str, str, str]] = []
    for name, url in views:
        site_content_url, view_path = extract_view_info_from_url(url)
        view_infos.append((name, site_content_url, view_path))

    servers_to_try = []
    if args.mobile:
        servers_to_try = [MOBILE_SERVER_URL, args.server]
    else:
        servers_to_try = [args.server, MOBILE_SERVER_URL]

    errors: list[str] = []
    if args.dry_run:
        for name, site_content_url, view_path in view_infos:
            out_path = REPO_ROOT / "data" / f"{safe_filename(name)}.csv"
            site_hint = f"(site={site_content_url}) " if site_content_url else ""
            print(f"[dry-run] {site_hint}{name}: {view_path} -> {out_path}")
        return 0

    output_items: list[tuple[str, Path]] = [
        (name, REPO_ROOT / "data" / f"{safe_filename(name)}.csv") for name, _, _ in view_infos
    ]

    if args.no_fetch:
        for name, out_path in output_items:
            if not out_path.exists():
                print(f"Missing csv file for {name}: {out_path}", file=sys.stderr)
                return 2
    else:
        active_server: str | None = None
        if tabcmd_available():
            for server in servers_to_try:
                try:
                    tabcmd_login(server, token_name=token_name, token_value=token_value, timeout_s=args.timeout)
                    active_server = server
                    break
                except Exception:
                    continue

            if not active_server:
                print("Failed to login to tableau server via tabcmd (all candidates).", file=sys.stderr)
                return 1

            try:
                for name, _, view_path in view_infos:
                    try:
                        out_path = REPO_ROOT / "data" / f"{safe_filename(name)}.csv"
                        print(f"Exporting {name} -> {out_path.name}")
                        export_csv(view_path=view_path, output_csv=out_path, timeout_s=args.timeout)
                    except Exception as e:
                        errors.append(f"{name}: {e}")
            finally:
                try:
                    tabcmd_logout(timeout_s=args.timeout)
                except Exception:
                    pass
        else:
            first_site = view_infos[0][1]
            if any(site != first_site for _, site, _ in view_infos):
                print("Multiple tableau sites detected in urls; not supported in one run.", file=sys.stderr)
                return 2

            active_api_version: str | None = None
            auth_token: str | None = None
            site_id: str | None = None
            for server in servers_to_try:
                try:
                    api_version = choose_api_version(server, timeout_s=args.timeout)
                    token, sid = tableau_sign_in(
                        server,
                        api_version,
                        token_name=token_name,
                        token_value=token_value,
                        site_content_url=first_site,
                        timeout_s=args.timeout,
                    )
                    active_server = server
                    active_api_version = api_version
                    auth_token = token
                    site_id = sid
                    break
                except Exception:
                    continue

            if not active_server:
                print("Failed to login to tableau server via REST API (all candidates).", file=sys.stderr)
                return 1

            try:
                if not active_api_version or not auth_token or not site_id:
                    raise RuntimeError("Missing REST session info after signin")
                for name, _, view_path in view_infos:
                    try:
                        out_path = REPO_ROOT / "data" / f"{safe_filename(name)}.csv"
                        print(f"Exporting {name} -> {out_path.name}")
                        view_id = tableau_find_view_id(
                            active_server,
                            active_api_version,
                            auth_token=auth_token,
                            site_id=site_id,
                            view_content_url=view_path,
                            timeout_s=args.timeout,
                        )
                        csv_bytes = tableau_download_view_data_csv(
                            active_server,
                            active_api_version,
                            auth_token=auth_token,
                            site_id=site_id,
                            view_id=view_id,
                            timeout_s=args.timeout,
                        )
                        out_path.parent.mkdir(parents=True, exist_ok=True)
                        out_path.write_bytes(csv_bytes)
                    except Exception as e:
                        errors.append(f"{name}: {e}")
            finally:
                try:
                    if active_api_version and auth_token:
                        tableau_sign_out(active_server, active_api_version, auth_token, timeout_s=args.timeout)
                except Exception:
                    pass

    if not args.no_transform:
        for _, out_path in output_items:
            if out_path.exists():
                pivot_measure_long_to_wide(out_path)

    if not args.no_schema:
        existing_items = [(name, out_path) for name, out_path in output_items if out_path.exists()]
        if existing_items:
            schema_md = build_schema_markdown(existing_items)
            update_data_info_schema(Path(args.data_info), schema_md)

    if errors:
        print("Some exports failed:", file=sys.stderr)
        for e in errors:
            print(f"- {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
