import argparse
import csv
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

from tools.query_csv import query_csv_tool


def load_dotenv(dotenv_path: Path) -> dict[str, str]:
    if not dotenv_path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        env[k] = v
    return env


def http_post_json(url: str, *, headers: dict[str, str], body: dict, timeout_s: int) -> dict:
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url=url, method="POST", data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def list_script_tools(scripts_dir: Path) -> list[str]:
    if not scripts_dir.exists():
        return []
    out: list[str] = []
    for p in sorted(scripts_dir.glob("*.py")):
        if p.name.startswith("_"):
            continue
        out.append(p.name)
    return out


def preview_csv(path: Path, max_lines: int = 20) -> str:
    if not path.exists():
        return ""
    lines: list[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= max_lines:
                break
            lines.append(",".join(row))
    return "\n".join(lines)


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        n = -1
        for n, _ in enumerate(reader):
            pass
        return max(0, n)


def read_text(path: Path, max_chars: int = 5000) -> str:
    if not path.exists():
        return ""
    data = path.read_text(encoding="utf-8", errors="ignore")
    if len(data) > max_chars:
        return data[:max_chars]
    return data


def detect_output_paths(stdout: str) -> list[Path]:
    paths: list[Path] = []
    for line in stdout.splitlines():
        m = re.search(r"输出:\s*(.+)$", line.strip())
        if not m:
            continue
        p = Path(m.group(1).strip())
        paths.append(p)
        schema_path = p.with_suffix(".schema.json")
        if schema_path.exists():
            paths.append(schema_path)
    for line in stdout.splitlines():
        m = re.search(r"输出Schema:\s*(.+)$", line.strip())
        if not m:
            continue
        p = Path(m.group(1).strip())
        paths.append(p)
    return paths


def run_script_tool(repo_root: Path, *, script_name: str, args: list[str] | None, timeout_s: int) -> str:
    scripts_dir = repo_root / "scripts"
    allowed = set(list_script_tools(scripts_dir))
    if script_name not in allowed:
        return json.dumps({"ok": False, "error": f"script not allowed: {script_name}", "allowed": sorted(allowed)}, ensure_ascii=False)

    script_path = scripts_dir / script_name
    argv = [sys.executable, str(script_path)]
    if args:
        argv.extend(args)

    proc = subprocess.run(argv, capture_output=True, text=True, cwd=str(repo_root), timeout=timeout_s, check=False)
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    out_paths = detect_output_paths(stdout)
    outputs: list[dict] = []
    for p in out_paths:
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        if p.suffix.lower() == ".csv":
            outputs.append(
                {
                    "path": str(p),
                    "type": "csv",
                    "rows": count_csv_rows(p),
                    "preview": preview_csv(p, max_lines=20),
                }
            )
        elif p.name.endswith(".schema.json") or p.suffix.lower() == ".json":
            outputs.append(
                {
                    "path": str(p),
                    "type": "json",
                    "preview": read_text(p, max_chars=5000),
                }
            )
        else:
            outputs.append({"path": str(p), "type": "file"})

    return json.dumps(
        {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": stdout.strip(),
            "stderr": stderr.strip(),
            "outputs": outputs,
        },
        ensure_ascii=False,
    )


def log(line: str) -> None:
    sys.stderr.write(line.rstrip() + "\n")
    sys.stderr.flush()


def summarize_tool_result(tool_name: str, tool_content: str) -> str:
    try:
        data = json.loads(tool_content)
    except Exception:
        return f"[tool:{tool_name}] (non-json)\n{tool_content[:800]}"

    if tool_name == "run_script":
        outputs = data.get("outputs") or []
        output_paths = []
        for o in outputs:
            if isinstance(o, dict) and o.get("path"):
                output_paths.append(str(o.get("path")))
        stdout = (data.get("stdout") or "").strip()
        stdout_first = "\n".join(stdout.splitlines()[:10])
        return (
            f"[tool:run_script] ok={data.get('ok')} returncode={data.get('returncode')}\n"
            f"stdout(first 10 lines):\n{stdout_first}\n"
            f"outputs: {output_paths}"
        )

    if tool_name == "query_csv":
        return (
            f"[tool:query_csv] ok={data.get('ok')} path={data.get('path')} "
            f"total_matched={data.get('total_matched')} rows_returned={len(data.get('rows') or [])}"
        )

    return f"[tool:{tool_name}] ok={data.get('ok')}"


def deepseek_chat(
    *,
    base_url: str,
    api_key: str,
    model: str,
    messages: list[dict],
    tools: list[dict] | None,
    timeout_s: int,
    thinking: bool,
) -> dict:
    url = f"{base_url.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body: dict = {
        "model": model,
        "messages": messages,
    }
    if tools:
        body["tools"] = tools
    if thinking:
        body["thinking"] = {"type": "enabled"}
    return http_post_json(url, headers=headers, body=body, timeout_s=timeout_s)


def main(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parent
    dotenv = load_dotenv(repo_root / ".env")
    api_key = dotenv.get("DEEPSEEK_API_KEY") or os.environ.get("DEEPSEEK_API_KEY") or ""
    base_url = dotenv.get("DEEPSEEK_BASE_URL") or os.environ.get("DEEPSEEK_BASE_URL") or "https://api.deepseek.com"

    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default="")
    parser.add_argument("--model", default="deepseek-chat")
    parser.add_argument("--thinking", action="store_true")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args(argv)

    if not api_key:
        print("Missing DEEPSEEK_API_KEY in .env or environment", file=sys.stderr)
        return 2

    scripts_dir = repo_root / "scripts"
    script_names = list_script_tools(scripts_dir)
    tool_spec = [
        {
            "type": "function",
            "function": {
                "name": "run_script",
                "description": "Run a python script under ./scripts with arguments and return stdout/stderr and preview of output files",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "script_name": {"type": "string", "description": f"One of: {', '.join(script_names)}"},
                        "args": {"type": "array", "items": {"type": "string"}},
                        "timeout_s": {"type": "integer", "description": "Timeout seconds for running the script"},
                    },
                    "required": ["script_name"],
                },
            },
        }
        ,
        {
            "type": "function",
            "function": {
                "name": "query_csv",
                "description": "Query a CSV under ./out or ./data with simple filters and return rows as JSON",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "CSV path, absolute or relative to repo"},
                        "select": {"type": "array", "items": {"type": "string"}},
                        "filters": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "col": {"type": "string"},
                                    "op": {"type": "string", "description": "eq|ne|contains|in|gt|gte|lt|lte"},
                                    "value": {},
                                },
                                "required": ["col", "op", "value"],
                            },
                        },
                        "order_by": {
                            "type": "object",
                            "properties": {"col": {"type": "string"}, "desc": {"type": "boolean"}},
                        },
                        "limit": {"type": "integer"},
                    },
                    "required": ["path"],
                },
            },
        }
    ]

    query = args.query.strip()
    if not query:
        query = sys.stdin.read().strip()

    system_prompt = (
        "你是一个数据分析助手。你可以通过工具 run_script 来运行本仓库 ./scripts 下的脚本。\n"
        "脚本一般会把结果写入 ./out 目录，并在 stdout 中打印一行 '输出: <path>'，并可能输出 schema JSON。\n"
        "你还可以用工具 query_csv 对 ./out 或 ./data 下的 CSV 做筛选查询。\n"
        "你必须采用循环模式：每一轮先做 planning（明确需要哪些脚本、要读哪个 schema、要在 CSV 查什么），再执行查询；最多 5 轮。\n"
        "当你认为信息足够时，输出最终答案；如果还需要下一轮，输出 NEED_MORE。"
    )

    messages: list[dict] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": query},
    ]

    max_rounds = 5
    for round_idx in range(max_rounds):
        log(f"=== Loop Round {round_idx + 1}/{max_rounds}: planning ===")
        planning_prompt = (
            "请先输出本轮 planning，要求：\n"
            "1) 说明你将运行哪些脚本（如需要）以及为什么；\n"
            "2) 你将使用哪些 schema JSON 来确定列含义；\n"
            "3) 你将对哪些 CSV 发起哪些 query_csv 查询（filters/select/order_by/limit）。\n"
            "planning 输出完成后不要调用工具。若本轮无需继续，直接给出最终答案。"
        )
        resp_plan = deepseek_chat(
            base_url=base_url,
            api_key=api_key,
            model=args.model,
            messages=messages + [{"role": "user", "content": planning_prompt}],
            tools=None,
            timeout_s=args.timeout,
            thinking=args.thinking,
        )
        choice_plan = (resp_plan.get("choices") or [{}])[0]
        msg_plan = choice_plan.get("message") or {}
        if "role" not in msg_plan:
            msg_plan["role"] = "assistant"
        messages.append(msg_plan)
        plan_content = (msg_plan.get("content") or "").strip()
        if plan_content:
            log(plan_content)

        log(f"=== Loop Round {round_idx + 1}/{max_rounds}: execute ===")
        execution_prompt = (
            "按上一条 planning 执行：需要运行脚本就调用 run_script；需要查数据就调用 query_csv。\n"
            "在拿到足够信息前可多次调用工具。完成后：如果还需要下一轮，输出 NEED_MORE；否则输出最终答案。"
        )
        messages.append({"role": "user", "content": execution_prompt})

        while True:
            resp = deepseek_chat(
                base_url=base_url,
                api_key=api_key,
                model=args.model,
                messages=messages,
                tools=tool_spec,
                timeout_s=args.timeout,
                thinking=args.thinking,
            )
            choice = (resp.get("choices") or [{}])[0]
            msg = choice.get("message") or {}
            messages.append(msg)
            tool_calls = msg.get("tool_calls")
            if not tool_calls:
                content = (msg.get("content") or "").strip()
                if content:
                    log(content)
                if content == "NEED_MORE":
                    break
                print(content, flush=True)
                return 0

            for tc in tool_calls:
                fn = (tc.get("function") or {}).get("name")
                raw_args = (tc.get("function") or {}).get("arguments") or "{}"
                try:
                    call_args = json.loads(raw_args)
                except Exception:
                    call_args = {}

                if fn == "run_script":
                    log(f"[tool call] run_script args={raw_args}")
                    result = run_script_tool(
                        repo_root,
                        script_name=str(call_args.get("script_name") or ""),
                        args=call_args.get("args") or [],
                        timeout_s=int(call_args.get("timeout_s") or args.timeout),
                    )
                elif fn == "query_csv":
                    log(f"[tool call] query_csv args={raw_args}")
                    result = query_csv_tool(
                        repo_root,
                        path=str(call_args.get("path") or ""),
                        select=call_args.get("select"),
                        filters=call_args.get("filters"),
                        order_by=call_args.get("order_by"),
                        limit=int(call_args.get("limit") or 20),
                    )
                else:
                    result = json.dumps({"ok": False, "error": f"unknown tool: {fn}"}, ensure_ascii=False)

                messages.append({"role": "tool", "tool_call_id": tc.get("id"), "content": result})
                log(summarize_tool_result(str(fn or ""), result))

    print("在 5 轮内未能完成回答。请缩小问题范围或指定要运行的脚本/查询条件。", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
