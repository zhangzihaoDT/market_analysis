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

import tempfile
import os


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
                "description": "Query a CSV under ./out or ./data with simple filters and return rows as JSON (path must be a CSV file, not a directory)",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "CSV file path (not directory), absolute or relative to repo"},
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
        "你是一个数据分析助手，拥有 ./data/ 下全部数据的读取权限。\n"
        "\n"
        "数据集（基础表，位于 ./data）：\n"
        "1) 细分市场销量：按区域（parent_region_name）与价格段（TP 5万1档）划分的市场销量。\n"
        "2) 分价格段量价：按品牌-车型构建的单一车型 TP 价格重心数据（TP重心 (数据桶)），用于查看各桶区间有哪些车型及其销量/价格表现。\n"
        "3) 重点关注新能源品牌：重点关注品牌的历史销量表现。\n"
        "\n"
        "可用工具：\n"
        "- run_script：运行 ./scripts 下脚本，通常产出 ./out 下的派生表；stdout 会包含 '输出: <path>'，并可能包含 schema JSON（'输出Schema: <path>'）。\n"
        "- query_csv：对 ./data 或 ./out 下的 CSV 做筛选查询。\n"
        "\n"
        "数据源选择：\n"
        "1) 若是明细核对/字段级问题/明确指向基础表，优先查 ./data。\n"
        "2) 若是指标汇总/同比环比/排名/品牌级 KPI，优先 run_script 生成 ./out，再查 ./out。\n"
        "3) 若两者都需要：先判断派生表是否必要，必要时先 run_script，再用基础表补充。\n"
        "\n"
        "工作流（最多 5 轮循环）：\n"
        "- step1 planning：明确使用基础表还是派生表；要运行哪些脚本；要读哪些 schema；要做哪些 CSV 查询。\n"
        "- step2 执行：调用工具并基于结果回答；若信息不足输出 NEED_MORE，否则输出最终答案。"
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
            "按上一条 planning 执行，但尽量避免多次工具调用：\n"
            "- 优先“写代码一次算清楚”的方式解决问题。直接生成能够读取 ./data 或 ./out 下目标 CSV、完成筛选/聚合并给出答案的 Python 代码。\n"
            "- 如果确需生成派生表，最多执行一次 run_script；随后用代码直接对 CSV 进行计算。\n"
            "- 严禁对目录调用 query_csv；path 必须为 CSV 文件路径。\n"
            "- 返回格式：仅返回一段 Python 代码，使用如下定界符包裹：\n"
            "‹execute_python›\n"
            "<python code here>\n"
            "‹/execute_python›\n"
            "- 代码要求：\n"
            "  1) 只用标准库（csv、json、pathlib 等）；\n"
            "  2) 显式写出目标 CSV 路径与筛选条件；\n"
            "  3) 打印最终答案与关键中间汇总（如各桶/各品牌数值）。\n"
            "若信息仍不足以写出可靠代码，再调用工具；若还需要下一轮，输出 NEED_MORE；否则直接给出最终答案。"
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
                # Execute embedded python code if present
                start_tag = "‹execute_python›"
                end_tag = "‹/execute_python›"
                if start_tag in content and end_tag in content:
                    code_blocks: list[str] = []
                    start_idx = 0
                    while True:
                        s = content.find(start_tag, start_idx)
                        if s == -1:
                            break
                        e = content.find(end_tag, s + len(start_tag))
                        if e == -1:
                            break
                        code = content[s + len(start_tag):e]
                        code_blocks.append(code.strip())
                        start_idx = e + len(end_tag)
                    if code_blocks:
                        code = code_blocks[0]
                        log("[execute_python] detected, running code block")
                        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as tf:
                            tf.write(code)
                            temp_path = tf.name
                        try:
                            proc = subprocess.run(
                                [sys.executable, temp_path],
                                cwd=str(repo_root),
                                capture_output=True,
                                text=True,
                                timeout=args.timeout,
                                check=False,
                                env={**os.environ},
                            )
                            if proc.stdout:
                                print(proc.stdout.rstrip(), flush=True)
                            if proc.stderr:
                                log(proc.stderr.rstrip())
                            return proc.returncode
                        finally:
                            try:
                                os.unlink(temp_path)
                            except Exception:
                                pass
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
