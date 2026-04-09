import argparse
import csv
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request
import time
from pathlib import Path

from tools.execute_sql import execute_sql_tool
from agent.memory.strategy_store import StrategyStore
from agent.memory.strategy_retriever import StrategyRetriever
from agent.memory.pattern_extractor import PatternExtractor
from agent.evaluator.evaluator import evaluate

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


def execute_python_code_tool(repo_root: Path, *, code: str, timeout_s: int) -> str:
    """Execute Python code in a temporary file and return stdout/stderr."""
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8") as tf:
        tf.write(code)
        temp_path = tf.name
    try:
        proc = subprocess.run(
            [sys.executable, temp_path],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            timeout=timeout_s,
            check=False,
            env={**os.environ},
        )
        return json.dumps(
            {
                "ok": proc.returncode == 0,
                "returncode": proc.returncode,
                "stdout": proc.stdout.strip(),
                "stderr": proc.stderr.strip(),
            },
            ensure_ascii=False,
        )
    except subprocess.TimeoutExpired:
        return json.dumps({"ok": False, "error": f"Execution timed out after {timeout_s}s"}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False)
    finally:
        try:
            os.unlink(temp_path)
        except Exception:
            pass


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

    if tool_name == "execute_sql":
        return (
            f"[tool:execute_sql] ok={data.get('ok')} "
            f"rows_returned={data.get('rows_returned')}\n"
            f"columns={data.get('columns')}"
        )

    if tool_name == "execute_python_code":
        stdout = (data.get("stdout") or "").strip()
        stderr = (data.get("stderr") or "").strip()
        out_first = "\n".join(stdout.splitlines()[:10])
        err_first = "\n".join(stderr.splitlines()[:10])
        return (
            f"[tool:execute_python_code] ok={data.get('ok')} returncode={data.get('returncode')}\n"
            f"stdout(first 10 lines):\n{out_first}\n"
            f"stderr(first 10 lines):\n{err_first}"
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
    thinking: bool = False,
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


def build_dynamic_schema_prompt(repo_root: Path) -> str:
    schema_info = []
    for dir_name in ["data", "out"]:
        dir_path = repo_root / dir_name
        if not dir_path.exists():
            continue
        
        for schema_file in sorted(dir_path.glob("*.schema.json")):
            try:
                with open(schema_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    file_name = data.get("file", schema_file.name.replace(".schema.json", ".csv"))
                    description = data.get("description", "")
                    fields = data.get("fields", [])
                    field_names = [f["name"] for f in fields]
                    
                    desc_str = f"（{description}）" if description else ""
                    schema_info.append(f"- ./{dir_name}/{file_name}{desc_str}：包含字段 {', '.join(field_names)}")
            except Exception:
                pass
                
    if not schema_info:
        return "（未扫描到任何数据表的 schema.json 文件）"
    return "\n".join(schema_info)


def main(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parent
    dotenv = load_dotenv(repo_root / ".env")
    api_key = dotenv.get("DEEPSEEK_API_KEY") or os.environ.get("DEEPSEEK_API_KEY") or ""
    base_url = dotenv.get("DEEPSEEK_BASE_URL") or os.environ.get("DEEPSEEK_BASE_URL") or "https://api.deepseek.com"

    parser = argparse.ArgumentParser()
    parser.add_argument("--query", default="")
    parser.add_argument("--model", default="deepseek-chat")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--learn", action="store_true", help="Trigger PatternExtractor to summarize past failed queries")
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
                "name": "execute_sql",
                "description": "Execute DuckDB SQL query directly against CSV files. e.g. SELECT * FROM 'data/xxx.csv'",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "DuckDB compatible SQL query. Enclose file paths in single quotes.",
                        },
                        "timeout_s": {"type": "integer", "description": "Timeout seconds for query execution"},
                    },
                    "required": ["sql"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "execute_python_code",
                "description": "Execute python code and return stdout/stderr. Use this for complex data processing or computation.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string", "description": "Python code to execute. Can import standard libraries, duckdb, pandas, etc."},
                        "timeout_s": {"type": "integer", "description": "Timeout seconds for running the code. Default is 30.", "default": 30},
                    },
                    "required": ["code"],
                },
            },
        }
    ]

    if getattr(args, "learn", False):
        store = StrategyStore(str(repo_root / "strategy_memory.db"))
        extractor = PatternExtractor(
            store, 
            deepseek_chat, 
            {"base_url": base_url, "api_key": api_key, "model": args.model, "timeout_s": args.timeout, "thinking": False, "tools": None}
        )
        extractor.summarize()
        print("Learning from past mistakes completed.", flush=True)
        return 0

    query = args.query.strip()
    if not query:
        query = sys.stdin.read().strip()

    store = StrategyStore(str(repo_root / "strategy_memory.db"))
    retriever = StrategyRetriever(store)
    patterns = retriever.match(query)
    
    if patterns:
        patterns_str = "历史经验（高相关）：\n"
        for p in patterns:
            patterns_str += f"- 问题模式: {p.get('pattern')}\n  推荐策略: {p.get('strategy')}\n  避免做法: {p.get('anti_pattern')}\n\n"
        print(f"已注入 {len(patterns)} 条历史经验策略。", file=sys.stderr)
    else:
        patterns_str = "暂无历史经验。\n"

    dynamic_schemas_str = build_dynamic_schema_prompt(repo_root)

    system_prompt = (
        "你是一个数据分析助手，拥有 ./data/ 和 ./out/ 下全部数据的读取权限。\n"
        "\n"
        "数据集（动态感知，位于 ./data 和 ./out）：\n"
        f"{dynamic_schemas_str}\n"
        "\n"
        f"{patterns_str}"
        "可用工具：\n"
        "- run_script：运行 ./scripts 下脚本，通常产出 ./out 下的派生表；stdout 会包含 '输出: <path>'，并可能包含 schema JSON（'输出Schema: <path>'）。\n"
        "- execute_sql：使用 DuckDB 直接针对 ./data 或 ./out 下的 CSV 执行 SQL 查询（例如 `SELECT * FROM 'data/xxx.csv' WHERE ... GROUP BY ...`）。\n"
        "- execute_python_code：执行 Python 代码，用于复杂计算、多表处理等，你可以直接在代码里 import duckdb, pandas 等处理 CSV 数据。\n"
        "\n"
        "执行原则与工作流：\n"
        "1. 优先思考并设计解决问题的逻辑。\n"
        "2. 优先使用 execute_sql 或者 execute_python_code 直接从基础表获取答案（DuckDB 性能最好）。\n"
        "3. 调用工具后，系统会将执行结果（如 stdout、错误信息）作为 Tool Response 返回给你。\n"
        "4. 你需要基于执行结果继续思考。如果报错，请分析错误信息，修改 SQL 或 Python 代码后重新调用工具。\n"
        "5. **强制终止指令**：当你通过工具获得了能够直接、准确回答用户核心问题的关键数据时，**必须立即停止任何进一步的数据探索（如查区域、查同比、查其他不相关价格段等）**，直接输出最终的纯文本自然语言回答，不再调用任何工具。\n"
    )

    messages: list[dict] = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": query},
    ]

    max_rounds = 10
    for round_idx in range(max_rounds):
        log(f"=== Loop Round {round_idx + 1}/{max_rounds} ===")
        
        resp = deepseek_chat(
            base_url=base_url,
            api_key=api_key,
            model=args.model,
            messages=messages,
            tools=tool_spec,
            timeout_s=args.timeout,
            thinking=False,
        )
        
        choice = (resp.get("choices") or [{}])[0]
        msg = choice.get("message") or {}
        messages.append(msg)
        
        tool_calls = msg.get("tool_calls")
        content = (msg.get("content") or "").strip()
        
        if not tool_calls:
            print("\n最终答案：", flush=True)
            if content:
                print(content, flush=True)
            return 0
            
        if content:
            log(content)
            
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
            elif fn == "execute_sql":
                log(f"[tool call] execute_sql args={raw_args}")
                start_time = time.time()
                sql_str = str(call_args.get("sql") or "")
                result = execute_sql_tool(
                    repo_root,
                    sql=sql_str,
                    timeout_s=int(call_args.get("timeout_s") or args.timeout),
                )
                latency = time.time() - start_time
                try:
                    res_json = json.loads(result)
                    rows = res_json.get("rows_returned", 0)
                    err = res_json.get("error", "")
                    eval_res = evaluate({
                        "query": query,
                        "sql": sql_str,
                        "result_rows": rows,
                        "error": err
                    })
                    
                    store.log_execution(
                        query=query,
                        sql=sql_str,
                        success=eval_res["success"],
                        latency=latency,
                        result_rows=rows,
                        error=err or ", ".join(eval_res.get("issues", []))
                    )
                    if not eval_res["success"]:
                        log(f"[evaluator] Issues detected: {eval_res['issues']}")
                except Exception:
                    pass
            elif fn == "execute_python_code":
                log(f"[tool call] execute_python_code")
                result = execute_python_code_tool(
                    repo_root,
                    code=str(call_args.get("code") or ""),
                    timeout_s=int(call_args.get("timeout_s") or 30),
                )
            else:
                result = json.dumps({"ok": False, "error": f"unknown tool: {fn}"}, ensure_ascii=False)

            messages.append({"role": "tool", "tool_call_id": tc.get("id"), "content": result})
            log(summarize_tool_result(str(fn or ""), result))

    print(f"在 {max_rounds} 轮内未能完成回答，可能任务过于复杂或模型陷入死循环。", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
