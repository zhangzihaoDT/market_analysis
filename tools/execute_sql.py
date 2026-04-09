import json
from pathlib import Path

def execute_sql_tool(repo_root: Path, *, sql: str, timeout_s: int = 30) -> str:
    """Execute SQL using DuckDB directly against CSV files."""
    try:
        import duckdb
    except ImportError:
        return json.dumps({"ok": False, "error": "duckdb module is not installed. Please run: pip install duckdb"}, ensure_ascii=False)

    try:
        # Create an in-memory duckdb connection
        con = duckdb.connect(":memory:")
        # Execute the query
        result = con.execute(sql).fetchall()
        # Get column names
        columns = [desc[0] for desc in con.description] if con.description else []
        
        # Convert result to list of dicts for JSON serialization
        rows = [dict(zip(columns, row)) for row in result]
        
        return json.dumps({
            "ok": True,
            "columns": columns,
            "rows_returned": len(rows),
            "rows": rows
        }, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False, default=str)