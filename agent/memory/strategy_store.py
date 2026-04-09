import duckdb
from pathlib import Path
import datetime
import threading

class StrategyStore:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path: str = "strategy_memory.db"):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(StrategyStore, cls).__new__(cls)
                cls._instance._init_instance(db_path)
            return cls._instance

    def _init_instance(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with duckdb.connect(self.db_path) as con:
            con.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_strategy_memory;
            """)
            con.execute("""
                CREATE SEQUENCE IF NOT EXISTS seq_agent_logs;
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS strategy_memory (
                    id INTEGER DEFAULT nextval('seq_strategy_memory'),
                    pattern TEXT,
                    strategy TEXT,
                    anti_pattern TEXT,
                    success_rate FLOAT,
                    use_count INTEGER,
                    updated_at TIMESTAMP
                );
            """)
            con.execute("""
                CREATE TABLE IF NOT EXISTS agent_logs (
                    id INTEGER DEFAULT nextval('seq_agent_logs'),
                    query TEXT,
                    sql TEXT,
                    success BOOLEAN,
                    latency FLOAT,
                    result_rows INTEGER,
                    error TEXT,
                    created_at TIMESTAMP
                );
            """)

    def log_execution(self, query: str, sql: str, success: bool, latency: float, result_rows: int, error: str):
        with duckdb.connect(self.db_path) as con:
            con.execute("""
                INSERT INTO agent_logs (query, sql, success, latency, result_rows, error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (query, sql, success, latency, result_rows, error, datetime.datetime.now()))

    def upsert_pattern(self, pattern: str, strategy: str, anti_pattern: str, success_rate: float = 1.0):
        with duckdb.connect(self.db_path) as con:
            result = con.execute("SELECT id, use_count FROM strategy_memory WHERE pattern = ?", (pattern,)).fetchone()
            if result:
                pattern_id, use_count = result
                con.execute("""
                    UPDATE strategy_memory 
                    SET strategy = ?, anti_pattern = ?, success_rate = ?, use_count = ?, updated_at = ?
                    WHERE id = ?
                """, (strategy, anti_pattern, success_rate, use_count + 1, datetime.datetime.now(), pattern_id))
            else:
                con.execute("""
                    INSERT INTO strategy_memory (pattern, strategy, anti_pattern, success_rate, use_count, updated_at)
                    VALUES (?, ?, ?, ?, 1, ?)
                """, (pattern, strategy, anti_pattern, success_rate, datetime.datetime.now()))

    def get_all_patterns(self, limit: int = 10):
        with duckdb.connect(self.db_path) as con:
            rows = con.execute("SELECT pattern, strategy, anti_pattern FROM strategy_memory ORDER BY use_count DESC LIMIT ?", (limit,)).fetchall()
            return [{"pattern": r[0], "strategy": r[1], "anti_pattern": r[2]} for r in rows]

    def get_failed_logs(self, limit: int = 100):
        with duckdb.connect(self.db_path) as con:
            rows = con.execute("""
                SELECT query, sql, error, result_rows 
                FROM agent_logs 
                WHERE success = FALSE 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (limit,)).fetchall()
            return [{"query": r[0], "sql": r[1], "error": r[2], "result_rows": r[3]} for r in rows]
