import json
import logging
from .strategy_store import StrategyStore

class PatternExtractor:
    def __init__(self, store: StrategyStore, chat_func, chat_kwargs: dict):
        self.store = store
        self.chat_func = chat_func
        self.chat_kwargs = chat_kwargs

    def summarize(self):
        failed_tasks = self.store.get_failed_logs(limit=20)
        if not failed_tasks:
            return

        tasks_text = ""
        for i, task in enumerate(failed_tasks):
            tasks_text += (
                f"{i+1}. Query: {task.get('query')}\n"
                f"   SQL/Code: {task.get('sql')}\n"
                f"   Error: {task.get('error')}\n"
                f"   Result Rows: {task.get('result_rows')}\n\n"
            )

        prompt = f"""
以下是数据分析Agent近期的多个执行失败或次优的查询案例：
{tasks_text}

请分析这些案例，总结出系统性的经验规律，包括：
- pattern (问题模式/场景)
- strategy (推荐策略，例如如何改进SQL、优先用哪些字段)
- anti_pattern (需要避免的错误做法)

请输出严格的 JSON 数组格式，不要包含其他无关内容。
例如：
[
  {{
    "pattern": "时间筛选错误",
    "strategy": "统一使用order_month字段",
    "anti_pattern": "使用order_date做模糊筛选"
  }}
]
"""
        messages = [{"role": "user", "content": prompt}]
        try:
            resp = self.chat_func(messages=messages, **self.chat_kwargs)
            choice = (resp.get("choices") or [{}])[0]
            content = (choice.get("message") or {}).get("content", "")

            # 提取 JSON 块
            start = content.find("[")
            end = content.rfind("]") + 1
            if start != -1 and end > start:
                patterns = json.loads(content[start:end])
                for p in patterns:
                    self.store.upsert_pattern(
                        p.get("pattern", ""),
                        p.get("strategy", ""),
                        p.get("anti_pattern", "")
                    )
        except Exception as e:
            logging.error(f"Pattern extraction failed: {e}")
