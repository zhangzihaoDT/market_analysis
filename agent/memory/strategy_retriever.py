import jieba
from .strategy_store import StrategyStore

class StrategyRetriever:
    def __init__(self, store: StrategyStore):
        self.store = store

    def match(self, query: str, limit: int = 3) -> list[dict]:
        """
        基于简单的关键词匹配（Jaccard 相似度思想）检索最相关的策略。
        """
        # 获取所有策略
        all_patterns = self.store.get_all_patterns(limit=50)
        if not all_patterns:
            return []

        # 对用户查询进行分词
        query_words = set(jieba.lcut(query))

        scored_patterns = []
        for p in all_patterns:
            # 对 pattern 描述进行分词
            pattern_desc = p.get("pattern", "")
            pattern_words = set(jieba.lcut(pattern_desc))
            
            # 计算交集（重合词的数量）
            intersection = query_words.intersection(pattern_words)
            
            # 简单的打分：重合词数量越多，分数越高。也可以除以并集计算 Jaccard 相似度
            score = len(intersection)
            
            # 即使没有完全匹配，也保留基础的分数（比如依靠使用次数排序）
            scored_patterns.append({
                "score": score,
                "pattern": p
            })

        # 按分数降序排列，分数相同则保持原有顺序（按 use_count）
        scored_patterns.sort(key=lambda x: x["score"], reverse=True)

        # 只返回前 N 个最相关的，且仅当有一定相关性时（比如 score > 0）
        # 这里为了保证总能兜底，只要有就返回前 limit 个
        top_patterns = [x["pattern"] for x in scored_patterns[:limit]]
        return top_patterns
