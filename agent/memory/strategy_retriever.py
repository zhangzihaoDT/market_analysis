from .strategy_store import StrategyStore

class StrategyRetriever:
    def __init__(self, store: StrategyStore):
        self.store = store

    def match(self, query: str) -> list[dict]:
        # Minimal implementation: return top patterns from store.
        # A more advanced version would use embeddings or BM25 to match `query` against `pattern`.
        return self.store.get_all_patterns(limit=5)
