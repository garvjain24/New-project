import math
import re
from collections import Counter

class LocalSimilarityResolver:
    def __init__(self, playbooks, max_history=200):
        self.playbooks = playbooks
        self.max_history = max_history

    def _tokenize(self, text):
        return re.findall(r"[a-z0-9_]+", text.lower())

    def _doc_text(self, item):
        return " ".join(
            str(item.get(key, ""))
            for key in [
                "error_type",
                "dataset",
                "finding",
                "strategy",
                "resolution_action",
                "status",
                "when_to_use",
            ]
        )

    def _vectorize(self, docs):
        tokenized = [self._tokenize(doc) for doc in docs]
        df = Counter()
        for tokens in tokenized:
            for token in set(tokens):
                df[token] += 1
        total = max(len(docs), 1)
        vectors = []
        for tokens in tokenized:
            counts = Counter(tokens)
            vec = {}
            for token, count in counts.items():
                idf = math.log((1 + total) / (1 + df[token])) + 1
                vec[token] = count * idf
            vectors.append(vec)
        return vectors

    def _cosine(self, left, right):
        if not left or not right:
            return 0.0
        common = set(left) & set(right)
        numerator = sum(left[token] * right[token] for token in common)
        left_norm = math.sqrt(sum(value * value for value in left.values()))
        right_norm = math.sqrt(sum(value * value for value in right.values()))
        if not left_norm or not right_norm:
            return 0.0
        return numerator / (left_norm * right_norm)

    def retrieve(self, issue, history_logs):
        corpus = []
        for item in self.playbooks:
            corpus.append(
                {
                    "source": "playbook",
                    "error_type": item["error_type"],
                    "dataset": "",
                    "finding": item["when_to_use"],
                    "strategy": item["strategy"],
                    "status": "template",
                }
            )
        for item in history_logs[-self.max_history :]:
            corpus.append(
                {
                    "source": "history",
                    "error_type": item.get("error_type", ""),
                    "dataset": item.get("dataset", ""),
                    "finding": item.get("finding", ""),
                    "strategy": item.get("resolution_action", ""),
                    "status": item.get("status", ""),
                }
            )
        query = {
            "error_type": issue.get("error_type", ""),
            "dataset": issue.get("dataset", ""),
            "finding": issue.get("finding", ""),
            "strategy": "",
            "status": "",
        }
        docs = [self._doc_text(query)] + [self._doc_text(item) for item in corpus]
        vectors = self._vectorize(docs)
        query_vec = vectors[0]
        scored = []
        for item, vec in zip(corpus, vectors[1:]):
            score = self._cosine(query_vec, vec)
            scored.append({**item, "score": round(score, 4)})
        scored.sort(key=lambda item: item["score"], reverse=True)
        return scored[:3]
