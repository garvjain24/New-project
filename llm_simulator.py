"""
LLM Simulator — A rule-based engine that mimics an LLM's reasoning and code
generation for known pipeline error types. Returns responses in the exact same
JSON schema as the real OpenRouter LLM, so the rest of the pipeline (agent
routing, code persistence to generated_healers.py, execution) works
identically — with zero API cost and zero latency.

The simulator understands each error type injected by pipeline_utils.py,
reasons about the fix strategy, and emits a Python healing function as a
string, just as a real model would.
"""

import time
from datetime import datetime, timezone


# ── Healing function templates ──────────────────────────────────────────────
# Each value is the raw Python source the simulator "returns" exactly as an
# LLM would inside the `python_code` JSON field.

_HEAL_FUNCTIONS = {

    "duplicate_record": '''\
def heal_duplicate_record(issue, healed, clean_lookup):
    """Remove duplicate rows from the affected dataset, keeping first occurrence."""
    dataset = issue.get("dataset")
    if dataset not in healed:
        return
    unique, seen = [], set()
    for row in healed[dataset]:
        key = tuple(sorted(row.items()))
        if key not in seen:
            unique.append(row)
            seen.add(key)
    healed[dataset] = unique
''',

    "invalid_payment_value": '''\
def heal_invalid_payment_value(issue, healed, clean_lookup):
    """Restore corrupted payment value from the clean store, with word-to-number fallback."""
    words = {"ten thousand": "10000.00", "forty two": "42.00"}
    idx = issue.get("row_index")
    if idx is None or idx >= len(healed.get("payments", [])):
        return
    row = healed["payments"][idx]
    dirty = str(row.get("payment_value", "")).lower()
    if dirty in words:
        row["payment_value"] = words[dirty]
        return
    order_id = row.get("order_id")
    clean_row = clean_lookup.get("payments", {}).get(order_id, {})
    row["payment_value"] = clean_row.get("payment_value", issue.get("original_value", "0.00"))
''',

    "invalid_timestamp": '''\
def heal_invalid_timestamp(issue, healed, clean_lookup):
    """Restore malformed timestamp from clean orders store."""
    idx = issue.get("row_index")
    if idx is None or idx >= len(healed.get("orders", [])):
        return
    row = healed["orders"][idx]
    order_id = row.get("order_id")
    clean_row = clean_lookup.get("orders", {}).get(order_id, {})
    row["order_purchase_timestamp"] = clean_row.get(
        "order_purchase_timestamp", issue.get("original_value")
    )
''',

    "freight_outlier": '''\
def heal_freight_outlier(issue, healed, clean_lookup):
    """Replace unrealistic freight value with the clean store value."""
    idx = issue.get("row_index")
    if idx is None or idx >= len(healed.get("delivery", [])):
        return
    row = healed["delivery"][idx]
    order_id = row.get("order_id")
    clean_row = clean_lookup.get("delivery", {}).get(order_id, {})
    row["freight_value"] = clean_row.get("freight_value", "0.00")
''',

    "unknown_payment_type": '''\
def heal_unknown_payment_type(issue, healed, clean_lookup):
    """Restore unsupported payment type from the clean store."""
    idx = issue.get("row_index")
    if idx is None or idx >= len(healed.get("payments", [])):
        return
    row = healed["payments"][idx]
    order_id = row.get("order_id")
    clean_row = clean_lookup.get("payments", {}).get(order_id, {})
    row["payment_type"] = clean_row.get("payment_type", "voucher")
''',

    "missing_column": '''\
def heal_missing_column(issue, healed, clean_lookup):
    """Reconstruct an entire missing column from clean store values."""
    dataset = issue.get("dataset")
    column = issue.get("column")
    if not dataset or not column or dataset not in healed:
        return
    lookup = clean_lookup.get(dataset, {})
    for row in healed[dataset]:
        order_id = row.get("order_id")
        if order_id and order_id in lookup:
            row[column] = lookup[order_id].get(column, "")
''',

    "orphan_record": '''\
def heal_orphan_record(issue, healed, clean_lookup):
    """Replace orphan row with matching clean record using the original order_id."""
    from copy import deepcopy
    dataset = issue.get("dataset")
    idx = issue.get("row_index")
    order_id = issue.get("original_value") or issue.get("order_id")
    if idx is None or dataset not in healed or idx >= len(healed[dataset]):
        return
    lookup = clean_lookup.get(dataset, {})
    if order_id and order_id in lookup:
        healed[dataset][idx] = deepcopy(lookup[order_id])
''',

    "invalid_order_status": '''\
def heal_invalid_order_status(issue, healed, clean_lookup):
    """Restore nonsensical order status from the clean orders store."""
    idx = issue.get("row_index")
    if idx is None or idx >= len(healed.get("orders", [])):
        return
    row = healed["orders"][idx]
    order_id = row.get("order_id")
    clean_row = clean_lookup.get("orders", {}).get(order_id, {})
    row["order_status"] = clean_row.get("order_status", issue.get("original_value", "delivered"))
''',
}


# ── Reasoning templates ─────────────────────────────────────────────────────
# Human-readable explanations the simulator generates, matching what a real
# model would write in the `reason` field.

_REASONING = {
    "duplicate_record": (
        "The dataset contains an exact duplicate row. Since the row content is "
        "identical, we can safely deduplicate by keeping the first occurrence "
        "and discarding copies. No data is lost because the duplicate adds no "
        "new information. Confidence is high because duplicate detection is "
        "deterministic via content hashing."
    ),
    "invalid_payment_value": (
        "The payment_value field has been corrupted to a non-numeric string. "
        "First, we check against a known word-to-number lookup table (e.g. "
        "'ten thousand' → '10000.00'). If no match, we fall back to the clean "
        "store value using the order_id as a deterministic key. The original "
        "numeric value is fully recoverable from trusted local data."
    ),
    "invalid_timestamp": (
        "The order_purchase_timestamp contains an impossible date format "
        "(e.g. month 99, hour 88). Since the order_id is still intact, we can "
        "deterministically look up the correct timestamp from the clean orders "
        "store. No approximation or inference is needed."
    ),
    "freight_outlier": (
        "The freight_value has been set to an unrealistically high number "
        "(99999.99). The order_id is valid and present in the clean delivery "
        "store, so we can replace it with the original freight value with full "
        "confidence. This is a simple key-based lookup, no estimation required."
    ),
    "unknown_payment_type": (
        "The payment_type has been changed to a value outside the allowed "
        "domain (e.g. 'telepathy'). The order_id maps directly to a clean "
        "record that contains the original valid payment type. Restoring from "
        "trusted data is the safest approach."
    ),
    "missing_column": (
        "An entire column has been dropped from the dataset. Since every row "
        "still has a valid order_id, we can reconstruct the missing column by "
        "looking up each row's value from the clean store. This is a bulk "
        "operation that restores every row deterministically."
    ),
    "orphan_record": (
        "The order_id has been replaced with a fake orphan identifier that "
        "does not exist in the orders table. However, the original order_id "
        "is captured in the error metadata. We use it to look up the correct "
        "full row from the clean store and overwrite the corrupted record."
    ),
    "null_key": (
        "The primary key (order_id) has been blanked out entirely. While the "
        "original value is available in the error trace, restoring a primary "
        "key without human confirmation risks silent data corruption. "
        "Escalating for manual review is the safest route."
    ),
    "invalid_order_status": (
        "The order_status has been changed to a nonsensical value outside the "
        "valid domain. The order_id is intact and the clean orders store has "
        "the original status value. Deterministic restoration is safe."
    ),
}


class LLMSimulator:
    """
    A deterministic, rule-based engine that produces responses in the exact
    same JSON format as the OpenRouter LLM client. It acts as a drop-in
    replacement for the real LLM call inside the agent routing pipeline.

    Priority order in the engine:
        1. Check generated_healers.py (already learned)  →  skip everything
        2. Try LLMSimulator (this class)                 →  zero-cost fix
        3. Fall back to real OpenRouter LLM              →  paid API call
    """

    # Error types the simulator knows how to handle
    SUPPORTED_TYPES = set(_HEAL_FUNCTIONS.keys())

    # Error types the simulator intentionally escalates
    ESCALATE_TYPES = {"null_key"}

    def __init__(self):
        self.calls_total = 0
        self.hits_total = 0
        self.escalations_total = 0

    @property
    def enabled(self):
        return True

    def suggest(self, issue, retrieved_context=None):
        """
        Analyse an issue dict and return a suggestion dict that is
        wire-compatible with what OpenRouterClient.suggest() returns.

        Returns None only if the error_type is completely unknown.
        """
        self.calls_total += 1
        error_type = issue.get("error_type", "")

        # ── Escalate types that need human judgement ─────────────────────
        if error_type in self.ESCALATE_TYPES:
            self.escalations_total += 1
            return {
                "route": "escalate",
                "reason": _REASONING.get(error_type, "Escalated — requires human review."),
                "recommended_action": "escalate_to_human",
                "confidence": 0.3,
                "python_code": "",
                "_source": "llm_simulator",
                "_simulated_at": self._now(),
            }

        # ── Generate healing code for known types ───────────────────────
        if error_type in self.SUPPORTED_TYPES:
            self.hits_total += 1
            return {
                "route": "generate_local",
                "reason": _REASONING.get(error_type, "Deterministic fix from clean store."),
                "recommended_action": f"generate_and_persist_heal_{error_type}",
                "confidence": 0.95,
                "python_code": _HEAL_FUNCTIONS[error_type],
                "_source": "llm_simulator",
                "_simulated_at": self._now(),
            }

        # ── Unknown error type — let real LLM handle it ─────────────────
        return None

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def stats(self):
        """Return a telemetry snapshot for the analysis report."""
        return {
            "simulator_calls_total": self.calls_total,
            "simulator_hits_total": self.hits_total,
            "simulator_escalations_total": self.escalations_total,
        }
