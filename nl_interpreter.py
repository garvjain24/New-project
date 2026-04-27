"""
Natural Language Fix Interpreter — Parses human instructions written in
plain English and maps them to concrete data repair actions. Simulates
an LLM "reasoning" pipeline: intent extraction → strategy selection →
action execution, all locally with zero API cost.

Returns a structured response that mimics the chain-of-thought a real
LLM would produce, complete with reasoning steps and a final action.
"""

import re
import time
import random
from datetime import datetime, timezone


# ── Intent patterns ─────────────────────────────────────────────────────────
# Each pattern maps a regex (or set of keywords) to a repair intent.

_INTENT_PATTERNS = [
    {
        "intent": "restore_from_clean",
        "keywords": [
            "restore", "original", "clean", "reset", "revert",
            "correct", "fix from clean", "use clean", "use original",
            "get from clean", "pull from clean", "recover",
        ],
        "description": "Restore value from the clean/original data store",
    },
    {
        "intent": "set_value",
        "keywords": [
            "set to", "change to", "update to", "make it",
            "replace with", "should be", "put", "assign",
            "the correct value is"
        ],
        "description": "Set a specific value provided by the user",
    },
    {
        "intent": "delete_row",
        "keywords": [
            "delete", "remove", "drop", "discard", "get rid of",
        ],
        "description": "Remove the problematic row entirely",
    },
    {
        "intent": "deduplicate",
        "keywords": [
            "deduplicate", "dedup", "remove duplicate", "remove copy",
            "keep one", "keep first",
        ],
        "description": "Remove duplicate entries",
    },
    {
        "intent": "escalate",
        "keywords": [
            "skip", "ignore", "leave it", "don't fix", "not sure",
            "i don't know", "escalate", "pass",
        ],
        "description": "Skip this fix / leave escalated",
    },
]


def _extract_quoted_value(text):
    """Pull a value from quotes, e.g. 'set to "delivered"' → 'delivered'"""
    match = re.search(r'["\']([^"\']+)["\']', text)
    if match:
        return match.group(1)
    # Try after specific prefixes
    prefixes = [
        "the correct value is ", "set to ", "change to ", 
        "update to ", "replace with ", "should be ", "make it "
    ]
    for prefix in prefixes:
        idx = text.lower().find(prefix)
        if idx >= 0:
            candidate = text[idx + len(prefix):].strip().rstrip(".")
            if candidate:
                return candidate
    return None


def _match_intent(text):
    """Score each intent pattern against the input text."""
    text_lower = text.lower().strip()
    best_intent = None
    best_score = 0

    for pattern in _INTENT_PATTERNS:
        score = 0
        for keyword in pattern["keywords"]:
            if keyword in text_lower:
                score += len(keyword)  # longer matches = more specific
        if score > best_score:
            best_score = score
            best_intent = pattern

    return best_intent, best_score


class NLFixInterpreter:
    """
    Interprets natural language fix instructions and produces a structured
    action plan that looks like an LLM chain-of-thought response.
    """

    def __init__(self):
        self.interpretations_total = 0

    def interpret(self, instruction, approval_context):
        """
        Parse a natural language instruction and return a simulated
        LLM response with reasoning steps and a concrete action.

        Args:
            instruction: The user's natural language fix instruction
            approval_context: Dict with error_id, dataset, column, 
                              current_value, proposed_value, order_id, etc.

        Returns:
            Dict with reasoning_steps, final_action, resolved_value,
            confidence, and timing metadata.
        """
        self.interpretations_total += 1
        start_time = time.time()

        # ── Step 1: Parse intent ────────────────────────────────────────
        intent_match, score = _match_intent(instruction)
        extracted_value = _extract_quoted_value(instruction)

        intent = intent_match["intent"] if intent_match else "restore_from_clean"
        intent_desc = intent_match["description"] if intent_match else "Default: restore from clean"

        # ── Step 2: Build reasoning chain ───────────────────────────────
        reasoning_steps = []

        reasoning_steps.append({
            "step": 1,
            "phase": "Intent Recognition",
            "thought": f"Analyzing user instruction: \"{instruction}\"",
            "result": f"Detected intent: {intent} ({intent_desc})",
            "duration_ms": random.randint(120, 350),
        })

        reasoning_steps.append({
            "step": 2,
            "phase": "Context Analysis",
            "thought": (
                f"Error context — dataset: {approval_context.get('dataset', '?')}, "
                f"column: {approval_context.get('column', '?')}, "
                f"current (dirty) value: {approval_context.get('current_value', '?')}, "
                f"proposed (clean) value: {approval_context.get('proposed_value', '?')}"
            ),
            "result": "Context loaded. Evaluating repair strategy.",
            "duration_ms": random.randint(80, 200),
        })

        # ── Step 3: Determine action ────────────────────────────────────
        resolved_value = None
        action_description = ""
        confidence = 0.0

        if intent == "restore_from_clean":
            resolved_value = approval_context.get("proposed_value") or approval_context.get("order_id")
            action_description = (
                f"Restoring the original clean value '{resolved_value}' from the trusted data store. "
                f"This overwrites the corrupted value '{approval_context.get('current_value', '?')}'."
            )
            confidence = 0.95
            reasoning_steps.append({
                "step": 3,
                "phase": "Strategy Selection",
                "thought": (
                    "User wants to restore from clean data. The proposed_value from the "
                    "clean store is available and trustworthy."
                ),
                "result": f"Will restore value to: {resolved_value}",
                "duration_ms": random.randint(100, 250),
            })

        elif intent == "set_value":
            if extracted_value:
                resolved_value = extracted_value
                action_description = (
                    f"Setting field to user-specified value '{extracted_value}'. "
                    f"Previous corrupted value was '{approval_context.get('current_value', '?')}'."
                )
                confidence = 0.90
                reasoning_steps.append({
                    "step": 3,
                    "phase": "Value Extraction",
                    "thought": f"User specified a concrete value: \"{extracted_value}\"",
                    "result": f"Will set field to: {extracted_value}",
                    "duration_ms": random.randint(90, 200),
                })
            else:
                # Could not extract a value — fall back to clean
                resolved_value = approval_context.get("proposed_value") or approval_context.get("order_id")
                action_description = (
                    f"Could not extract a specific value from instruction. "
                    f"Falling back to clean store value: '{resolved_value}'."
                )
                confidence = 0.70
                reasoning_steps.append({
                    "step": 3,
                    "phase": "Value Extraction",
                    "thought": "No explicit value found in instruction. Falling back to clean data.",
                    "result": f"Will use clean store value: {resolved_value}",
                    "duration_ms": random.randint(150, 300),
                })

        elif intent == "delete_row":
            resolved_value = "__DELETE__"
            action_description = (
                "User requested row deletion. Marking row for removal."
            )
            confidence = 0.85
            reasoning_steps.append({
                "step": 3,
                "phase": "Strategy Selection",
                "thought": "User explicitly wants this row removed from the dataset.",
                "result": "Row will be marked for deletion.",
                "duration_ms": random.randint(80, 180),
            })

        elif intent == "deduplicate":
            resolved_value = approval_context.get("proposed_value") or approval_context.get("order_id")
            action_description = (
                "User wants deduplication. Keeping first occurrence and restoring to clean state."
            )
            confidence = 0.90
            reasoning_steps.append({
                "step": 3,
                "phase": "Strategy Selection",
                "thought": "Deduplication requested. Will restore the original unique row.",
                "result": f"Will restore to clean value: {resolved_value}",
                "duration_ms": random.randint(100, 220),
            })

        elif intent == "escalate":
            resolved_value = None
            action_description = "User chose to skip this fix. Error remains escalated."
            confidence = 1.0
            reasoning_steps.append({
                "step": 3,
                "phase": "Strategy Selection",
                "thought": "User does not want to fix this issue right now.",
                "result": "No action taken. Error stays in escalated state.",
                "duration_ms": random.randint(50, 120),
            })

        # ── Step 4: Confidence assessment ───────────────────────────────
        reasoning_steps.append({
            "step": 4,
            "phase": "Confidence Assessment",
            "thought": (
                f"Evaluated repair strategy against error context. "
                f"Intent confidence: {confidence:.0%}."
            ),
            "result": (
                f"Final action: {action_description}" 
                if intent != "escalate" 
                else "No repair action — user chose to skip."
            ),
            "duration_ms": random.randint(60, 150),
        })

        elapsed_ms = int((time.time() - start_time) * 1000)
        total_simulated_ms = sum(step["duration_ms"] for step in reasoning_steps)

        return {
            "instruction": instruction,
            "detected_intent": intent,
            "intent_description": intent_desc,
            "reasoning_steps": reasoning_steps,
            "resolved_value": resolved_value,
            "action_description": action_description,
            "confidence": confidence,
            "skip": intent == "escalate",
            "elapsed_ms": elapsed_ms,
            "simulated_thinking_ms": total_simulated_ms,
            "_source": "nl_fix_interpreter",
            "_interpreted_at": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        }
