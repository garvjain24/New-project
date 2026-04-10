import csv
import io
import json
import math
import os
import random
import re
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from pipeline_utils import (
    REQUIRED_COLUMNS,
    generate_dirty_dataset,
    load_dataset,
    write_dataset,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WEB_DIR = BASE_DIR / "web"
CLEAN_DIR = DATA_DIR / "clean"
DIRTY_DIR = DATA_DIR / "dirty"
LOGS_DIR = DATA_DIR / "logs"
RUNS_DIR = DATA_DIR / "runs"
CYCLE_SECONDS = 15
MAX_EVENTS = 80
MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 15

LOCAL_PLAYBOOKS = [
    {
        "error_type": "duplicate_record",
        "strategy": "deduplicate keeping first unique row",
        "when_to_use": "row content is duplicated and primary keys still allow safe collapse",
    },
    {
        "error_type": "invalid_payment_value",
        "strategy": "normalize text to numeric if obvious else restore from trusted local store",
        "when_to_use": "payment_value is malformed or non-numeric",
    },
    {
        "error_type": "invalid_timestamp",
        "strategy": "restore timestamp from trusted local store",
        "when_to_use": "timestamp format is invalid",
    },
    {
        "error_type": "freight_outlier",
        "strategy": "replace with trusted freight value from local store",
        "when_to_use": "freight is unrealistic but order_id is available",
    },
    {
        "error_type": "unknown_payment_type",
        "strategy": "restore supported payment type from trusted local store",
        "when_to_use": "payment type is unsupported but order_id is available",
    },
    {
        "error_type": "missing_column",
        "strategy": "reconstruct missing field from trusted local store",
        "when_to_use": "column is recoverable from local canonical data",
    },
    {
        "error_type": "null_key",
        "strategy": "require human review unless a deterministic local trace exists",
        "when_to_use": "primary key is blank or ambiguous",
    },
    {
        "error_type": "orphan_record",
        "strategy": "restore from trusted local store if confident else escalate",
        "when_to_use": "foreign key no longer matches a clean order",
    },
]


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


class OpenRouterClient:
    def __init__(self):
        self.api_key = os.environ.get("OPENROUTER_API_KEY", "")
        self.model = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")
        self.site_url = os.environ.get("OPENROUTER_SITE_URL", "http://localhost")
        self.site_name = os.environ.get("OPENROUTER_APP_NAME", "Self-Healing Agentic Data Pipeline")

    @property
    def enabled(self):
        return bool(self.api_key)

    def suggest(self, issue, retrieved_context):
        if not self.enabled:
            return None
        prompt = {
            "task": "Suggest a safe data-healing action for this pipeline issue.",
            "rules": [
                "Prefer local trusted data over invention.",
                "If the issue can be fixed deterministically from local trusted data, say local_only.",
                "If confidence is low or primary key is ambiguous, say escalate.",
                "Return compact JSON only.",
            ],
            "issue": {
                "error_type": issue.get("error_type"),
                "dataset": issue.get("dataset"),
                "finding": issue.get("finding"),
                "dirty_value": issue.get("dirty_value"),
                "original_value": issue.get("original_value"),
                "order_id": issue.get("order_id"),
            },
            "retrieved_context": retrieved_context,
            "output_schema": {
                "route": "local_only | llm_guided_local | escalate",
                "reason": "short string",
                "recommended_action": "short string",
                "confidence": "0-1 float",
            },
        }
        request = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=json.dumps(
                {
                    "model": self.model,
                    "temperature": 0,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": "You are a careful data reliability agent."},
                        {"role": "user", "content": json.dumps(prompt)},
                    ],
                }
            ).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": self.site_url,
                "X-OpenRouter-Title": self.site_name,
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            return None
        try:
            content = payload["choices"][0]["message"]["content"]
            return json.loads(content)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError):
            return None


class SimulationEngine:
    def __init__(self):
        self.lock = threading.Lock()
        self.pending_approvals = []
        self.all_escalations = []
        self.local_resolver = LocalSimilarityResolver(LOCAL_PLAYBOOKS)
        self.openrouter = OpenRouterClient()
        self.started_at = time.time()
        self.last_crash_at = None
        self.telemetry = {
            "llm_calls_total": 0,
            "llm_success_total": 0,
            "local_similarity_route_total": 0,
            "local_fallback_route_total": 0,
            "llm_guided_route_total": 0,
            "local_rule_resolved_total": 0,
            "llm_assisted_resolved_total": 0,
            "human_approval_requests_total": 0,
            "human_resolution_requests_total": 0,
            "human_approved_total": 0,
            "human_rejected_total": 0,
            "manual_fix_total": 0,
            "successful_batches_total": 0,
            "failed_batches_total": 0,
            "report_download_total": 0,
        }
        self.state = {
            "running": True,
            "cycle_seconds": CYCLE_SECONDS,
            "last_run_at": None,
            "next_run_at": time.time() + 2,
            "cycle_count": 0,
            "event_feed": [],
            "history": [],
            "current_cycle": None,
            "metrics": {
                "total_batches": 0,
                "detected_issues": 0,
                "auto_resolved": 0,
                "human_escalations": 0,
            },
        }

    def _mean(self, values):
        return round(sum(values) / len(values), 2) if values else 0.0

    def _pct(self, value, total):
        return round((value / total) * 100, 2) if total else 0.0

    def _report_paths(self):
        return {
            "json": LOGS_DIR / "latest_analysis_report.json",
            "md": LOGS_DIR / "latest_analysis_report.md",
        }

    def _historical_logs(self):
        logs = []
        for batch in self.state.get("history", []):
            logs.extend(batch.get("detailed_logs", []))
        return logs

    def build_analysis_report(self):
        recent_batches = self.state.get("history", [])
        recent_logs = self._historical_logs()
        route_counter = Counter(log.get("agent_route", "unknown") for log in recent_logs)
        error_counter = Counter(log.get("error_type", "unknown") for log in recent_logs)
        status_counter = Counter(log.get("status", "unknown") for log in recent_logs)
        dataset_counter = Counter(log.get("dataset", "unknown") for log in recent_logs)
        now = time.time()
        successful = self.telemetry["successful_batches_total"]
        failed = self.telemetry["failed_batches_total"]
        total_runs = successful + failed
        open_escalations = [item for item in self.all_escalations if item.get("status") == "pending"]
        report = {
            "generated_at": self._now_text(),
            "system": {
                "server_started_at": datetime.fromtimestamp(self.started_at, timezone.utc)
                .astimezone()
                .strftime("%Y-%m-%d %H:%M:%S"),
                "uptime_seconds": round(now - self.started_at, 2),
                "crash_free_uptime_seconds": round(now - (self.last_crash_at or self.started_at), 2),
                "crash_count": failed,
                "successful_batches_total": successful,
                "failed_batches_total": failed,
                "batch_success_rate_pct": self._pct(successful, total_runs),
            },
            "agent_metrics": {
                "openrouter_enabled": self.openrouter.enabled,
                "openrouter_model": self.openrouter.model,
                "llm_calls_total": self.telemetry["llm_calls_total"],
                "llm_success_total": self.telemetry["llm_success_total"],
                "local_similarity_route_total": self.telemetry["local_similarity_route_total"],
                "local_fallback_route_total": self.telemetry["local_fallback_route_total"],
                "llm_guided_route_total": self.telemetry["llm_guided_route_total"],
                "local_rule_resolved_total": self.telemetry["local_rule_resolved_total"],
                "llm_assisted_resolved_total": self.telemetry["llm_assisted_resolved_total"],
            },
            "human_metrics": {
                "human_approval_requests_total": self.telemetry["human_approval_requests_total"],
                "human_resolution_requests_total": self.telemetry["human_resolution_requests_total"],
                "human_approved_total": self.telemetry["human_approved_total"],
                "human_rejected_total": self.telemetry["human_rejected_total"],
                "manual_fix_total": self.telemetry["manual_fix_total"],
                "open_human_queue_total": len(open_escalations),
            },
            "performance_metrics": {
                "mttd_seconds": self._mean(
                    [log.get("detection_latency_sec", 0) for log in recent_logs if log.get("detection_latency_sec") is not None]
                ),
                "mttr_seconds": self._mean(
                    [log.get("resolution_latency_sec", 0) for log in recent_logs if log.get("resolution_latency_sec") is not None]
                ),
                "issues_detected_total": self.state["metrics"]["detected_issues"],
                "issues_resolved_total": self.state["metrics"]["auto_resolved"],
                "issues_escalated_total": self.state["metrics"]["human_escalations"],
            },
            "recent_breakdowns": {
                "routes": dict(route_counter),
                "error_types": dict(error_counter),
                "statuses": dict(status_counter),
                "datasets": dict(dataset_counter),
            },
            "recent_batches": [
                {
                    "batch_id": batch.get("batch_id"),
                    "started_at": batch.get("started_at"),
                    "batch_size": batch.get("batch_size"),
                    "status": batch.get("status"),
                    "detected": batch.get("summary", {}).get("detected", 0),
                    "resolved": batch.get("summary", {}).get("resolved", 0),
                    "escalated": batch.get("summary", {}).get("escalated", 0),
                }
                for batch in recent_batches[:12]
            ],
            "open_escalations": [
                {
                    "error_id": item.get("error_id"),
                    "batch_id": item.get("batch_id"),
                    "dataset": item.get("dataset"),
                    "status": item.get("status"),
                    "reason": item.get("reason"),
                    "proposed_value": item.get("proposed_value"),
                }
                for item in open_escalations[:50]
            ],
            "downloads": {
                "report_download_total": self.telemetry["report_download_total"],
                "json_path": str(self._report_paths()["json"]),
                "markdown_path": str(self._report_paths()["md"]),
            },
        }
        return report

    def _analysis_markdown(self, report):
        recent_batches_lines = "\n".join(
            f"- `{item['batch_id']}` | status={item['status']} | size={item['batch_size']} | detected={item['detected']} | resolved={item['resolved']} | escalated={item['escalated']}"
            for item in report["recent_batches"]
        ) or "- None"
        open_escalation_lines = "\n".join(
            f"- `{item['error_id']}` | batch={item['batch_id']} | dataset={item['dataset']} | status={item['status']} | reason={item['reason']}"
            for item in report["open_escalations"]
        ) or "- None"
        return f"""# Pipeline Analysis Report

Generated at: {report['generated_at']}

## System

- Uptime seconds: {report['system']['uptime_seconds']}
- Crash-free uptime seconds: {report['system']['crash_free_uptime_seconds']}
- Crash count: {report['system']['crash_count']}
- Successful batches: {report['system']['successful_batches_total']}
- Failed batches: {report['system']['failed_batches_total']}
- Batch success rate: {report['system']['batch_success_rate_pct']}%

## Agent Metrics

- LLM calls: {report['agent_metrics']['llm_calls_total']}
- LLM successful suggestions: {report['agent_metrics']['llm_success_total']}
- Local similarity routes: {report['agent_metrics']['local_similarity_route_total']}
- Local fallback routes: {report['agent_metrics']['local_fallback_route_total']}
- LLM guided routes: {report['agent_metrics']['llm_guided_route_total']}
- Local rule resolutions: {report['agent_metrics']['local_rule_resolved_total']}
- LLM assisted resolutions: {report['agent_metrics']['llm_assisted_resolved_total']}

## Human Metrics

- Human approval requests: {report['human_metrics']['human_approval_requests_total']}
- Human full-resolution requests: {report['human_metrics']['human_resolution_requests_total']}
- Human approvals completed: {report['human_metrics']['human_approved_total']}
- Human rejections: {report['human_metrics']['human_rejected_total']}
- Manual fixes: {report['human_metrics']['manual_fix_total']}
- Open human queue: {report['human_metrics']['open_human_queue_total']}

## Performance

- MTTD: {report['performance_metrics']['mttd_seconds']}s
- MTTR: {report['performance_metrics']['mttr_seconds']}s
- Issues detected total: {report['performance_metrics']['issues_detected_total']}
- Issues resolved total: {report['performance_metrics']['issues_resolved_total']}
- Issues escalated total: {report['performance_metrics']['issues_escalated_total']}

## Recent Batches

{recent_batches_lines}

## Open Escalations

{open_escalation_lines}
"""

    def _write_analysis_reports(self):
        report = self.build_analysis_report()
        paths = self._report_paths()
        paths["json"].write_text(json.dumps(report, indent=2), encoding="utf-8")
        paths["md"].write_text(self._analysis_markdown(report), encoding="utf-8")

    def _agent_route(self, issue):
        retrieved = self.local_resolver.retrieve(issue, self._historical_logs())
        top_score = retrieved[0]["score"] if retrieved else 0.0
        local_safe_types = {
            "duplicate_record",
            "invalid_payment_value",
            "invalid_timestamp",
            "freight_outlier",
            "unknown_payment_type",
            "missing_column",
        }
        if issue.get("error_type") in local_safe_types and top_score >= 0.1:
            self.telemetry["local_similarity_route_total"] += 1
            return {
                "route": "local_similarity",
                "reason": "Matched local playbook/history with enough confidence.",
                "retrieved_context": retrieved,
                "llm_suggestion": None,
            }
        if self.openrouter.enabled:
            self.telemetry["llm_calls_total"] += 1
        suggestion = self.openrouter.suggest(issue, retrieved)
        if suggestion:
            route = suggestion.get("route", "llm_guided_local")
            if route == "llm_guided_local":
                self.telemetry["llm_guided_route_total"] += 1
            self.telemetry["llm_success_total"] += 1
            return {
                "route": route,
                "reason": suggestion.get("reason", "LLM-guided decision."),
                "retrieved_context": retrieved,
                "llm_suggestion": suggestion,
            }
        self.telemetry["local_fallback_route_total"] += 1
        return {
            "route": "local_fallback",
            "reason": "No LLM available or no strong local match; using conservative local rules.",
            "retrieved_context": retrieved,
            "llm_suggestion": None,
        }

    def _write_latest_logs(self):
        current = self.state.get("current_cycle")
        if not current:
            return
        (LOGS_DIR / "latest_resolution_log.json").write_text(
            json.dumps(
                {
                    "batch_id": current["batch_id"],
                    "detailed_logs": current["detailed_logs"],
                    "pending_approvals": self.pending_approvals,
                    "all_escalations": self.all_escalations,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_analysis_reports()

    def _now_text(self):
        return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def _simulated_detection_latency(self, severity):
        return {
            "critical": 2,
            "high": 4,
            "medium": 6,
            "low": 8,
        }.get(severity, 5)

    def _simulated_resolution_latency(self, error_type, escalated=False):
        if escalated:
            return {
                "null_key": 180,
                "orphan_record": 150,
                "missing_column": 120,
            }.get(error_type, 210)
        return {
            "duplicate_record": 7,
            "invalid_payment_value": 11,
            "invalid_timestamp": 9,
            "freight_outlier": 6,
            "unknown_payment_type": 5,
            "missing_column": 14,
        }.get(error_type, 10)

    def _load_clean(self):
        if not (CLEAN_DIR / "orders.csv").exists():
            raise FileNotFoundError(
                "Clean dataset missing. Run `python3 scripts/prepare_olist_subset.py` first."
            )
        return load_dataset(CLEAN_DIR)

    def _select_batch(self, clean, rng):
        size = rng.randint(MIN_BATCH_SIZE, MAX_BATCH_SIZE)
        orders = deepcopy(clean["orders"])
        rng.shuffle(orders)
        orders = orders[:size]
        order_ids = {row["order_id"] for row in orders}
        return {
            "orders": orders,
            "payments": [deepcopy(row) for row in clean["payments"] if row.get("order_id") in order_ids],
            "delivery": [deepcopy(row) for row in clean["delivery"] if row.get("order_id") in order_ids],
        }

    def _detect_issues(self, clean, dirty, injected_errors):
        clean_orders = {row["order_id"] for row in clean["orders"] if row.get("order_id")}
        detected = []

        for error in injected_errors:
            entry = {
                "error_id": error["error_id"],
                "detected_at": self._now_text(),
                "detection_latency_sec": self._simulated_detection_latency(error["severity"]),
                "dataset": error["dataset"],
                "severity": error["severity"],
                "error_type": error["type"],
                "row_index": error.get("row_index"),
                "column": error.get("column"),
                "message": error["message"],
                "original_value": error.get("original_value"),
                "dirty_value": error.get("dirty_value"),
                "order_id": error.get("original_value") if error.get("column") == "order_id" else None,
                "detector": "validation-engine",
            }
            dataset = error["dataset"]
            error_type = error["type"]
            if entry["order_id"] is None and error.get("row_index") is not None:
                idx = error["row_index"]
                if 0 <= idx < len(dirty[dataset]):
                    entry["order_id"] = dirty[dataset][idx].get("order_id") or error.get("original_value")

            if error_type == "missing_column":
                entry["finding"] = f"Required column `{error['column']}` is missing from {dataset}."
            elif error_type == "null_key":
                entry["finding"] = "Blank order_id prevents deterministic joins."
            elif error_type == "duplicate_record":
                entry["finding"] = "A duplicate record is present in the dataset."
            elif error_type == "invalid_payment_value":
                entry["finding"] = "Payment value is no longer numeric."
            elif error_type == "orphan_record":
                orphan_id = error.get("dirty_value")
                entry["finding"] = f"`{orphan_id}` does not exist in the clean orders store."
                entry["reference_check"] = orphan_id in clean_orders
            elif error_type == "invalid_timestamp":
                entry["finding"] = "Purchase timestamp format is invalid."
            elif error_type == "freight_outlier":
                entry["finding"] = "Freight value exceeds acceptable range."
            elif error_type == "unknown_payment_type":
                entry["finding"] = "Payment type falls outside the allowed domain."
            else:
                entry["finding"] = "Validation found a data quality issue."

            detected.append(entry)

        return detected

    def _resolve_issues(self, clean, dirty, detected):
        healed = deepcopy(dirty)
        clean_lookup = {
            name: {row["order_id"]: row for row in rows if row.get("order_id")}
            for name, rows in clean.items()
        }
        words_to_numbers = {"ten thousand": "10000.00", "forty two": "42.00"}
        logs = []
        approvals = []

        for issue in detected:
            dataset = issue["dataset"]
            error_type = issue["error_type"]
            row_index = issue.get("row_index")
            column = issue.get("column")
            target_order_id = issue.get("order_id")
            log = {
                "error_id": issue["error_id"],
                "detected_at": issue["detected_at"],
                "resolved_at": self._now_text(),
                "detection_latency_sec": issue.get("detection_latency_sec", 0),
                "dataset": dataset,
                "error_type": error_type,
                "severity": issue["severity"],
                "row_index": row_index,
                "column": column,
                "finding": issue["finding"],
                "dirty_value": issue.get("dirty_value"),
                "original_value": issue.get("original_value"),
                "order_id": target_order_id,
                "resolver": "AI Agent",
                "status": "resolved",
                "resolution_action": "",
                "resolved_value": None,
                "approval_required": False,
                "resolution_latency_sec": 0,
                "agent_route": "",
                "agent_reason": "",
                "retrieved_context": [],
                "llm_suggestion": None,
                "escalation_mode": "",
            }
            agent_plan = self._agent_route(issue)
            log["agent_route"] = agent_plan["route"]
            log["agent_reason"] = agent_plan["reason"]
            log["retrieved_context"] = agent_plan["retrieved_context"]
            log["llm_suggestion"] = agent_plan["llm_suggestion"]

            if error_type == "missing_column":
                restored = 0
                for row in healed[dataset]:
                    order_id = row.get("order_id")
                    if order_id and order_id in clean_lookup[dataset]:
                        row[column] = clean_lookup[dataset][order_id].get(column, "")
                        restored += 1
                if restored == len(healed[dataset]):
                    log["resolution_action"] = f"Recovered missing column `{column}` from clean dataset."
                    log["resolved_value"] = f"restored-for-{restored}-rows"
                    log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)
                else:
                    log["status"] = "escalated"
                    log["resolver"] = "Human Ops"
                    log["approval_required"] = True
                    log["escalation_mode"] = "human_resolution"
                    log["resolution_action"] = f"Could not reconstruct `{column}` for every row."
                    log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type, escalated=True)

            elif error_type == "null_key":
                log["status"] = "escalated"
                log["resolver"] = "Human Ops"
                log["approval_required"] = True
                log["escalation_mode"] = "human_approval"
                proposed_value = issue.get("original_value")
                log["resolution_action"] = "Waiting for human approval to restore the primary key."
                log["resolved_value"] = proposed_value
                log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type, escalated=True)
                approvals.append(
                    {
                        "error_id": log["error_id"],
                        "cycle_id": issue["error_id"].rsplit("-", 1)[0],
                        "dataset": dataset,
                        "row_index": row_index,
                        "column": "order_id",
                        "order_id": proposed_value,
                        "current_value": issue.get("dirty_value"),
                        "proposed_value": proposed_value,
                        "reason": "Restore blank primary key from clean source trace.",
                        "status": "pending",
                        "proposed_field": "order_id",
                        "request_kind": "human_approval",
                    }
                )

            elif error_type == "duplicate_record":
                unique = []
                seen = set()
                for row in healed[dataset]:
                    row_key = json.dumps(row, sort_keys=True)
                    if row_key not in seen:
                        unique.append(row)
                        seen.add(row_key)
                healed[dataset] = unique
                log["resolution_action"] = "Removed duplicate rows and kept the first unique copy."
                log["resolved_value"] = f"{len(unique)} unique rows remain"
                log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)

            elif error_type == "invalid_payment_value":
                target = healed["payments"][row_index]
                dirty_value = str(target.get("payment_value", ""))
                if dirty_value.lower() in words_to_numbers:
                    target["payment_value"] = words_to_numbers[dirty_value.lower()]
                else:
                    order_id = target.get("order_id")
                    fallback = clean_lookup["payments"].get(order_id, {}).get("payment_value", "0.00")
                    target["payment_value"] = fallback
                log["resolution_action"] = "Normalized payment value using a repair rule and clean fallback."
                log["resolved_value"] = target["payment_value"]
                log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)

            elif error_type == "orphan_record":
                order_id = issue.get("original_value")
                if (
                    agent_plan["route"] != "escalate"
                    and row_index is not None
                    and row_index < len(healed[dataset])
                    and order_id in clean_lookup[dataset]
                ):
                    healed[dataset][row_index] = deepcopy(clean_lookup[dataset][order_id])
                    log["resolution_action"] = "Replaced orphan record with matching clean record from standard store."
                    log["resolved_value"] = order_id
                    log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)
                else:
                    log["status"] = "escalated"
                    log["resolver"] = "Human Ops"
                    log["approval_required"] = True
                    log["escalation_mode"] = "human_approval"
                    log["resolution_action"] = "Orphan record needs human confirmation before replacement."
                    log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type, escalated=True)
                    approvals.append(
                        {
                            "error_id": log["error_id"],
                            "cycle_id": issue["error_id"].rsplit("-", 1)[0],
                            "dataset": dataset,
                            "row_index": row_index,
                            "column": "row",
                            "order_id": order_id,
                            "current_value": issue.get("dirty_value"),
                            "proposed_value": order_id,
                            "reason": "Replace orphan row with clean record after human approval.",
                            "status": "pending",
                            "proposed_field": "order_id",
                            "request_kind": "human_approval",
                        }
                    )

            elif error_type == "invalid_timestamp":
                target = healed["orders"][row_index]
                order_id = target.get("order_id")
                resolved_value = clean_lookup["orders"].get(order_id, {}).get(
                    "order_purchase_timestamp",
                    issue.get("original_value"),
                )
                target["order_purchase_timestamp"] = resolved_value
                log["resolution_action"] = "Restored timestamp from clean order record."
                log["resolved_value"] = resolved_value
                log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)

            elif error_type == "freight_outlier":
                target = healed["delivery"][row_index]
                order_id = target.get("order_id")
                resolved_value = clean_lookup["delivery"].get(order_id, {}).get("freight_value", "0.00")
                target["freight_value"] = resolved_value
                log["resolution_action"] = "Replaced freight outlier with clean freight value."
                log["resolved_value"] = resolved_value
                log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)

            elif error_type == "unknown_payment_type":
                target = healed["payments"][row_index]
                order_id = target.get("order_id")
                resolved_value = clean_lookup["payments"].get(order_id, {}).get("payment_type", "voucher")
                target["payment_type"] = resolved_value
                log["resolution_action"] = "Mapped unsupported payment type back to clean domain value."
                log["resolved_value"] = resolved_value
                log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)

            else:
                log["status"] = "escalated"
                log["resolver"] = "Human Ops"
                log["approval_required"] = True
                log["escalation_mode"] = "human_resolution"
                log["resolution_action"] = "No automated playbook exists for this error."
                log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type, escalated=True)

            if log["status"] == "resolved":
                log["resolver"] = (
                    "OpenRouter LLM + Local Context"
                    if agent_plan["llm_suggestion"] and agent_plan["route"] == "llm_guided_local"
                    else "Local Agent"
                )
                if agent_plan["llm_suggestion"] and agent_plan["route"] == "llm_guided_local":
                    self.telemetry["llm_assisted_resolved_total"] += 1
                else:
                    self.telemetry["local_rule_resolved_total"] += 1
            elif log["status"] == "escalated":
                if log["escalation_mode"] == "human_approval":
                    self.telemetry["human_approval_requests_total"] += 1
                else:
                    self.telemetry["human_resolution_requests_total"] += 1

            logs.append(log)

        return healed, logs, approvals

    def _write_batch_outputs(self, batch_id, clean, dirty, healed, injected_errors, detailed_logs):
        batch_dir = RUNS_DIR / batch_id
        batch_dir.mkdir(parents=True, exist_ok=True)
        write_dataset(batch_dir / "clean", clean)
        write_dataset(batch_dir / "dirty", dirty, dirty=True)
        write_dataset(batch_dir / "healed", healed)
        (LOGS_DIR / f"{batch_id}_injected_errors.json").write_text(
            json.dumps({"batch_id": batch_id, "injected_errors": injected_errors}, indent=2),
            encoding="utf-8",
        )
        (LOGS_DIR / f"{batch_id}_resolution_log.json").write_text(
            json.dumps(
                {
                    "batch_id": batch_id,
                    "detailed_logs": detailed_logs,
                    "pending_approvals": self.pending_approvals,
                    "all_escalations": self.all_escalations,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_latest_logs()

    def run_cycle(self):
        cycle_number = self.state["cycle_count"] + 1
        batch_id = f"batch-{cycle_number:03d}"
        started_at = self._now_text()
        rng = random.Random()
        clean = self._select_batch(self._load_clean(), rng)
        dirty, injected_errors = generate_dirty_dataset(clean, cycle_id=batch_id, rng=rng)
        write_dataset(DIRTY_DIR, dirty, dirty=True)
        dirty = load_dataset(DIRTY_DIR, dirty=True)

        detected = self._detect_issues(clean, dirty, injected_errors)
        healed, detailed_logs, approvals = self._resolve_issues(clean, dirty, detected)
        for approval in approvals:
            approval["batch_id"] = batch_id
            approval["created_at"] = started_at
            approval["history"] = []
        self.pending_approvals.extend(approvals)
        self.all_escalations.extend(deepcopy(approvals))
        self._write_batch_outputs(batch_id, clean, dirty, healed, injected_errors, detailed_logs)

        resolved = sum(1 for log in detailed_logs if log["status"] == "resolved")
        escalated = sum(1 for log in detailed_logs if log["status"] == "escalated")
        now = time.time()

        current_cycle = {
            "batch_id": batch_id,
            "started_at": started_at,
            "batch_size": len(clean["orders"]),
            "status": "healthy" if not detected else ("attention" if escalated else "degraded"),
            "injected_errors": injected_errors,
            "detected_issues": detected,
            "detailed_logs": detailed_logs,
            "datasets": {"clean": clean, "dirty": dirty, "healed": healed},
            "summary": {
                "injected": len(injected_errors),
                "detected": len(detected),
                "resolved": resolved,
                "escalated": escalated,
            },
        }

        feed = [
            {
                "time": started_at,
                "stage": "dirty-data",
                "message": f"{batch_id}: random dirty-data errors injected into a {len(clean['orders'])}-row batch.",
            }
        ]
        for log in detailed_logs[:8]:
            feed.append(
                {
                    "time": started_at,
                    "stage": "healing" if log["status"] == "resolved" else "escalation",
                    "message": f"{log['error_id']} | {log['resolution_action']}",
                }
            )

        with self.lock:
            self.state["cycle_count"] = cycle_number
            self.state["last_run_at"] = now
            self.state["next_run_at"] = now + CYCLE_SECONDS
            self.state["current_cycle"] = current_cycle
            self.state["history"] = [current_cycle] + self.state["history"]
            self.state["event_feed"] = (feed + self.state["event_feed"])[:MAX_EVENTS]
            self.state["metrics"]["total_batches"] += 1
            self.state["metrics"]["detected_issues"] += len(detected)
            self.state["metrics"]["auto_resolved"] += resolved
            self.state["metrics"]["human_escalations"] += escalated
            self.telemetry["successful_batches_total"] += 1
            self._write_latest_logs()

    def _find_log(self, error_id):
        current = self.state.get("current_cycle") or {}
        for log in current.get("detailed_logs", []):
            if log["error_id"] == error_id:
                return log
        return None

    def _find_batch_record(self, batch_id):
        current = self.state.get("current_cycle")
        if current and (current.get("batch_id") or current.get("cycle_id")) == batch_id:
            return current
        for batch in self.state.get("history", []):
            if (batch.get("batch_id") or batch.get("cycle_id")) == batch_id:
                return batch
        return None

    def _find_log_in_batch(self, batch, error_id):
        for log in batch.get("detailed_logs", []):
            if log["error_id"] == error_id:
                return log
        return None

    def _find_approval(self, error_id, require_pending=False):
        for item in self.pending_approvals:
            if item["error_id"] == error_id and (not require_pending or item["status"] == "pending"):
                return item
        return None

    def _find_escalation(self, error_id):
        for item in self.all_escalations:
            if item["error_id"] == error_id:
                return item
        return None

    def _update_metrics_from_history(self):
        self.state["metrics"]["auto_resolved"] = sum(
            batch["summary"]["resolved"] for batch in self.state["history"]
        )
        self.state["metrics"]["human_escalations"] = sum(
            batch["summary"]["escalated"] for batch in self.state["history"]
        )

    def _prepend_event(self, stage, message, event_time=None):
        self.state["event_feed"] = [
            {
                "time": event_time or self._now_text(),
                "stage": stage,
                "message": message,
            }
        ] + self.state["event_feed"]
        self.state["event_feed"] = self.state["event_feed"][:MAX_EVENTS]

    def _active_resolution_statuses(self):
        return {"resolved", "approved", "manually_fixed"}

    def _find_batch_containing_log(self, error_id):
        current = self.state.get("current_cycle")
        if current and any(log.get("error_id") == error_id for log in current.get("detailed_logs", [])):
            return current
        for batch in self.state.get("history", []):
            if any(log.get("error_id") == error_id for log in batch.get("detailed_logs", [])):
                return batch
        return None

    def _clean_lookup(self, clean):
        return {
            name: {row["order_id"]: row for row in rows if row.get("order_id")}
            for name, rows in clean.items()
        }

    def _apply_resolution_log(self, healed, clean_lookup, log):
        dataset = log.get("dataset")
        error_type = log.get("error_type")
        row_index = log.get("row_index")
        column = log.get("column")
        resolved_value = log.get("resolved_value")
        if dataset not in healed:
            return

        if error_type == "missing_column":
            for row in healed[dataset]:
                order_id = row.get("order_id")
                if order_id and order_id in clean_lookup[dataset]:
                    row[column] = clean_lookup[dataset][order_id].get(column, "")
            return

        if error_type == "duplicate_record":
            unique = []
            seen = set()
            for row in healed[dataset]:
                row_key = json.dumps(row, sort_keys=True)
                if row_key not in seen:
                    unique.append(row)
                    seen.add(row_key)
            healed[dataset] = unique
            return

        if row_index is None or row_index >= len(healed[dataset]):
            return

        if error_type == "invalid_payment_value":
            healed[dataset][row_index]["payment_value"] = resolved_value
            return

        if error_type == "orphan_record":
            order_id = resolved_value or log.get("original_value")
            if order_id in clean_lookup[dataset]:
                healed[dataset][row_index] = deepcopy(clean_lookup[dataset][order_id])
            return

        if error_type == "invalid_timestamp":
            healed[dataset][row_index][column or "order_purchase_timestamp"] = resolved_value
            return

        if error_type == "freight_outlier":
            healed[dataset][row_index][column or "freight_value"] = resolved_value
            return

        if error_type == "unknown_payment_type":
            healed[dataset][row_index][column or "payment_type"] = resolved_value
            return

        if error_type == "null_key":
            healed[dataset][row_index]["order_id"] = resolved_value
            return

        if column == "row":
            order_id = resolved_value or log.get("original_value")
            if order_id in clean_lookup[dataset]:
                healed[dataset][row_index] = deepcopy(clean_lookup[dataset][order_id])
            else:
                healed[dataset][row_index]["order_id"] = order_id
            return

        if column:
            healed[dataset][row_index][column] = resolved_value

    def _rebuild_healed_for_batch(self, batch):
        healed = deepcopy(batch["datasets"]["dirty"])
        clean_lookup = self._clean_lookup(batch["datasets"]["clean"])
        for log in batch.get("detailed_logs", []):
            if log.get("status") in self._active_resolution_statuses():
                self._apply_resolution_log(healed, clean_lookup, log)
        batch["datasets"]["healed"] = healed

    def _refresh_batch_summary(self, batch):
        resolved = sum(
            1 for item in batch["detailed_logs"] if item.get("status") in self._active_resolution_statuses()
        )
        escalated = sum(1 for item in batch["detailed_logs"] if item.get("status") == "escalated")
        batch["summary"]["resolved"] = resolved
        batch["summary"]["escalated"] = escalated
        detected = len(batch.get("detected_issues", []))
        if detected == 0:
            batch["status"] = "healthy"
        elif escalated > 0:
            batch["status"] = "attention"
        else:
            batch["status"] = "degraded"

    def _remove_pending_approvals_for_batch(self, batch_id):
        self.pending_approvals = [
            item for item in self.pending_approvals if item.get("batch_id") != batch_id
        ]

    def _mark_batch_escalations_rolled_back(self, batch_id, error_ids=None):
        target_ids = set(error_ids or [])
        for item in self.all_escalations:
            if item.get("batch_id") != batch_id:
                continue
            if target_ids and item.get("error_id") not in target_ids:
                continue
            item["status"] = "rolled_back"
            item["rolled_back_at"] = self._now_text()

    def _mark_log_reopened(self, log, reason):
        previous_status = log.get("status")
        history = log.setdefault("rollback_history", [])
        history.append(
            {
                "at": self._now_text(),
                "from_status": previous_status,
                "from_action": log.get("resolution_action"),
                "from_value": log.get("resolved_value"),
                "reason": reason,
            }
        )
        log["status"] = "reopened"
        log["resolver"] = "Rollback Controller"
        log["approval_required"] = False
        log["resolved_at"] = self._now_text()
        log["resolution_action"] = reason
        log["resolved_value"] = None

    def rollback_error(self, error_id):
        with self.lock:
            batch = self._find_batch_containing_log(error_id)
            log = self._find_log_in_batch(batch, error_id) if batch else None
            if not batch or not log:
                raise ValueError("Resolved issue not found.")
            if log.get("status") not in self._active_resolution_statuses():
                raise ValueError("Only active resolved issues can be rolled back.")

            batch_id = batch.get("batch_id") or batch.get("cycle_id")
            self._mark_batch_escalations_rolled_back(batch_id, [error_id])
            self._mark_log_reopened(log, "Rolled back this resolution. Issue reopened for another healing attempt.")
            self._rebuild_healed_for_batch(batch)
            self._refresh_batch_summary(batch)
            self._update_metrics_from_history()
            self._prepend_event("rollback", f"{error_id} | Per-error rollback completed for {batch_id}.")
            self._write_latest_logs()

    def rollback_batch(self, batch_id):
        with self.lock:
            batch = self._find_batch_record(batch_id)
            if not batch:
                raise ValueError("Batch not found.")

            active_logs = [
                log for log in batch.get("detailed_logs", []) if log.get("status") in self._active_resolution_statuses()
            ]
            if not active_logs and batch["datasets"]["healed"] == batch["datasets"]["dirty"]:
                raise ValueError("Batch is already at its dirty-state baseline.")

            self._remove_pending_approvals_for_batch(batch_id)
            self._mark_batch_escalations_rolled_back(batch_id)
            for log in batch.get("detailed_logs", []):
                if log.get("status") in self._active_resolution_statuses() or log.get("status") in {"escalated", "rejected"}:
                    self._mark_log_reopened(log, "Batch rollback restored the batch to the original dirty-state baseline.")
            batch["datasets"]["healed"] = deepcopy(batch["datasets"]["dirty"])
            self._refresh_batch_summary(batch)
            self._update_metrics_from_history()
            self._prepend_event("rollback", f"{batch_id} | Batch rollback restored healed data to the saved dirty snapshot.")
            self._write_latest_logs()

    def reapply_batch_healing(self, batch_id):
        with self.lock:
            batch = self._find_batch_record(batch_id)
            if not batch:
                raise ValueError("Batch not found.")

            self._remove_pending_approvals_for_batch(batch_id)
            healed, detailed_logs, approvals = self._resolve_issues(
                deepcopy(batch["datasets"]["clean"]),
                deepcopy(batch["datasets"]["dirty"]),
                deepcopy(batch.get("detected_issues", [])),
            )
            started_at = self._now_text()
            for approval in approvals:
                approval["batch_id"] = batch_id
                approval["created_at"] = started_at
                approval["history"] = []
            self.pending_approvals.extend(approvals)
            self.all_escalations.extend(deepcopy(approvals))
            batch["datasets"]["healed"] = healed
            batch["detailed_logs"] = detailed_logs
            self._refresh_batch_summary(batch)
            self._update_metrics_from_history()
            self._prepend_event("healing", f"{batch_id} | Batch healing was reapplied from the original dirty snapshot.")
            self._write_latest_logs()

    def approve_escalation(self, error_id):
        with self.lock:
            approval = self._find_approval(error_id, require_pending=True)
            batch = self._find_batch_record(approval["batch_id"]) if approval else None
            if not approval or not batch:
                raise ValueError("Pending approval not found.")

            dataset = approval["dataset"]
            row_index = approval["row_index"]
            clean = batch["datasets"]["clean"]
            healed = batch["datasets"]["healed"]
            log = self._find_log_in_batch(batch, error_id)
            if log is None:
                raise ValueError("Resolution log not found.")

            if approval["column"] == "order_id":
                if row_index is None or row_index >= len(healed[dataset]):
                    raise ValueError("Target row missing.")
                healed[dataset][row_index]["order_id"] = approval["proposed_value"]
            elif approval["column"] == "row":
                order_id = approval["order_id"]
                clean_row = next((row for row in clean[dataset] if row.get("order_id") == order_id), None)
                if clean_row is None or row_index is None or row_index >= len(healed[dataset]):
                    raise ValueError("Clean replacement row not found.")
                healed[dataset][row_index] = deepcopy(clean_row)
            else:
                raise ValueError("Unsupported approval operation.")

            approval["status"] = "approved"
            approval["approved_at"] = self._now_text()
            approval["history"].append({"action": "approved", "at": approval["approved_at"]})
            escalation = self._find_escalation(error_id)
            if escalation:
                escalation.update(deepcopy(approval))
            self.telemetry["human_approved_total"] += 1
            log["status"] = "approved"
            log["resolver"] = "Human Approved"
            log["approval_required"] = False
            log["resolved_at"] = approval["approved_at"]
            log["resolution_action"] = f"Approved by human reviewer. {approval['reason']}"
            log["resolved_value"] = approval["proposed_value"]
            log["resolution_latency_sec"] = max(log.get("resolution_latency_sec", 0), 240)

            resolved = sum(1 for item in batch["detailed_logs"] if item["status"] in {"resolved", "approved", "manually_fixed"})
            escalated = sum(1 for item in batch["detailed_logs"] if item["status"] == "escalated")
            batch["summary"]["resolved"] = resolved
            batch["summary"]["escalated"] = escalated
            batch["status"] = "healthy" if escalated == 0 else "attention"
            self._update_metrics_from_history()
            self._prepend_event(
                "approval",
                f"{error_id} | Human approval applied for {approval['batch_id']}.",
                approval["approved_at"],
            )
            self._write_latest_logs()

    def reject_escalation(self, error_id):
        with self.lock:
            approval = self._find_approval(error_id, require_pending=True)
            batch = self._find_batch_record(approval["batch_id"]) if approval else None
            if not approval:
                raise ValueError("Pending approval not found.")
            approval["status"] = "rejected"
            approval["rejected_at"] = self._now_text()
            approval["history"].append({"action": "rejected", "at": approval["rejected_at"]})
            escalation = self._find_escalation(error_id)
            if escalation:
                escalation.update(deepcopy(approval))
            self.telemetry["human_rejected_total"] += 1
            log = self._find_log_in_batch(batch, error_id) if batch else None
            if log:
                log["status"] = "rejected"
                log["resolver"] = "Human Reviewer"
                log["approval_required"] = False
                log["resolved_at"] = approval["rejected_at"]
                log["resolution_action"] = "Rejected by human reviewer. Proposed change was not applied."
                log["resolution_latency_sec"] = max(log.get("resolution_latency_sec", 0), 180)
                if batch:
                    batch["summary"]["escalated"] = sum(
                        1 for item in batch["detailed_logs"] if item["status"] == "escalated"
                    )
            self._prepend_event(
                "review",
                f"{error_id} | Human reviewer rejected the proposed fix.",
                approval["rejected_at"],
            )
            self._write_latest_logs()

    def manual_fix_escalation(self, error_id, manual_value):
        with self.lock:
            approval = self._find_approval(error_id, require_pending=True)
            batch = self._find_batch_record(approval["batch_id"]) if approval else None
            if not approval or not batch:
                raise ValueError("Pending approval not found.")
            dataset = approval["dataset"]
            row_index = approval["row_index"]
            log = self._find_log_in_batch(batch, error_id)
            if log is None:
                raise ValueError("Resolution log not found.")
            if row_index is None or row_index >= len(batch["datasets"]["healed"][dataset]):
                raise ValueError("Target row missing.")

            if approval["column"] == "order_id":
                batch["datasets"]["healed"][dataset][row_index]["order_id"] = manual_value
            elif approval["column"] == "row":
                batch["datasets"]["healed"][dataset][row_index]["order_id"] = manual_value
            else:
                batch["datasets"]["healed"][dataset][row_index][approval["column"]] = manual_value

            approval["status"] = "manually_fixed"
            approval["manual_value"] = manual_value
            approval["manual_fixed_at"] = self._now_text()
            approval["history"].append({"action": "manual_fix", "at": approval["manual_fixed_at"], "value": manual_value})
            escalation = self._find_escalation(error_id)
            if escalation:
                escalation.update(deepcopy(approval))
            self.telemetry["manual_fix_total"] += 1

            log["status"] = "manually_fixed"
            log["resolver"] = "Human Reviewer"
            log["approval_required"] = False
            log["resolved_at"] = approval["manual_fixed_at"]
            log["resolution_action"] = "Manually fixed by human reviewer."
            log["resolved_value"] = manual_value
            log["resolution_latency_sec"] = max(log.get("resolution_latency_sec", 0), 300)
            batch["summary"]["resolved"] = sum(
                1 for item in batch["detailed_logs"] if item["status"] in {"resolved", "approved", "manually_fixed"}
            )
            batch["summary"]["escalated"] = sum(
                1 for item in batch["detailed_logs"] if item["status"] == "escalated"
            )
            batch["status"] = "healthy" if batch["summary"]["escalated"] == 0 else "attention"
            self._update_metrics_from_history()
            self._prepend_event(
                "approval",
                f"{error_id} | Manual fix applied by human reviewer for {approval['batch_id']}.",
                approval["manual_fixed_at"],
            )
            self._write_latest_logs()

    def loop(self):
        while True:
            time.sleep(1)
            with self.lock:
                running = self.state["running"]
                next_run = self.state["next_run_at"]
            if running and time.time() >= next_run:
                try:
                    self.run_cycle()
                except Exception as exc:
                    with self.lock:
                        self.last_crash_at = time.time()
                        self.telemetry["failed_batches_total"] += 1
                        self.state["event_feed"] = [
                            {
                                "time": self._now_text(),
                                "stage": "system",
                                "message": f"Simulation error: {exc}",
                            }
                        ] + self.state["event_feed"]
                        self.state["next_run_at"] = time.time() + CYCLE_SECONDS

    def handle_action(self, action):
        with self.lock:
            if action == "start":
                self.state["running"] = True
                self.state["next_run_at"] = time.time() + 1
            elif action == "stop":
                self.state["running"] = False
            elif action == "run-now":
                self.state["next_run_at"] = time.time()
            else:
                raise ValueError(f"Unsupported action: {action}")

    def snapshot(self):
        with self.lock:
            payload = deepcopy(self.state)
        payload["server_time"] = time.time()
        payload["required_columns"] = REQUIRED_COLUMNS
        payload["pending_approvals"] = deepcopy(self.pending_approvals)
        payload["all_escalations"] = deepcopy(self.all_escalations)
        payload["analytics"] = self.build_analysis_report()
        return payload

    def batch_errors(self, batch_id):
        with self.lock:
            batch = next(
                (
                    deepcopy(item)
                    for item in self.state.get("history", [])
                    if (item.get("batch_id") or item.get("cycle_id")) == batch_id
                ),
                None,
            )
        if batch is None:
            return None
        return {
            "batch_id": batch_id,
            "detailed_logs": batch.get("detailed_logs", []),
        }

    def current_healed_csv(self, dataset_name):
        with self.lock:
            current = deepcopy(self.state.get("current_cycle"))
        if not current:
            return None

        rows = current.get("datasets", {}).get("healed", {}).get(dataset_name)
        if rows is None:
            return None

        fieldnames = []
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
        return {
            "batch_id": current.get("batch_id") or current.get("cycle_id") or "latest",
            "dataset": dataset_name,
            "csv": buffer.getvalue(),
        }


ENGINE = SimulationEngine()


class Handler(BaseHTTPRequestHandler):
    def _json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, path, content_type):
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _download(self, filename, data, content_type):
        body = data.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._json(ENGINE.snapshot())
            return
        if parsed.path.startswith("/api/healed-data/") and parsed.path.endswith(".csv"):
            dataset_name = parsed.path.removeprefix("/api/healed-data/").removesuffix(".csv")
            if dataset_name not in {"orders", "payments", "delivery"}:
                self._json({"ok": False, "error": "Dataset not found."}, status=404)
                return
            payload = ENGINE.current_healed_csv(dataset_name)
            if payload is None:
                self._json({"ok": False, "error": "No healed dataset available yet."}, status=404)
                return
            self._download(
                f"{payload['batch_id']}-{dataset_name}-healed.csv",
                payload["csv"],
                "text/csv; charset=utf-8",
            )
            return
        if parsed.path.startswith("/api/batch/") and parsed.path.endswith("/errors"):
            parts = parsed.path.strip("/").split("/")
            batch_id = parts[2] if len(parts) == 4 else ""
            payload = ENGINE.batch_errors(batch_id)
            if payload is None:
                self._json({"ok": False, "error": f"Batch `{batch_id}` not found."}, status=404)
                return
            self._json(payload)
            return
        if parsed.path == "/api/analysis-report.json":
            with ENGINE.lock:
                ENGINE.telemetry["report_download_total"] += 1
            self._download(
                "analysis-report.json",
                json.dumps(ENGINE.build_analysis_report(), indent=2),
                "application/json; charset=utf-8",
            )
            return
        if parsed.path == "/api/analysis-report.md":
            with ENGINE.lock:
                ENGINE.telemetry["report_download_total"] += 1
            self._download(
                "analysis-report.md",
                ENGINE._analysis_markdown(ENGINE.build_analysis_report()),
                "text/markdown; charset=utf-8",
            )
            return
        if parsed.path in {"/", "/index.html"}:
            self._file(WEB_DIR / "index.html", "text/html; charset=utf-8")
            return
        if parsed.path == "/styles.css":
            self._file(WEB_DIR / "styles.css", "text/css; charset=utf-8")
            return
        if parsed.path == "/app.js":
            self._file(WEB_DIR / "app.js", "application/javascript; charset=utf-8")
            return
        self.send_error(404, "Not found")

    def do_POST(self):
        if urlparse(self.path).path != "/api/action":
            self.send_error(404, "Not found")
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length or 0).decode("utf-8") or "{}")
        try:
            if payload.get("action") == "approve-escalation":
                ENGINE.approve_escalation(payload.get("error_id", ""))
            elif payload.get("action") == "reject-escalation":
                ENGINE.reject_escalation(payload.get("error_id", ""))
            elif payload.get("action") == "manual-fix-escalation":
                ENGINE.manual_fix_escalation(
                    payload.get("error_id", ""),
                    payload.get("manual_value", ""),
                )
            elif payload.get("action") == "rollback-error":
                ENGINE.rollback_error(payload.get("error_id", ""))
            elif payload.get("action") == "rollback-batch":
                ENGINE.rollback_batch(payload.get("batch_id", ""))
            elif payload.get("action") == "reapply-batch-healing":
                ENGINE.reapply_batch_healing(payload.get("batch_id", ""))
            else:
                ENGINE.handle_action(payload.get("action", ""))
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, status=400)
            return
        self._json({"ok": True, "state": ENGINE.snapshot()})

    def log_message(self, format, *args):
        return


def main():
    DIRTY_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=ENGINE.loop, daemon=True).start()
    host = os.environ.get("SIM_HOST", "127.0.0.1")
    port = int(os.environ.get("SIM_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Server running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
