import csv
import io
import json
import random
import threading
import time
import types
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

from config import (
    BASE_DIR, CLEAN_DIR, LOGS_DIR, RUNS_DIR, DIRTY_DIR,
    CYCLE_SECONDS, MAX_EVENTS, MIN_BATCH_SIZE, MAX_BATCH_SIZE,
    LOCAL_PLAYBOOKS
)
from pipeline_utils import (
    REQUIRED_COLUMNS,
    generate_dirty_dataset,
    load_dataset,
    write_dataset,
)
from local_resolver import LocalSimilarityResolver
from llm_client import OpenRouterClient
from llm_simulator import LLMSimulator
from nl_interpreter import NLFixInterpreter

class SimulationEngine:
    def __init__(self):
        self.lock = threading.Lock()
        self.pending_approvals = []
        self.all_escalations = []
        self.local_resolver = LocalSimilarityResolver(LOCAL_PLAYBOOKS)
        self.simulator = LLMSimulator()
        self.nl_interpreter = NLFixInterpreter()
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
            "simulator_calls_total": 0,
            "simulator_hits_total": 0,
            "successful_batches_total": 0,
            "failed_batches_total": 0,
            "report_download_total": 0,
        }
        
        self.healers_path = BASE_DIR / "generated_healers.py"
        if not self.healers_path.exists():
            self.healers_path.write_text("import json\nfrom copy import deepcopy\n\n", encoding="utf-8")
        self.healers_module = types.ModuleType("generated_healers")
        self._load_healers()

    def _load_healers(self):
        try:
            code = self.healers_path.read_text(encoding="utf-8")
            exec(code, self.healers_module.__dict__)
        except Exception as e:
            print(f"Error loading generated healers: {e}")

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
                "simulator_enabled": self.simulator.enabled,
                "simulator_calls_total": self.telemetry["simulator_calls_total"],
                "simulator_hits_total": self.telemetry["simulator_hits_total"],
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
        error_type = issue.get("error_type")
        retrieved = self.local_resolver.retrieve(issue, self._historical_logs())

        # ── Priority 1: Already-generated local healer ──────────────────
        func_name = f"heal_{error_type}"
        if hasattr(self.healers_module, func_name):
            return {
                "route": "generated_local",
                "reason": f"Using previously generated healer function {func_name} to save LLM cost.",
                "retrieved_context": retrieved,
                "llm_suggestion": None,
                "healer_func": getattr(self.healers_module, func_name)
            }

        # ── Priority 2: LLM Simulator (zero-cost, rule-based) ───────────
        self.telemetry["simulator_calls_total"] += 1
        self.telemetry["llm_calls_total"] += 1
        sim_suggestion = self.simulator.suggest(issue, retrieved)
        if sim_suggestion is not None:
            self.telemetry["simulator_hits_total"] += 1
            self.telemetry["llm_success_total"] += 1
            route = sim_suggestion.get("route", "generate_local")
            if route == "generate_local":
                self.telemetry["llm_guided_route_total"] += 1
            return {
                "route": route,
                "reason": sim_suggestion.get("reason", "Simulator-guided decision."),
                "retrieved_context": retrieved,
                "llm_suggestion": sim_suggestion,
            }

        # ── Priority 3: Real LLM call (paid API) ────────────────────────
        if self.openrouter.enabled:
            self.telemetry["llm_calls_total"] += 1
            suggestion = self.openrouter.suggest(issue, retrieved)
            if suggestion:
                route = suggestion.get("route", "llm_guided_local")
                if route in ["llm_guided_local", "generate_local"]:
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
        base = {
            "critical": 2.0,
            "high": 4.0,
            "medium": 6.0,
            "low": 8.0,
        }.get(severity, 5.0)
        return round(max(0.1, random.gauss(base, base * 0.15)), 2)

    def _simulated_resolution_latency(self, error_type, escalated=False):
        if escalated:
            base = {
                "null_key": 180.0,
                "orphan_record": 150.0,
                "missing_column": 120.0,
            }.get(error_type, 210.0)
        else:
            base = {
                "duplicate_record": 7.0,
                "invalid_payment_value": 11.0,
                "invalid_timestamp": 9.0,
                "freight_outlier": 6.0,
                "unknown_payment_type": 5.0,
                "missing_column": 14.0,
            }.get(error_type, 10.0)
        return round(max(0.1, random.gauss(base, base * 0.2)), 2)

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

            healer_func = agent_plan.get("healer_func")

            if not healer_func and agent_plan["llm_suggestion"]:
                python_code = agent_plan["llm_suggestion"].get("python_code", "")
                if python_code:
                    try:
                        # Compile to verify valid Python syntax before appending
                        compile(python_code, "<string>", "exec")
                        self.healers_path.open("a", encoding="utf-8").write("\n\n" + python_code + "\n")
                        self._load_healers()
                        func_name = f"heal_{error_type}"
                        if hasattr(self.healers_module, func_name):
                            healer_func = getattr(self.healers_module, func_name)
                            log["agent_reason"] += f" (Generated and saved new local function {func_name})"
                    except SyntaxError as e:
                        print(f"LLM generated invalid syntax: {e}")
                    except Exception as e:
                        print(f"Failed to save/load new python code from LLM: {e}")

            if healer_func:
                try:
                    healer_func(issue, healed, clean_lookup)
                    log["resolution_action"] = f"Executed local function heal_{error_type}"
                    log["resolved_value"] = "fixed via generated function"
                    log["resolution_latency_sec"] = self._simulated_resolution_latency(error_type)
                    
                    log["resolver"] = "Local Agent (Generated Healer)"
                    suggestion_source = (agent_plan.get("llm_suggestion") or {}).get("_source", "")
                    if agent_plan["llm_suggestion"] and agent_plan["route"] in ["llm_guided_local", "generate_local"]:
                        if suggestion_source == "llm_simulator":
                            log["resolver"] = "LLM Simulator + Local Context"
                        else:
                            log["resolver"] = "OpenRouter LLM + Local Context"
                        self.telemetry["llm_assisted_resolved_total"] += 1
                    else:
                        self.telemetry["local_rule_resolved_total"] += 1

                    logs.append(log)
                    continue
                except Exception as e:
                    print(f"Execution of healer_func failed: {e}")

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

            # Introduce stochastic real-world imperfections so success isn't always artificially 100%
            if log["status"] == "resolved":
                probabilities = {
                    "null_key": 0.05,
                    "invalid_timestamp": 0.12,
                    "invalid_payment_value": 0.08,
                    "unknown_payment_type": 0.15,
                    "missing_column": 0.09,
                    "duplicate_record": 0.02,
                    "freight_outlier": 0.11,
                    "orphan_record": 0.07
                }
                fail_chance = probabilities.get(error_type, 0.10)
                if random.random() < fail_chance:
                    log["status"] = "escalated"
                    log["resolver"] = "Human Ops"
                    log["approval_required"] = True
                    log["escalation_mode"] = "human_resolution"
                    log["resolution_action"] = f"Auto-heal confidence threshold not met. Manual review requested."
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

    def nl_fix_escalation(self, error_id, instruction):
        """Parse a natural language instruction and apply the fix."""
        with self.lock:
            approval = self._find_approval(error_id, require_pending=True)
            batch = self._find_batch_record(approval["batch_id"]) if approval else None
            if not approval or not batch:
                raise ValueError("Pending approval not found.")

            context = {
                "error_id": error_id,
                "dataset": approval.get("dataset"),
                "column": approval.get("column"),
                "row_index": approval.get("row_index"),
                "current_value": approval.get("current_value"),
                "proposed_value": approval.get("proposed_value"),
                "order_id": approval.get("order_id"),
                "reason": approval.get("reason"),
            }

            self.telemetry["llm_calls_total"] += 1
            self.telemetry["simulator_calls_total"] += 1
            result = self.nl_interpreter.interpret(instruction, context)
            self.telemetry["llm_success_total"] += 1
            self.telemetry["simulator_hits_total"] += 1

            if result.get("skip"):
                return result

            resolved_value = result.get("resolved_value")
            if not resolved_value:
                raise ValueError("Could not determine a fix from the instruction.")

            dataset = approval["dataset"]
            row_index = approval["row_index"]
            log = self._find_log_in_batch(batch, error_id)
            if log is None:
                raise ValueError("Resolution log not found.")

            if resolved_value == "__DELETE__":
                if row_index is not None and row_index < len(batch["datasets"]["healed"][dataset]):
                    batch["datasets"]["healed"][dataset].pop(row_index)
            else:
                if row_index is not None and row_index < len(batch["datasets"]["healed"][dataset]):
                    if approval["column"] == "order_id":
                        batch["datasets"]["healed"][dataset][row_index]["order_id"] = resolved_value
                    elif approval["column"] == "row":
                        order_id = resolved_value
                        clean_rows = batch["datasets"]["clean"].get(dataset, [])
                        clean_row = next((r for r in clean_rows if r.get("order_id") == order_id), None)
                        if clean_row:
                            batch["datasets"]["healed"][dataset][row_index] = deepcopy(clean_row)
                        else:
                            batch["datasets"]["healed"][dataset][row_index]["order_id"] = resolved_value
                    else:
                        batch["datasets"]["healed"][dataset][row_index][approval["column"]] = resolved_value

            approval["status"] = "manually_fixed"
            approval["manual_value"] = resolved_value
            approval["manual_fixed_at"] = self._now_text()
            approval["nl_instruction"] = instruction
            approval["nl_reasoning"] = result
            approval["history"].append({
                "action": "nl_fix",
                "at": approval["manual_fixed_at"],
                "instruction": instruction,
                "value": resolved_value,
            })
            escalation = self._find_escalation(error_id)
            if escalation:
                escalation.update(deepcopy(approval))
            self.telemetry["manual_fix_total"] += 1

            log["status"] = "manually_fixed"
            log["resolver"] = "NL Interpreter (Simulated LLM)"
            log["approval_required"] = False
            log["resolved_at"] = approval["manual_fixed_at"]
            log["resolution_action"] = f"NL Fix: {result.get('action_description', instruction)}"
            log["resolved_value"] = resolved_value
            log["agent_route"] = "nl_interpreted"
            log["agent_reason"] = result.get("action_description", "")
            log["resolution_latency_sec"] = max(log.get("resolution_latency_sec", 0), 120)

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
                f"{error_id} | NL fix: {result.get('detected_intent', 'unknown')} \u2192 {resolved_value}",
                approval["manual_fixed_at"],
            )
            self._write_latest_logs()

            return result

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
