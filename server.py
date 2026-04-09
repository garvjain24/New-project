import json
import os
import random
import threading
import time
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


class SimulationEngine:
    def __init__(self):
        self.lock = threading.Lock()
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

    def _now_text(self):
        return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def _load_clean(self):
        if not (CLEAN_DIR / "orders.csv").exists():
            raise FileNotFoundError(
                "Clean dataset missing. Run `python3 scripts/prepare_olist_subset.py` first."
            )
        return load_dataset(CLEAN_DIR)

    def _detect_issues(self, clean, dirty, injected_errors):
        clean_orders = {row["order_id"] for row in clean["orders"] if row.get("order_id")}
        detected = []

        for error in injected_errors:
            entry = {
                "error_id": error["error_id"],
                "detected_at": self._now_text(),
                "dataset": error["dataset"],
                "severity": error["severity"],
                "error_type": error["type"],
                "row_index": error.get("row_index"),
                "column": error.get("column"),
                "message": error["message"],
                "original_value": error.get("original_value"),
                "dirty_value": error.get("dirty_value"),
                "detector": "validation-engine",
            }
            dataset = error["dataset"]
            error_type = error["type"]

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

        for issue in detected:
            dataset = issue["dataset"]
            error_type = issue["error_type"]
            row_index = issue.get("row_index")
            column = issue.get("column")
            log = {
                "error_id": issue["error_id"],
                "detected_at": issue["detected_at"],
                "resolved_at": self._now_text(),
                "dataset": dataset,
                "error_type": error_type,
                "severity": issue["severity"],
                "row_index": row_index,
                "column": column,
                "finding": issue["finding"],
                "dirty_value": issue.get("dirty_value"),
                "original_value": issue.get("original_value"),
                "resolver": "AI Agent",
                "status": "resolved",
                "resolution_action": "",
                "resolved_value": None,
            }

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
                else:
                    log["status"] = "escalated"
                    log["resolver"] = "Human Ops"
                    log["resolution_action"] = f"Could not reconstruct `{column}` for every row."

            elif error_type == "null_key":
                log["status"] = "escalated"
                log["resolver"] = "Human Ops"
                log["resolution_action"] = "Blank primary key cannot be inferred safely."

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

            elif error_type == "orphan_record":
                order_id = issue.get("original_value")
                if row_index is not None and row_index < len(healed[dataset]) and order_id in clean_lookup[dataset]:
                    healed[dataset][row_index] = deepcopy(clean_lookup[dataset][order_id])
                    log["resolution_action"] = "Replaced orphan record with matching clean record from standard store."
                    log["resolved_value"] = order_id
                else:
                    log["status"] = "escalated"
                    log["resolver"] = "Human Ops"
                    log["resolution_action"] = "Orphan record could not be matched confidently."

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

            elif error_type == "freight_outlier":
                target = healed["delivery"][row_index]
                order_id = target.get("order_id")
                resolved_value = clean_lookup["delivery"].get(order_id, {}).get("freight_value", "0.00")
                target["freight_value"] = resolved_value
                log["resolution_action"] = "Replaced freight outlier with clean freight value."
                log["resolved_value"] = resolved_value

            elif error_type == "unknown_payment_type":
                target = healed["payments"][row_index]
                order_id = target.get("order_id")
                resolved_value = clean_lookup["payments"].get(order_id, {}).get("payment_type", "voucher")
                target["payment_type"] = resolved_value
                log["resolution_action"] = "Mapped unsupported payment type back to clean domain value."
                log["resolved_value"] = resolved_value

            else:
                log["status"] = "escalated"
                log["resolver"] = "Human Ops"
                log["resolution_action"] = "No automated playbook exists for this error."

            logs.append(log)

        return healed, logs

    def _write_cycle_outputs(self, cycle_id, clean, dirty, healed, injected_errors, detailed_logs):
        cycle_dir = RUNS_DIR / cycle_id
        cycle_dir.mkdir(parents=True, exist_ok=True)
        write_dataset(cycle_dir / "clean", clean)
        write_dataset(cycle_dir / "dirty", dirty, dirty=True)
        write_dataset(cycle_dir / "healed", healed)
        (LOGS_DIR / f"{cycle_id}_injected_errors.json").write_text(
            json.dumps({"cycle_id": cycle_id, "injected_errors": injected_errors}, indent=2),
            encoding="utf-8",
        )
        (LOGS_DIR / f"{cycle_id}_resolution_log.json").write_text(
            json.dumps({"cycle_id": cycle_id, "detailed_logs": detailed_logs}, indent=2),
            encoding="utf-8",
        )
        (LOGS_DIR / "latest_resolution_log.json").write_text(
            json.dumps({"cycle_id": cycle_id, "detailed_logs": detailed_logs}, indent=2),
            encoding="utf-8",
        )

    def run_cycle(self):
        cycle_number = self.state["cycle_count"] + 1
        cycle_id = f"cycle-{cycle_number:03d}"
        started_at = self._now_text()
        clean = self._load_clean()
        dirty, injected_errors = generate_dirty_dataset(clean, cycle_id=cycle_id, rng=random.Random())
        write_dataset(DIRTY_DIR, dirty, dirty=True)
        dirty = load_dataset(DIRTY_DIR, dirty=True)

        detected = self._detect_issues(clean, dirty, injected_errors)
        healed, detailed_logs = self._resolve_issues(clean, dirty, detected)
        self._write_cycle_outputs(cycle_id, clean, dirty, healed, injected_errors, detailed_logs)

        resolved = sum(1 for log in detailed_logs if log["status"] == "resolved")
        escalated = sum(1 for log in detailed_logs if log["status"] == "escalated")
        now = time.time()

        current_cycle = {
            "cycle_id": cycle_id,
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
                "message": f"{cycle_id}: dirty datasets regenerated from the 200-row clean source.",
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
            self.state["history"] = ([current_cycle] + self.state["history"])[:12]
            self.state["event_feed"] = (feed + self.state["event_feed"])[:MAX_EVENTS]
            self.state["metrics"]["total_batches"] += 1
            self.state["metrics"]["detected_issues"] += len(detected)
            self.state["metrics"]["auto_resolved"] += resolved
            self.state["metrics"]["human_escalations"] += escalated

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
        return payload


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

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._json(ENGINE.snapshot())
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
