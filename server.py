import csv
import json
import os
import random
import sqlite3
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
WEB_DIR = BASE_DIR / "web"
RUNS_DIR = DATA_DIR / "runs"
DB_PATH = DATA_DIR / "standard.db"
CYCLE_SECONDS = 15
SAMPLE_SIZE = 18
MAX_EVENTS = 80


class SimulationEngine:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = True
        self.next_run_at = time.time() + 2
        self.state = {
            "running": True,
            "cycle_seconds": CYCLE_SECONDS,
            "last_run_at": None,
            "next_run_at": self.next_run_at,
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

    def _timestamp(self):
        return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def _connect(self):
        if not DB_PATH.exists():
            raise FileNotFoundError(
                "standard.db not found. Run `python3 scripts/prepare_olist_subset.py` first."
            )
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def _sample_rows(self):
        with self._connect() as conn:
            orders = [
                dict(row)
                for row in conn.execute(
                    """
                    SELECT order_id, customer_id, order_purchase_timestamp, order_status
                    FROM orders
                    ORDER BY RANDOM()
                    LIMIT ?
                    """,
                    (SAMPLE_SIZE,),
                ).fetchall()
            ]

            if not orders:
                raise RuntimeError("No records available in standard.db")

            order_ids = [row["order_id"] for row in orders]
            placeholders = ",".join("?" for _ in order_ids)
            payments = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT order_id, payment_type, payment_value, payment_installments
                    FROM payments
                    WHERE order_id IN ({placeholders})
                    """,
                    order_ids,
                ).fetchall()
            ]
            delivery = [
                dict(row)
                for row in conn.execute(
                    f"""
                    SELECT order_id, shipping_limit_date, seller_id, freight_value
                    FROM delivery
                    WHERE order_id IN ({placeholders})
                    """,
                    order_ids,
                ).fetchall()
            ]
        return {"orders": orders, "payments": payments, "delivery": delivery}

    def _required_columns(self):
        return {
            "orders": ["order_id", "customer_id", "order_purchase_timestamp", "order_status"],
            "payments": ["order_id", "payment_type", "payment_value", "payment_installments"],
            "delivery": ["order_id", "shipping_limit_date", "seller_id", "freight_value"],
        }

    def _inject_errors(self, datasets):
        dirty = deepcopy(datasets)
        available = [
            self._err_missing_column,
            self._err_null_key,
            self._err_duplicate_row,
            self._err_invalid_payment_value,
            self._err_orphan_record,
            self._err_bad_timestamp,
            self._err_suspicious_freight,
            self._err_unknown_payment_type,
        ]
        random.shuffle(available)
        error_count = random.randint(2, 4)
        selected = available[:error_count]
        applied = []
        for fn in selected:
            info = fn(dirty)
            if info:
                applied.append(info)
        return dirty, applied

    def _pick_dataset_row(self, datasets, name):
        rows = datasets.get(name, [])
        return (rows, random.randrange(len(rows))) if rows else (rows, None)

    def _err_missing_column(self, datasets):
        dataset = random.choice(["payments", "delivery"])
        rows = datasets[dataset]
        if not rows:
            return None
        column = random.choice(
            ["payment_value"] if dataset == "payments" else ["seller_id", "freight_value"]
        )
        for row in rows:
            row.pop(column, None)
        return {
            "type": "schema_drift",
            "dataset": dataset,
            "severity": "high",
            "message": f"Removed required column `{column}` from {dataset}.",
        }

    def _err_null_key(self, datasets):
        dataset = random.choice(["orders", "payments", "delivery"])
        rows, idx = self._pick_dataset_row(datasets, dataset)
        if idx is None:
            return None
        rows[idx]["order_id"] = ""
        return {
            "type": "null_key",
            "dataset": dataset,
            "severity": "critical",
            "message": f"Blank `order_id` injected into {dataset}.",
        }

    def _err_duplicate_row(self, datasets):
        dataset = random.choice(["orders", "payments", "delivery"])
        rows, idx = self._pick_dataset_row(datasets, dataset)
        if idx is None:
            return None
        rows.append(deepcopy(rows[idx]))
        return {
            "type": "duplicate_record",
            "dataset": dataset,
            "severity": "medium",
            "message": f"Duplicated one record in {dataset}.",
        }

    def _err_invalid_payment_value(self, datasets):
        rows = datasets["payments"]
        if not rows:
            return None
        idx = random.randrange(len(rows))
        rows[idx]["payment_value"] = random.choice(["ten thousand", "NaN??", "forty two"])
        return {
            "type": "type_error",
            "dataset": "payments",
            "severity": "medium",
            "message": "Corrupted `payment_value` with text.",
        }

    def _err_orphan_record(self, datasets):
        dataset = random.choice(["payments", "delivery"])
        rows = datasets[dataset]
        if not rows:
            return None
        idx = random.randrange(len(rows))
        rows[idx]["order_id"] = f"orphan-{random.randint(1000, 9999)}"
        return {
            "type": "referential_error",
            "dataset": dataset,
            "severity": "high",
            "message": f"Injected orphan `{dataset}` record with no matching order.",
        }

    def _err_bad_timestamp(self, datasets):
        rows = datasets["orders"]
        if not rows:
            return None
        idx = random.randrange(len(rows))
        rows[idx]["order_purchase_timestamp"] = "2026/99/99 88:61:00"
        return {
            "type": "timestamp_error",
            "dataset": "orders",
            "severity": "medium",
            "message": "Malformed purchase timestamp injected.",
        }

    def _err_suspicious_freight(self, datasets):
        rows = datasets["delivery"]
        if not rows:
            return None
        idx = random.randrange(len(rows))
        rows[idx]["freight_value"] = "99999.99"
        return {
            "type": "outlier",
            "dataset": "delivery",
            "severity": "low",
            "message": "Inserted unrealistic freight charge outlier.",
        }

    def _err_unknown_payment_type(self, datasets):
        rows = datasets["payments"]
        if not rows:
            return None
        idx = random.randrange(len(rows))
        rows[idx]["payment_type"] = "telepathy"
        return {
            "type": "domain_error",
            "dataset": "payments",
            "severity": "low",
            "message": "Unknown payment type injected.",
        }

    def _validate(self, datasets):
        issues = []
        required = self._required_columns()
        orders_index = {
            row["order_id"]
            for row in datasets["orders"]
            if row.get("order_id")
        }

        for dataset_name, rows in datasets.items():
            expected = required[dataset_name]
            actual = set(rows[0].keys()) if rows else set(expected)
            for column in expected:
                if column not in actual:
                    issues.append(
                        {
                            "kind": "missing_column",
                            "dataset": dataset_name,
                            "severity": "high",
                            "message": f"Missing required column `{column}`.",
                        }
                    )

            seen = set()
            for idx, row in enumerate(rows):
                order_id = row.get("order_id", "")
                if not order_id:
                    issues.append(
                        {
                            "kind": "null_key",
                            "dataset": dataset_name,
                            "severity": "critical",
                            "message": f"Row {idx + 1} has blank `order_id`.",
                        }
                    )
                if order_id:
                    row_key = json.dumps(row, sort_keys=True)
                    if row_key in seen:
                        issues.append(
                            {
                                "kind": "duplicate_record",
                                "dataset": dataset_name,
                                "severity": "medium",
                                "message": f"Duplicate row detected in {dataset_name}.",
                            }
                        )
                    seen.add(row_key)

                if dataset_name in {"payments", "delivery"} and order_id and order_id not in orders_index:
                    issues.append(
                        {
                            "kind": "orphan_record",
                            "dataset": dataset_name,
                            "severity": "high",
                            "message": f"`{order_id}` has no matching order record.",
                        }
                    )

                if dataset_name == "payments":
                    if "payment_value" in row:
                        try:
                            float(row["payment_value"])
                        except (TypeError, ValueError):
                            issues.append(
                                {
                                    "kind": "invalid_payment_value",
                                    "dataset": dataset_name,
                                    "severity": "medium",
                                    "message": f"Invalid payment value `{row['payment_value']}`.",
                                }
                            )
                    if row.get("payment_type") and row["payment_type"] not in {
                        "credit_card",
                        "debit_card",
                        "voucher",
                        "boleto",
                        "pix",
                    }:
                        issues.append(
                            {
                                "kind": "unknown_payment_type",
                                "dataset": dataset_name,
                                "severity": "low",
                                "message": f"Unsupported payment type `{row['payment_type']}`.",
                            }
                        )

                if dataset_name == "delivery" and "freight_value" in row:
                    try:
                        freight = float(row["freight_value"])
                        if freight > 1000:
                            issues.append(
                                {
                                    "kind": "freight_outlier",
                                    "dataset": dataset_name,
                                    "severity": "low",
                                    "message": f"Suspicious freight value `{row['freight_value']}`.",
                                }
                            )
                    except (TypeError, ValueError):
                        issues.append(
                            {
                                "kind": "invalid_freight_value",
                                "dataset": dataset_name,
                                "severity": "medium",
                                "message": f"Invalid freight value `{row['freight_value']}`.",
                            }
                        )

                if dataset_name == "orders" and "order_purchase_timestamp" in row:
                    try:
                        datetime.strptime(row["order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S")
                    except (TypeError, ValueError):
                        issues.append(
                            {
                                "kind": "invalid_timestamp",
                                "dataset": dataset_name,
                                "severity": "medium",
                                "message": f"Invalid timestamp `{row['order_purchase_timestamp']}`.",
                            }
                        )
        return issues

    def _heal(self, clean, dirty, issues):
        healed = deepcopy(dirty)
        audit = []
        resolved = 0
        escalated = 0
        words_to_numbers = {
            "ten thousand": 10000.0,
            "forty two": 42.0,
        }
        clean_lookup = {
            name: {row["order_id"]: row for row in rows if row.get("order_id")}
            for name, rows in clean.items()
        }

        for issue in issues:
            kind = issue["kind"]
            dataset = issue["dataset"]
            action = {
                "issue": issue["message"],
                "dataset": dataset,
                "resolution": "",
                "owner": "AI Agent",
                "status": "resolved",
            }

            if kind == "missing_column":
                column = issue["message"].split("`")[1]
                for row in healed[dataset]:
                    order_id = row.get("order_id")
                    if order_id and order_id in clean_lookup[dataset]:
                        row[column] = clean_lookup[dataset][order_id].get(column, "")
                if any(column not in row for row in healed[dataset]):
                    action["status"] = "escalated"
                    action["owner"] = "Human Ops"
                    action["resolution"] = f"Could not reconstruct `{column}` for all rows."
                    escalated += 1
                else:
                    action["resolution"] = f"Recovered `{column}` from the standard database."
                    resolved += 1

            elif kind == "null_key":
                action["status"] = "escalated"
                action["owner"] = "Human Ops"
                action["resolution"] = "Blank primary key blocked automated recovery."
                escalated += 1

            elif kind == "duplicate_record":
                unique = []
                seen = set()
                for row in healed[dataset]:
                    row_key = json.dumps(row, sort_keys=True)
                    if row_key not in seen:
                        unique.append(row)
                        seen.add(row_key)
                healed[dataset] = unique
                action["resolution"] = "Removed duplicate rows."
                resolved += 1

            elif kind == "orphan_record":
                healed[dataset] = [
                    row
                    for row in healed[dataset]
                    if row.get("order_id") in clean_lookup["orders"]
                ]
                action["resolution"] = "Quarantined orphan records."
                resolved += 1

            elif kind == "invalid_payment_value":
                for row in healed["payments"]:
                    value = row.get("payment_value")
                    if isinstance(value, str) and value.lower() in words_to_numbers:
                        row["payment_value"] = f"{words_to_numbers[value.lower()]:.2f}"
                    else:
                        try:
                            float(value)
                        except (TypeError, ValueError):
                            order_id = row.get("order_id")
                            clean_row = clean_lookup["payments"].get(order_id)
                            row["payment_value"] = (
                                clean_row["payment_value"] if clean_row else "0.00"
                            )
                action["resolution"] = "Normalized payment values using rule-based AI recovery."
                resolved += 1

            elif kind == "unknown_payment_type":
                for row in healed["payments"]:
                    if row.get("payment_type") == "telepathy":
                        row["payment_type"] = "voucher"
                action["resolution"] = "Mapped unknown payment type to nearest supported type."
                resolved += 1

            elif kind == "freight_outlier":
                for row in healed["delivery"]:
                    try:
                        if float(row.get("freight_value", 0)) > 1000:
                            row["freight_value"] = "89.90"
                    except (TypeError, ValueError):
                        pass
                action["resolution"] = "Clamped abnormal freight values to a safe threshold."
                resolved += 1

            elif kind == "invalid_timestamp":
                for row in healed["orders"]:
                    value = row.get("order_purchase_timestamp")
                    try:
                        datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    except (TypeError, ValueError):
                        order_id = row.get("order_id")
                        clean_row = clean_lookup["orders"].get(order_id)
                        row["order_purchase_timestamp"] = (
                            clean_row["order_purchase_timestamp"]
                            if clean_row
                            else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                action["resolution"] = "Restored malformed timestamps from the standard database."
                resolved += 1

            else:
                action["status"] = "escalated"
                action["owner"] = "Human Ops"
                action["resolution"] = "No automated playbook available."
                escalated += 1

            audit.append(action)

        remaining = self._validate(healed)
        for issue in remaining:
            audit.append(
                {
                    "issue": issue["message"],
                    "dataset": issue["dataset"],
                    "resolution": "Residual issue remains after AI attempt.",
                    "owner": "Human Ops",
                    "status": "escalated",
                }
            )
            escalated += 1

        return healed, audit, resolved, escalated, remaining

    def _write_cycle_files(self, cycle_id, clean, dirty, healed):
        cycle_dir = RUNS_DIR / cycle_id
        cycle_dir.mkdir(parents=True, exist_ok=True)
        for stage_name, datasets in [("clean", clean), ("dirty", dirty), ("healed", healed)]:
            stage_dir = cycle_dir / stage_name
            stage_dir.mkdir(parents=True, exist_ok=True)
            for dataset_name, rows in datasets.items():
                path = stage_dir / f"{dataset_name}.csv"
                fieldnames = []
                for row in rows:
                    for key in row.keys():
                        if key not in fieldnames:
                            fieldnames.append(key)
                with path.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(row)

    def run_cycle(self):
        clean = self._sample_rows()
        dirty, injected = self._inject_errors(clean)
        issues = self._validate(dirty)
        healed, audit, resolved, escalated, remaining = self._heal(clean, dirty, issues)
        cycle_number = self.state["cycle_count"] + 1
        cycle_id = f"cycle-{cycle_number:03d}"
        self._write_cycle_files(cycle_id, clean, dirty, healed)

        status = "healthy" if not issues else ("degraded" if not remaining else "attention")
        now = time.time()
        record = {
            "cycle_id": cycle_id,
            "started_at": self._timestamp(),
            "status": status,
            "batch_size": len(clean["orders"]),
            "injected_errors": injected,
            "detected_issues": issues,
            "audit": audit,
            "remaining_issues": remaining,
            "datasets": {
                "clean": clean,
                "dirty": dirty,
                "healed": healed,
            },
            "summary": {
                "injected": len(injected),
                "detected": len(issues),
                "resolved": resolved,
                "escalated": escalated,
            },
        }

        event_lines = [
            {
                "time": record["started_at"],
                "stage": "ingestion",
                "message": f"{cycle_id}: pulled {len(clean['orders'])} orders from the standard database.",
            }
        ]
        event_lines.extend(
            {
                "time": record["started_at"],
                "stage": "detection",
                "message": info["message"],
            }
            for info in injected
        )
        event_lines.extend(
            {
                "time": record["started_at"],
                "stage": "healing" if item["status"] == "resolved" else "escalation",
                "message": f"{item['owner']}: {item['resolution']}",
            }
            for item in audit[:6]
        )

        with self.lock:
            self.state["cycle_count"] = cycle_number
            self.state["last_run_at"] = now
            self.state["next_run_at"] = now + CYCLE_SECONDS
            self.state["current_cycle"] = record
            self.state["history"] = ([record] + self.state["history"])[:12]
            self.state["event_feed"] = (event_lines + self.state["event_feed"])[:MAX_EVENTS]
            self.state["metrics"]["total_batches"] += 1
            self.state["metrics"]["detected_issues"] += len(issues)
            self.state["metrics"]["auto_resolved"] += resolved
            self.state["metrics"]["human_escalations"] += escalated

    def scheduler_loop(self):
        while True:
            time.sleep(1)
            if not self.running:
                continue
            with self.lock:
                next_run_at = self.state["next_run_at"]
                running = self.state["running"]
            if running and time.time() >= next_run_at:
                try:
                    self.run_cycle()
                except Exception as exc:
                    with self.lock:
                        self.state["event_feed"] = [
                            {
                                "time": self._timestamp(),
                                "stage": "system",
                                "message": f"Simulation error: {exc}",
                            }
                        ] + self.state["event_feed"]
                        self.state["next_run_at"] = time.time() + CYCLE_SECONDS

    def get_state(self):
        with self.lock:
            snapshot = deepcopy(self.state)
        snapshot["server_time"] = time.time()
        return snapshot

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


ENGINE = SimulationEngine()


class Handler(BaseHTTPRequestHandler):
    def _json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, path, content_type):
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/state":
            self._json(ENGINE.get_state())
            return
        if parsed.path in {"/", "/index.html"}:
            self._serve_file(WEB_DIR / "index.html", "text/html; charset=utf-8")
            return
        if parsed.path == "/styles.css":
            self._serve_file(WEB_DIR / "styles.css", "text/css; charset=utf-8")
            return
        if parsed.path == "/app.js":
            self._serve_file(WEB_DIR / "app.js", "application/javascript; charset=utf-8")
            return
        self.send_error(404, "Not found")

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/api/action":
            self.send_error(404, "Not found")
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length or 0)
        payload = json.loads(raw.decode("utf-8") or "{}")
        try:
            ENGINE.handle_action(payload.get("action", ""))
        except Exception as exc:
            self._json({"ok": False, "error": str(exc)}, status=400)
            return
        self._json({"ok": True, "state": ENGINE.get_state()})

    def log_message(self, format, *args):
        return


def main():
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    thread = threading.Thread(target=ENGINE.scheduler_loop, daemon=True)
    thread.start()
    host = os.environ.get("SIM_HOST", "127.0.0.1")
    port = int(os.environ.get("SIM_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), Handler)
    print(f"Server running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
