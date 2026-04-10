import csv
import random
import sqlite3
from copy import deepcopy
from pathlib import Path

REQUIRED_COLUMNS = {
    "orders": ["order_id", "customer_id", "order_purchase_timestamp", "order_status"],
    "payments": ["order_id", "payment_type", "payment_value", "payment_installments"],
    "delivery": ["order_id", "shipping_limit_date", "seller_id", "freight_value"],
}

DEFAULT_SOURCE_DIR = Path("/Users/garv/Downloads/archive")


def read_csv_rows(path):
    with Path(path).open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path, rows, fieldnames=None):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
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


def load_dataset(directory, dirty=False):
    directory = Path(directory)
    suffix = "_dirty" if dirty else ""
    return {
        "orders": read_csv_rows(directory / f"orders{suffix}.csv"),
        "payments": read_csv_rows(directory / f"payments{suffix}.csv"),
        "delivery": read_csv_rows(directory / f"delivery{suffix}.csv"),
    }


def write_dataset(directory, dataset, dirty=False):
    directory = Path(directory)
    suffix = "_dirty" if dirty else ""
    for name, rows in dataset.items():
        fieldnames = None if dirty else REQUIRED_COLUMNS[name]
        write_csv_rows(directory / f"{name}{suffix}.csv", rows, fieldnames)


def build_clean_dataset(rows, source_dir):
    source_dir = Path(source_dir)
    orders_path = source_dir / "olist_orders_dataset.csv"
    payments_path = source_dir / "olist_order_payments_dataset.csv"
    items_path = source_dir / "olist_order_items_dataset.csv"

    if not (orders_path.exists() and payments_path.exists() and items_path.exists()):
        return None

    orders_source = read_csv_rows(orders_path)
    payments_source = read_csv_rows(payments_path)
    items_source = read_csv_rows(items_path)

    selected_orders = []
    selected_ids = set()
    for row in orders_source:
        order_id = row.get("order_id")
        if not order_id or order_id in selected_ids:
            continue
        selected_orders.append(
            {
                "order_id": order_id,
                "customer_id": row.get("customer_id", ""),
                "order_purchase_timestamp": row.get("order_purchase_timestamp", ""),
                "order_status": row.get("order_status", ""),
            }
        )
        selected_ids.add(order_id)
        if len(selected_orders) >= rows:
            break

    payment_by_order = {}
    for row in payments_source:
        order_id = row.get("order_id")
        if order_id in selected_ids and order_id not in payment_by_order:
            payment_by_order[order_id] = {
                "order_id": order_id,
                "payment_type": row.get("payment_type", ""),
                "payment_value": row.get("payment_value", ""),
                "payment_installments": row.get("payment_installments", ""),
            }

    delivery_by_order = {}
    for row in items_source:
        order_id = row.get("order_id")
        if order_id in selected_ids and order_id not in delivery_by_order:
            delivery_by_order[order_id] = {
                "order_id": order_id,
                "shipping_limit_date": row.get("shipping_limit_date", ""),
                "seller_id": row.get("seller_id", ""),
                "freight_value": row.get("freight_value", ""),
            }

    orders = []
    payments = []
    delivery = []
    for row in selected_orders:
        order_id = row["order_id"]
        if order_id in payment_by_order and order_id in delivery_by_order:
            orders.append(row)
            payments.append(payment_by_order[order_id])
            delivery.append(delivery_by_order[order_id])
        if len(orders) >= rows:
            break

    return {"orders": orders, "payments": payments, "delivery": delivery}


def build_database(db_path, dataset):
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()

    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                order_purchase_timestamp TEXT NOT NULL,
                order_status TEXT NOT NULL
            );
            CREATE TABLE payments (
                order_id TEXT PRIMARY KEY,
                payment_type TEXT NOT NULL,
                payment_value TEXT NOT NULL,
                payment_installments TEXT NOT NULL
            );
            CREATE TABLE delivery (
                order_id TEXT PRIMARY KEY,
                shipping_limit_date TEXT NOT NULL,
                seller_id TEXT NOT NULL,
                freight_value TEXT NOT NULL
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO orders
            (order_id, customer_id, order_purchase_timestamp, order_status)
            VALUES (:order_id, :customer_id, :order_purchase_timestamp, :order_status)
            """,
            dataset["orders"],
        )
        conn.executemany(
            """
            INSERT INTO payments
            (order_id, payment_type, payment_value, payment_installments)
            VALUES (:order_id, :payment_type, :payment_value, :payment_installments)
            """,
            dataset["payments"],
        )
        conn.executemany(
            """
            INSERT INTO delivery
            (order_id, shipping_limit_date, seller_id, freight_value)
            VALUES (:order_id, :shipping_limit_date, :seller_id, :freight_value)
            """,
            dataset["delivery"],
        )
        conn.commit()


def _append_error(errors, cycle_id, error_type, dataset, severity, message, row_index=None, column=None, original_value=None, dirty_value=None):
    errors.append(
        {
            "error_id": f"{cycle_id}-{len(errors) + 1:03d}",
            "cycle_id": cycle_id,
            "type": error_type,
            "dataset": dataset,
            "severity": severity,
            "row_index": row_index,
            "column": column,
            "message": message,
            "original_value": original_value,
            "dirty_value": dirty_value,
        }
    )


def generate_dirty_dataset(clean_dataset, cycle_id, error_count=None, rng=None):
    rng = rng or random.Random()
    dirty = deepcopy(clean_dataset)
    errors = []
    if error_count is None:
        error_count = rng.randint(4, 8)

    def pick_row(name):
        rows = dirty[name]
        if not rows:
            return None, None
        index = rng.randrange(len(rows))
        return rows, index

    operations = []

    def null_key():
        dataset = rng.choice(["orders", "payments", "delivery"])
        rows, idx = pick_row(dataset)
        if idx is None:
            return
        original = rows[idx].get("order_id", "")
        rows[idx]["order_id"] = ""
        _append_error(
            errors,
            cycle_id,
            "null_key",
            dataset,
            "critical",
            "Primary key order_id was blanked out.",
            row_index=idx,
            column="order_id",
            original_value=original,
            dirty_value="",
        )

    def duplicate_row():
        dataset = rng.choice(["orders", "payments", "delivery"])
        rows, idx = pick_row(dataset)
        if idx is None:
            return
        rows.append(deepcopy(rows[idx]))
        _append_error(
            errors,
            cycle_id,
            "duplicate_record",
            dataset,
            "medium",
            "A duplicate record was appended.",
            row_index=idx,
        )

    def invalid_payment_value():
        rows, idx = pick_row("payments")
        if idx is None:
            return
        if "payment_value" not in rows[idx]:
            return
        original = rows[idx]["payment_value"]
        dirty_value = rng.choice(["ten thousand", "NaN??", "forty two"])
        rows[idx]["payment_value"] = dirty_value
        _append_error(
            errors,
            cycle_id,
            "invalid_payment_value",
            "payments",
            "medium",
            "Payment value was converted from numeric to text.",
            row_index=idx,
            column="payment_value",
            original_value=original,
            dirty_value=dirty_value,
        )

    def orphan_record():
        dataset = rng.choice(["payments", "delivery"])
        rows, idx = pick_row(dataset)
        if idx is None:
            return
        original = rows[idx]["order_id"]
        dirty_value = f"orphan-{rng.randint(1000, 9999)}"
        rows[idx]["order_id"] = dirty_value
        _append_error(
            errors,
            cycle_id,
            "orphan_record",
            dataset,
            "high",
            "Foreign key no longer matches a clean order.",
            row_index=idx,
            column="order_id",
            original_value=original,
            dirty_value=dirty_value,
        )

    def bad_timestamp():
        rows, idx = pick_row("orders")
        if idx is None:
            return
        if "order_purchase_timestamp" not in rows[idx]:
            return
        original = rows[idx]["order_purchase_timestamp"]
        dirty_value = "2026/99/99 88:61:00"
        rows[idx]["order_purchase_timestamp"] = dirty_value
        _append_error(
            errors,
            cycle_id,
            "invalid_timestamp",
            "orders",
            "medium",
            "Order purchase timestamp was malformed.",
            row_index=idx,
            column="order_purchase_timestamp",
            original_value=original,
            dirty_value=dirty_value,
        )

    def suspicious_freight():
        rows, idx = pick_row("delivery")
        if idx is None:
            return
        if "freight_value" not in rows[idx]:
            return
        original = rows[idx]["freight_value"]
        dirty_value = "99999.99"
        rows[idx]["freight_value"] = dirty_value
        _append_error(
            errors,
            cycle_id,
            "freight_outlier",
            "delivery",
            "low",
            "Freight value was changed to an unrealistic outlier.",
            row_index=idx,
            column="freight_value",
            original_value=original,
            dirty_value=dirty_value,
        )

    def unknown_payment_type():
        rows, idx = pick_row("payments")
        if idx is None:
            return
        if "payment_type" not in rows[idx]:
            return
        original = rows[idx]["payment_type"]
        dirty_value = "telepathy"
        rows[idx]["payment_type"] = dirty_value
        _append_error(
            errors,
            cycle_id,
            "unknown_payment_type",
            "payments",
            "low",
            "Payment type was changed to an unsupported value.",
            row_index=idx,
            column="payment_type",
            original_value=original,
            dirty_value=dirty_value,
        )

    def missing_column():
        dataset = rng.choice(["payments", "delivery"])
        column = rng.choice(["payment_value"] if dataset == "payments" else ["seller_id", "freight_value"])
        for row in dirty[dataset]:
            row.pop(column, None)
        _append_error(
            errors,
            cycle_id,
            "missing_column",
            dataset,
            "high",
            "A required column was removed from the dataset.",
            column=column,
        )

    def invalid_order_status():
        rows, idx = pick_row("orders")
        if idx is None:
            return
        if "order_status" not in rows[idx]:
            return
        original = rows[idx]["order_status"]
        dirty_value = rng.choice(["delivered_to_mars", "quantum_entangled", "schrodingers_box"])
        rows[idx]["order_status"] = dirty_value
        _append_error(
            errors,
            cycle_id,
            "invalid_order_status",
            "orders",
            "low",
            "Order status was changed to a nonsensical anomaly.",
            row_index=idx,
            column="order_status",
            original_value=original,
            dirty_value=dirty_value,
        )


    operations.extend(
        [
            null_key,
            duplicate_row,
            invalid_payment_value,
            orphan_record,
            bad_timestamp,
            suspicious_freight,
            unknown_payment_type,
            missing_column,
        ]
    )

    rng.shuffle(operations)
    for operation in operations[:error_count]:
        operation()

    return dirty, errors
