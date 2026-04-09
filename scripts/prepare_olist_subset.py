import argparse
import csv
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
SOURCE_DIR = DATA_DIR / "source"
CLEAN_DIR = DATA_DIR / "clean"
DB_PATH = DATA_DIR / "standard.db"


def ensure_dirs():
    for path in [SOURCE_DIR, CLEAN_DIR, DATA_DIR / "runs"]:
        path.mkdir(parents=True, exist_ok=True)


def read_csv(path):
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def demo_seed(rows):
    base_time = datetime(2018, 1, 3, 10, 30, 0)
    orders = []
    payments = []
    delivery = []
    payment_types = ["credit_card", "debit_card", "voucher", "boleto", "pix"]
    statuses = ["delivered", "shipped", "processing", "invoiced"]

    for index in range(rows):
        order_id = f"order_{index + 1:04d}"
        customer_id = f"customer_{(index % 70) + 1:04d}"
        purchase_time = base_time + timedelta(hours=index * 5)
        shipping_time = purchase_time + timedelta(days=(index % 5) + 2)
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_purchase_timestamp": purchase_time.strftime("%Y-%m-%d %H:%M:%S"),
                "order_status": statuses[index % len(statuses)],
            }
        )
        payments.append(
            {
                "order_id": order_id,
                "payment_type": payment_types[index % len(payment_types)],
                "payment_value": f"{120 + (index * 7.45):.2f}",
                "payment_installments": str((index % 6) + 1),
            }
        )
        delivery.append(
            {
                "order_id": order_id,
                "shipping_limit_date": shipping_time.strftime("%Y-%m-%d %H:%M:%S"),
                "seller_id": f"seller_{(index % 35) + 1:03d}",
                "freight_value": f"{12 + (index % 9) * 3.25:.2f}",
            }
        )
    return orders, payments, delivery


def build_from_source(rows):
    orders_path = SOURCE_DIR / "olist_orders_dataset.csv"
    payments_path = SOURCE_DIR / "olist_order_payments_dataset.csv"
    items_path = SOURCE_DIR / "olist_order_items_dataset.csv"

    if not (orders_path.exists() and payments_path.exists() and items_path.exists()):
        return None

    orders_source = read_csv(orders_path)
    payments_source = read_csv(payments_path)
    items_source = read_csv(items_path)

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

    selected_orders = [
        row
        for row in selected_orders
        if row["order_id"] in payment_by_order and row["order_id"] in delivery_by_order
    ]
    selected_orders = selected_orders[:rows]
    selected_ids = {row["order_id"] for row in selected_orders}
    payments = [payment_by_order[order_id] for order_id in selected_ids]
    delivery = [delivery_by_order[order_id] for order_id in selected_ids]
    return selected_orders, payments, delivery


def build_database(orders, payments, delivery):
    if DB_PATH.exists():
        DB_PATH.unlink()

    with sqlite3.connect(DB_PATH) as conn:
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
            orders,
        )
        conn.executemany(
            """
            INSERT INTO payments
            (order_id, payment_type, payment_value, payment_installments)
            VALUES (:order_id, :payment_type, :payment_value, :payment_installments)
            """,
            payments,
        )
        conn.executemany(
            """
            INSERT INTO delivery
            (order_id, shipping_limit_date, seller_id, freight_value)
            VALUES (:order_id, :shipping_limit_date, :seller_id, :freight_value)
            """,
            delivery,
        )
        conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200)
    args = parser.parse_args()
    ensure_dirs()

    dataset = build_from_source(args.rows)
    if dataset is None:
        print("Real Olist CSVs not found. Building deterministic demo seed instead.")
        dataset = demo_seed(args.rows)
    else:
        print("Built simplified clean dataset from local Olist source CSVs.")

    orders, payments, delivery = dataset
    write_csv(
        CLEAN_DIR / "orders.csv",
        orders,
        ["order_id", "customer_id", "order_purchase_timestamp", "order_status"],
    )
    write_csv(
        CLEAN_DIR / "payments.csv",
        payments,
        ["order_id", "payment_type", "payment_value", "payment_installments"],
    )
    write_csv(
        CLEAN_DIR / "delivery.csv",
        delivery,
        ["order_id", "shipping_limit_date", "seller_id", "freight_value"],
    )
    build_database(orders, payments, delivery)
    print(f"Wrote clean CSVs to {CLEAN_DIR}")
    print(f"Built canonical SQLite database at {DB_PATH}")
    print(f"Orders: {len(orders)}, Payments: {len(payments)}, Delivery: {len(delivery)}")


if __name__ == "__main__":
    main()
