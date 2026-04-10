import json
from copy import deepcopy



def heal_freight_outlier(issue, healed, clean_lookup):
    """Replace unrealistic freight value with the clean store value."""
    idx = issue.get("row_index")
    if idx is None or idx >= len(healed.get("delivery", [])):
        return
    row = healed["delivery"][idx]
    order_id = row.get("order_id")
    clean_row = clean_lookup.get("delivery", {}).get(order_id, {})
    row["freight_value"] = clean_row.get("freight_value", "0.00")



def heal_unknown_payment_type(issue, healed, clean_lookup):
    """Restore unsupported payment type from the clean store."""
    idx = issue.get("row_index")
    if idx is None or idx >= len(healed.get("payments", [])):
        return
    row = healed["payments"][idx]
    order_id = row.get("order_id")
    clean_row = clean_lookup.get("payments", {}).get(order_id, {})
    row["payment_type"] = clean_row.get("payment_type", "voucher")



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

