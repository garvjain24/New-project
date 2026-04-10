from pathlib import Path

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
