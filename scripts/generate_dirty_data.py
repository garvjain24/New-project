import argparse
import json
import random
from pathlib import Path

from pipeline_utils import generate_dirty_dataset, load_dataset, write_dataset

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CLEAN_DIR = DATA_DIR / "clean"
DIRTY_DIR = DATA_DIR / "dirty"
LOGS_DIR = DATA_DIR / "logs"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cycle-id", default="manual")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--error-count", type=int, default=None)
    args = parser.parse_args()

    clean_dataset = load_dataset(CLEAN_DIR)
    rng = random.Random(args.seed)
    dirty_dataset, injected_errors = generate_dirty_dataset(
        clean_dataset,
        cycle_id=args.cycle_id,
        error_count=args.error_count,
        rng=rng,
    )

    DIRTY_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    write_dataset(DIRTY_DIR, dirty_dataset, dirty=True)

    latest_payload = {
        "cycle_id": args.cycle_id,
        "seed": args.seed,
        "error_count": len(injected_errors),
        "injected_errors": injected_errors,
    }
    latest_path = LOGS_DIR / "latest_injected_errors.json"
    latest_path.write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")
    (LOGS_DIR / f"{args.cycle_id}_injected_errors.json").write_text(
        json.dumps(latest_payload, indent=2),
        encoding="utf-8",
    )

    print(f"Dirty CSVs written to {DIRTY_DIR}")
    print(f"Injected error log written to {latest_path}")
    print(f"Injected errors: {len(injected_errors)}")


if __name__ == "__main__":
    main()
