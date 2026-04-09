import argparse
from pathlib import Path

from pipeline_utils import (
    DEFAULT_SOURCE_DIR,
    build_clean_dataset,
    build_database,
    write_dataset,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
CLEAN_DIR = DATA_DIR / "clean"
SOURCE_DIR = DATA_DIR / "source"
DB_PATH = DATA_DIR / "standard.db"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200)
    parser.add_argument("--source-dir", default="")
    args = parser.parse_args()

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "dirty").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "logs").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "runs").mkdir(parents=True, exist_ok=True)

    source_dir = Path(args.source_dir) if args.source_dir else SOURCE_DIR
    dataset = build_clean_dataset(args.rows, source_dir)

    if dataset is None and source_dir != DEFAULT_SOURCE_DIR:
        dataset = build_clean_dataset(args.rows, DEFAULT_SOURCE_DIR)
        source_dir = DEFAULT_SOURCE_DIR

    if dataset is None:
        raise FileNotFoundError(
            "Could not find the Olist CSVs. Put them in data/source or use --source-dir."
        )

    write_dataset(CLEAN_DIR, dataset)
    build_database(DB_PATH, dataset)

    print(f"Built clean dataset from {source_dir}")
    print(f"Clean CSVs written to {CLEAN_DIR}")
    print(f"Canonical database written to {DB_PATH}")
    print(
        "Rows: "
        f"orders={len(dataset['orders'])}, "
        f"payments={len(dataset['payments'])}, "
        f"delivery={len(dataset['delivery'])}"
    )


if __name__ == "__main__":
    main()
