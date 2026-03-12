from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from src.pipeline.pipeline_training import main as training_main
from src.utils.logging_setup import setup_logging, log_event


def dataset_hash(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest()


def register_dataset_version(dataset_path: Path, registry_path: Path) -> str:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    h = dataset_hash(dataset_path)

    if registry_path.exists():
        registry = json.loads(registry_path.read_text(encoding="utf-8"))
    else:
        registry = {}

    registry[str(dataset_path)] = {"hash": h}
    registry_path.write_text(json.dumps(registry, indent=2), encoding="utf-8")
    return h


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--processed", required=True)
    p.add_argument("--mlflow", default="./mlflow")
    p.add_argument("--out", default="./models")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.train")

    features_path = Path(args.processed) / "features.parquet"
    if not features_path.exists():
        log_event(logger, "features_missing")
        return

    registry_path = Path("mlops") / "dataset_versions.json"
    h = register_dataset_version(features_path, registry_path)
    log_event(logger, "dataset_versioned", hash=h)

    training_main_args = ["--processed", args.processed, "--mlflow", args.mlflow, "--out", args.out]
    import sys
    old = sys.argv
    try:
        sys.argv = ["pipeline_training"] + training_main_args
        training_main()
    finally:
        sys.argv = old


if __name__ == "__main__":
    main()
