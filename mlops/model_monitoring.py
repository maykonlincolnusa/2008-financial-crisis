from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def compute_drift_stats(df: pd.DataFrame) -> dict:
    stats = {}
    for col in df.select_dtypes(include=["number"]).columns:
        stats[col] = {
            "mean": float(df[col].mean()),
            "std": float(df[col].std(ddof=0)),
        }
    return stats


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True)
    p.add_argument("--out", default="mlops/monitoring.json")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.monitor")

    path = Path(args.data)
    df = pd.read_parquet(path)
    stats = compute_drift_stats(df)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    log_event(logger, "monitoring_saved", path=str(out_path))


if __name__ == "__main__":
    main()
