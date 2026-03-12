from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.models.model_xgboost import train_xgboost
from src.models.model_lightgbm import train_lightgbm
from src.models.model_deep_nn import train_deep_nn
from src.models.model_survival import train_survival
from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--processed", required=True)
    p.add_argument("--mlflow", default="./mlflow")
    p.add_argument("--out", default="./models")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("pipeline.training")

    processed = Path(args.processed)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    features_path = processed / "features.parquet"
    if not features_path.exists():
        log_event(logger, "features_missing", path=str(features_path))
        return

    df = pd.read_parquet(features_path)

    targets = [
        "target_default",
        "target_foreclosure",
        "target_delinquency",
    ]

    for target in targets:
        if target not in df.columns:
            log_event(logger, "target_missing", target=target)
            continue

        log_event(logger, "train_start", target=target)
        train_xgboost(df, target, out_dir, args.mlflow)
        train_lightgbm(df, target, out_dir, args.mlflow)
        train_deep_nn(df, target, out_dir, args.mlflow)

    if "time_to_event" in df.columns and "event" in df.columns:
        train_survival(df, "time_to_event", "event", out_dir, args.mlflow)

    log_event(logger, "training_done")


if __name__ == "__main__":
    main()
