from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.features.risk_features import build_risk_features
from src.utils.logging_setup import setup_logging, log_event
from src.pipeline.pipeline_ingest import validate_dataframe


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--staging", required=True)
    p.add_argument("--processed", required=True)
    p.add_argument("--validate", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("pipeline.features")

    staging = Path(args.staging)
    processed = Path(args.processed)
    processed.mkdir(parents=True, exist_ok=True)

    loans_dir = staging / "loans_parquet"
    macro_path = staging / "macro.parquet"
    hpi_path = staging / "housing_price.parquet"

    loans = pd.read_parquet(loans_dir) if loans_dir.exists() else pd.DataFrame()
    macro = pd.read_parquet(macro_path) if macro_path.exists() else pd.DataFrame()
    hpi = pd.read_parquet(hpi_path) if hpi_path.exists() else pd.DataFrame()

    features = build_risk_features(loans, macro, hpi)

    if args.validate:
        validate_dataframe(features)

    features_path = processed / "features.parquet"
    features.to_parquet(features_path, index=False)
    log_event(logger, "features_saved", path=str(features_path), rows=len(features))

    # Optional: derive X/y for supervised tasks
    y_cols = [c for c in features.columns if c.startswith("target_")]
    X = features.drop(columns=y_cols, errors="ignore")

    X_path = processed / "X.parquet"
    X.to_parquet(X_path, index=False)

    if y_cols:
        y = features[y_cols]
        y_path = processed / "y.parquet"
        y.to_parquet(y_path, index=False)


if __name__ == "__main__":
    main()
