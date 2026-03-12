from __future__ import annotations

import argparse
import pandas as pd
from sklearn.metrics import roc_auc_score

from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True)
    p.add_argument("--target", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.evaluation")

    df = pd.read_parquet(args.data)
    if args.target not in df.columns:
        log_event(logger, "target_missing", target=args.target)
        return

    y = df[args.target]
    pred_col = "crisis_probability_score"
    if pred_col not in df.columns:
        log_event(logger, "prediction_missing", column=pred_col)
        return

    auc = roc_auc_score(y, df[pred_col]) if y.nunique() > 1 else float("nan")
    log_event(logger, "evaluation_done", auc=f"{auc:.4f}")


if __name__ == "__main__":
    main()
