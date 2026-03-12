from __future__ import annotations

import argparse

from src.pipeline.pipeline_training import main as training_main
from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--processed", required=True)
    p.add_argument("--mlflow", default="./mlflow")
    p.add_argument("--out", default="./models")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.model_training")

    import sys
    old = sys.argv
    try:
        sys.argv = ["pipeline_training", "--processed", args.processed, "--mlflow", args.mlflow, "--out", args.out]
        training_main()
    finally:
        sys.argv = old

    log_event(logger, "model_training_done")


if __name__ == "__main__":
    main()
