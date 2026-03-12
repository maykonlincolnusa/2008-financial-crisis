from __future__ import annotations

import argparse
import pandas as pd

from src.risk_engine.risk_engine import run_risk_engine
from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--loans", required=True)
    p.add_argument("--macro", required=True)
    p.add_argument("--exposures", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.risk_pipeline")

    loans = pd.read_parquet(args.loans)
    macro = pd.read_parquet(args.macro)
    exposures = pd.read_parquet(args.exposures)

    result = run_risk_engine(loans, macro, exposures)
    log_event(logger, "risk_pipeline_done", global_risk=f"{result['global_risk_score']:.4f}")


if __name__ == "__main__":
    main()
