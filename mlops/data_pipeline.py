from __future__ import annotations

import argparse
from pathlib import Path

from src.global_data.global_data_ingestion import main as ingest_main
from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/global_lake")
    p.add_argument("--fred-series", default=None)
    p.add_argument("--wb-indicators", default=None)
    p.add_argument("--country", default="all")
    p.add_argument("--mock-market", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.data_pipeline")

    import sys
    old = sys.argv
    try:
        sys.argv = ["global_data_ingestion", "--out", args.out]
        if args.fred_series:
            sys.argv += ["--fred-series", args.fred_series]
        if args.wb_indicators:
            sys.argv += ["--wb-indicators", args.wb_indicators]
        if args.country:
            sys.argv += ["--country", args.country]
        if args.mock_market:
            sys.argv += ["--mock-market"]
        ingest_main()
    finally:
        sys.argv = old

    log_event(logger, "data_pipeline_done", out=str(Path(args.out)))


if __name__ == "__main__":
    main()
