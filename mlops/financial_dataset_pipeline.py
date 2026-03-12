from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from src.global_data.financial_api_connectors import fetch_fred_series, fetch_world_bank_indicator, fetch_market_index_mock
from src.early_warning.risk_indicator_builder import build_and_save_from_paths
from src.utils.config import load_config
from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="config/early_warning_sources.json")
    p.add_argument("--country", default="all")
    p.add_argument("--out", default="data/financial_dataset.parquet")
    return p.parse_args()


def _save_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def main():
    args = parse_args()
    logger = setup_logging("mlops.financial_dataset")
    cfg = load_config()

    config_path = Path(args.config)
    if not config_path.exists():
        log_event(logger, "config_missing", path=str(config_path))
        return

    config = json.loads(config_path.read_text(encoding="utf-8"))

    # Macro and market sources
    macro_frames = []
    market_frames = []
    hpi_frames = []

    for name, spec in config.items():
        if spec.get("source") == "world_bank":
            indicator = spec.get("indicator")
            if indicator and not indicator.startswith("TODO"):
                df = fetch_world_bank_indicator(indicator, country=args.country)
                df["metric"] = name
                macro_frames.append(df)
        if spec.get("source") == "fred":
            series_id = spec.get("series_id")
            if series_id and not series_id.startswith("TODO"):
                df = fetch_fred_series(series_id, cfg.fred_api_key)
                df["metric"] = name
                if name == "housing_price_growth":
                    hpi_frames.append(df)
                elif name == "financial_stress_index":
                    market_frames.append(df)

    if not market_frames:
        market_frames.append(fetch_market_index_mock())

    staging = Path("data/staging")
    if macro_frames:
        macro = pd.concat(macro_frames, ignore_index=True)
        _save_parquet(macro, staging / "macro.parquet")
        log_event(logger, "macro_saved", rows=len(macro))

    if hpi_frames:
        hpi = pd.concat(hpi_frames, ignore_index=True)
        _save_parquet(hpi, staging / "housing_price.parquet")
        log_event(logger, "hpi_saved", rows=len(hpi))

    if market_frames:
        market = pd.concat(market_frames, ignore_index=True)
        _save_parquet(market, staging / "market.parquet")
        log_event(logger, "market_saved", rows=len(market))

    # Loans dataset expected in staging or processed
    loans_path = "data/staging/loans.parquet"
    if not Path(loans_path).exists():
        log_event(logger, "loans_missing", path=loans_path)

    build_and_save_from_paths(
        loans_path=loans_path,
        macro_path=str(staging / "macro.parquet"),
        hpi_path=str(staging / "housing_price.parquet"),
        market_path=str(staging / "market.parquet"),
        out_path=args.out,
    )
    log_event(logger, "financial_dataset_done", path=args.out)


if __name__ == "__main__":
    main()
