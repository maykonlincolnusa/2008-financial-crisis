from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

import pandas as pd

from src.global_data.financial_api_connectors import (
    fetch_fred_series,
    fetch_world_bank_indicator,
    fetch_market_index_mock,
)
from src.utils.config import load_config
from src.utils.logging_setup import setup_logging, log_event


def _manifest_path(base_dir: Path) -> Path:
    """Return manifest path."""
    return base_dir / "_manifest.json"


def _load_manifest(base_dir: Path) -> dict:
    """Load manifest file if present."""
    path = _manifest_path(base_dir)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_manifest(base_dir: Path, manifest: dict) -> None:
    """Persist manifest file."""
    path = _manifest_path(base_dir)
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _append_parquet(df: pd.DataFrame, path: Path) -> None:
    """Append DataFrame to Parquet file, creating if missing."""
    if path.exists():
        existing = pd.read_parquet(path)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates()
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def incremental_ingest(base_dir: Path, name: str, df: pd.DataFrame, key_col: Optional[str] = None) -> None:
    """Incremental ingestion using manifest and optional key column."""
    logger = setup_logging("global_data.ingestion")
    manifest = _load_manifest(base_dir)

    if key_col and key_col in df.columns and name in manifest:
        last_value = manifest[name].get("last_key")
        if last_value is not None:
            df = df[df[key_col] > last_value]

    out_path = base_dir / f"{name}.parquet"
    _append_parquet(df, out_path)

    if key_col and key_col in df.columns and not df.empty:
        manifest[name] = {"last_key": str(df[key_col].max())}
        _save_manifest(base_dir, manifest)

    log_event(logger, "incremental_saved", name=name, rows=len(df), path=str(out_path))


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/global_lake")
    p.add_argument("--fred-series", default=None, help="Comma-separated list")
    p.add_argument("--wb-indicators", default=None, help="Comma-separated list")
    p.add_argument("--country", default="all")
    p.add_argument("--mock-market", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config()
    logger = setup_logging("global_data.ingestion")

    base_dir = Path(args.out)
    base_dir.mkdir(parents=True, exist_ok=True)

    if args.fred_series:
        for series in [s.strip() for s in args.fred_series.split(",") if s.strip()]:
            df = fetch_fred_series(series, cfg.fred_api_key)
            incremental_ingest(base_dir, f"fred_{series}", df, key_col="date")

    if args.wb_indicators:
        for ind in [s.strip() for s in args.wb_indicators.split(",") if s.strip()]:
            df = fetch_world_bank_indicator(ind, country=args.country)
            incremental_ingest(base_dir, f"wb_{ind}", df, key_col="date")

    if args.mock_market:
        df = fetch_market_index_mock()
        incremental_ingest(base_dir, "market_mock", df, key_col="date")

    log_event(logger, "ingestion_done")


if __name__ == "__main__":
    main()
