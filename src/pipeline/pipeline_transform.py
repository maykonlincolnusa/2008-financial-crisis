from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd

try:
    import polars as pl
except Exception:  # pragma: no cover
    pl = None

from src.utils.logging_setup import setup_logging, log_event
from src.pipeline.pipeline_ingest import validate_dataframe


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--validate", action="store_true")
    return p.parse_args()


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if "date" in col.lower() or col.lower().endswith("_dt"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        if df[col].dtype == object:
            df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


def _write_parquet(df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir, index=False, partition_cols=["year", "month"])


def main():
    args = parse_args()
    logger = setup_logging("pipeline.transform")

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_dir.rglob("*.csv"))
    if not csv_files:
        log_event(logger, "no_csv", input=str(input_dir))
        return

    out_path = output_dir / "loans_parquet"

    for csv_path in csv_files:
        if "fred" in csv_path.name.lower():
            continue
        log_event(logger, "read_csv", path=str(csv_path))
        if pl is not None:
            df_pl = pl.read_csv(csv_path)
            df = df_pl.to_pandas()
        else:
            df = pd.read_csv(csv_path)

        df = _normalize(df)

        date_cols = [c for c in df.columns if "date" in c.lower() or c.lower().endswith("_dt")]
        if date_cols:
            dt_col = date_cols[0]
            df["year"] = df[dt_col].dt.year
            df["month"] = df[dt_col].dt.month
        else:
            df["year"] = dt.datetime.utcnow().year
            df["month"] = dt.datetime.utcnow().month

        if args.validate:
            validate_dataframe(df)

        _write_parquet(df, out_path)

    # aggregate macro (FRED)
    fred_files = [p for p in csv_files if "fred" in p.name.lower()]
    if fred_files:
        frames = []
        for path in fred_files:
            df = pd.read_csv(path)
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            if "value" in df.columns:
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df["series"] = path.stem.replace("fred_", "")
            frames.append(df)
        macro = pd.concat(frames, ignore_index=True)
        macro_path = output_dir / "macro.parquet"
        macro.to_parquet(macro_path, index=False)
        log_event(logger, "macro_saved", path=str(macro_path), rows=len(macro))

    log_event(logger, "transform_done", out=str(out_path))


if __name__ == "__main__":
    main()
