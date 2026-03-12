from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.utils.logging_setup import setup_logging


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Diret?rio data/raw")
    p.add_argument("--output", required=True, help="Diret?rio data/staging")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("etl.macro")

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = [p for p in input_dir.rglob("*.csv") if "fred" in p.name.lower()]
    if not csv_files:
        logger.warning("Nenhum CSV FRED encontrado em %s", input_dir)
        return

    frames = []
    for path in csv_files:
        logger.info("Lendo %s", path)
        df = pd.read_csv(path)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["series"] = path.stem.replace("fred_", "")
        frames.append(df)

    all_df = pd.concat(frames, ignore_index=True)
    out_path = output_dir / "macro.parquet"
    all_df.to_parquet(out_path, index=False)
    logger.info("Macro agregado salvo em %s", out_path)


if __name__ == "__main__":
    main()
