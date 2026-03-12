from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd

from src.utils.logging_setup import setup_logging


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Diret?rio data/raw")
    p.add_argument("--output", required=True, help="Diret?rio data/staging")
    return p.parse_args()


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza??o b?sica: datas e num?ricos
    for col in df.columns:
        if "date" in col.lower() or col.lower().endswith("_dt"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        if df[col].dtype == object:
            # tenta converter num?ricos
            df[col] = pd.to_numeric(df[col], errors="ignore")
    return df


def main():
    args = parse_args()
    logger = setup_logging("etl.loans")

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_dir.rglob("*.csv"))
    if not csv_files:
        logger.warning("Nenhum CSV encontrado em %s", input_dir)
        return

    for csv_path in csv_files:
        logger.info("Processando %s", csv_path)
        df = pd.read_csv(csv_path)
        df = _normalize(df)

        # cria ano/mes com base na primeira coluna de data dispon?vel
        date_cols = [c for c in df.columns if "date" in c.lower() or c.lower().endswith("_dt")]
        if date_cols:
            dt_col = date_cols[0]
            df["year"] = df[dt_col].dt.year
            df["month"] = df[dt_col].dt.month
        else:
            df["year"] = dt.datetime.utcnow().year
            df["month"] = dt.datetime.utcnow().month

        # salva parquet particionado
        out_path = output_dir / "loans_parquet"
        out_path.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False, partition_cols=["year", "month"])

    logger.info("ETL loans conclu?do")


if __name__ == "__main__":
    main()
