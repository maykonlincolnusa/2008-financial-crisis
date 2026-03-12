from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.utils.logging_setup import setup_logging


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Diret?rio data/staging")
    p.add_argument("--output", required=True, help="Diret?rio data/processed")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("features")

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    loans_dir = input_dir / "loans_parquet"
    macro_path = input_dir / "macro.parquet"

    if not loans_dir.exists():
        logger.warning("Loans parquet n?o encontrado: %s", loans_dir)
        return

    loans = pd.read_parquet(loans_dir)

    if macro_path.exists():
        macro = pd.read_parquet(macro_path)
        # pivot macro em colunas por s?rie
        macro_pivot = macro.pivot_table(index="date", columns="series", values="value")
        macro_pivot = macro_pivot.sort_index().ffill()

        # mescla por data (aproxima pelo m?s)
        if "date" in loans.columns:
            loans["date"] = pd.to_datetime(loans["date"], errors="coerce")
            loans["date_month"] = loans["date"].dt.to_period("M").dt.to_timestamp()
            macro_pivot.index = pd.to_datetime(macro_pivot.index).to_period("M").to_timestamp()
            loans = loans.merge(macro_pivot, left_on="date_month", right_index=True, how="left")

    # exemplo: target de delinquency_12m (placeholder)
    if "delinquency" in loans.columns:
        loans["target_dq_12m"] = (loans["delinquency"] >= 1).astype(int)
    else:
        loans["target_dq_12m"] = 0

    # separa X, y
    y = loans["target_dq_12m"]
    X = loans.drop(columns=["target_dq_12m"], errors="ignore")

    X_path = output_dir / "X.parquet"
    y_path = output_dir / "y.parquet"
    X.to_parquet(X_path, index=False)
    y.to_frame("target_dq_12m").to_parquet(y_path, index=False)

    logger.info("Features salvas em %s e %s", X_path, y_path)


if __name__ == "__main__":
    main()
