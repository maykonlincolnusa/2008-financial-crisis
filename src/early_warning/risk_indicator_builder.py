from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def build_risk_indicators(loans: pd.DataFrame, macro: pd.DataFrame, hpi: pd.DataFrame, market: pd.DataFrame) -> pd.DataFrame:
    """Build early warning indicators for crisis detection."""
    logger = setup_logging("early_warning.indicators")

    df = pd.DataFrame()
    if "date" in loans.columns:
        df["date"] = pd.to_datetime(loans["date"], errors="coerce")
    elif "date" in macro.columns:
        df["date"] = pd.to_datetime(macro["date"], errors="coerce")
    else:
        df["date"] = pd.date_range(end=pd.Timestamp.utcnow(), periods=max(1, len(loans)), freq="M")

    # credit_to_gdp_ratio
    if "credit" in macro.columns and "gdp" in macro.columns:
        df["credit_to_gdp_ratio"] = macro["credit"] / macro["gdp"].replace({0: pd.NA})
    else:
        df["credit_to_gdp_ratio"] = 0.0

    # housing_price_growth
    if not hpi.empty and "value" in hpi.columns:
        hpi_sorted = hpi.copy()
        hpi_sorted["date"] = pd.to_datetime(hpi_sorted.get("date", df["date"]), errors="coerce")
        hpi_sorted = hpi_sorted.sort_values("date")
        df = df.merge(hpi_sorted[["date", "value"]].rename(columns={"value": "hpi"}), on="date", how="left")
        df["housing_price_growth"] = df["hpi"].pct_change().fillna(0)
    else:
        df["housing_price_growth"] = 0.0

    # mortgage_default_rate
    if "delinquency" in loans.columns:
        df["mortgage_default_rate"] = pd.to_numeric(loans["delinquency"], errors="coerce").fillna(0)
    else:
        df["mortgage_default_rate"] = 0.0

    # bank_leverage_ratio
    if "assets" in macro.columns and "equity" in macro.columns:
        df["bank_leverage_ratio"] = macro["assets"] / macro["equity"].replace({0: pd.NA})
    else:
        df["bank_leverage_ratio"] = 0.0

    # interest_rate_spread
    if "mortgage_rate" in loans.columns and "treasury_rate" in loans.columns:
        df["interest_rate_spread"] = loans["mortgage_rate"] - loans["treasury_rate"]
    else:
        df["interest_rate_spread"] = 0.0

    # financial_stress_index
    if not market.empty and "value" in market.columns:
        market_sorted = market.copy()
        market_sorted["date"] = pd.to_datetime(market_sorted.get("date", df["date"]), errors="coerce")
        market_sorted = market_sorted.sort_values("date")
        df = df.merge(market_sorted[["date", "value"]].rename(columns={"value": "market_index"}), on="date", how="left")
        df["financial_stress_index"] = df["market_index"].pct_change().rolling(5).std().fillna(0)
    else:
        df["financial_stress_index"] = 0.0

    log_event(logger, "indicators_built", rows=len(df))
    return df


def save_financial_dataset(df: pd.DataFrame, path: str = "data/financial_dataset.parquet") -> None:
    """Save consolidated financial dataset to parquet."""
    logger = setup_logging("early_warning.dataset")
    out = pd.DataFrame(df)
    out.to_parquet(path, index=False)
    log_event(logger, "dataset_saved", path=path, rows=len(out))


def build_and_save_from_paths(
    loans_path: str,
    macro_path: str,
    hpi_path: str,
    market_path: str,
    out_path: str = "data/financial_dataset.parquet",
) -> None:
    """Load datasets from disk, build indicators, and save a consolidated dataset."""
    logger = setup_logging("early_warning.dataset_builder")
    loans = pd.read_parquet(loans_path) if loans_path else pd.DataFrame()
    macro = pd.read_parquet(macro_path) if macro_path else pd.DataFrame()
    hpi = pd.read_parquet(hpi_path) if hpi_path else pd.DataFrame()
    market = pd.read_parquet(market_path) if market_path else pd.DataFrame()

    indicators = build_risk_indicators(loans, macro, hpi, market)
    save_financial_dataset(indicators, out_path)
    log_event(logger, "dataset_build_done", path=out_path)


def parse_args():
    """Parse CLI arguments for dataset building."""
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--loans", default="data/staging/loans.parquet")
    p.add_argument("--macro", default="data/staging/macro.parquet")
    p.add_argument("--hpi", default="data/staging/housing_price.parquet")
    p.add_argument("--market", default="data/staging/market.parquet")
    p.add_argument("--out", default="data/financial_dataset.parquet")
    return p.parse_args()


def main() -> None:
    """CLI entry point to build the financial dataset."""
    args = parse_args()
    build_and_save_from_paths(args.loans, args.macro, args.hpi, args.market, args.out)


if __name__ == "__main__":
    main()
