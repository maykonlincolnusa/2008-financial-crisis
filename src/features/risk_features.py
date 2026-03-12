from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def _safe_div(num, den):
    return num / den.replace({0: pd.NA})


def build_risk_features(loans: pd.DataFrame, macro: pd.DataFrame, hpi: pd.DataFrame) -> pd.DataFrame:
    logger = setup_logging("features.risk")

    df = loans.copy() if not loans.empty else pd.DataFrame()
    if df.empty:
        log_event(logger, "no_loans")
        return df

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["date_month"] = df["date"].dt.to_period("M").dt.to_timestamp()

    if "loan_amount" in df.columns and "property_value" in df.columns:
        df["loan_to_value_ratio"] = _safe_div(df["loan_amount"], df["property_value"])
    else:
        df["loan_to_value_ratio"] = pd.NA

    if "debt" in df.columns and "income" in df.columns:
        df["debt_to_income_ratio"] = _safe_div(df["debt"], df["income"])
    else:
        df["debt_to_income_ratio"] = pd.NA

    if "credit_score" in df.columns:
        df["credit_risk_score"] = (df["credit_score"] - df["credit_score"].min()) / (
            df["credit_score"].max() - df["credit_score"].min()
        )
    elif "fico" in df.columns:
        df["credit_risk_score"] = (df["fico"] - df["fico"].min()) / (df["fico"].max() - df["fico"].min())
    else:
        df["credit_risk_score"] = 0.0

    if not macro.empty and "date" in macro.columns and "series" in macro.columns:
        macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
        macro_pivot = macro.pivot_table(index="date", columns="series", values="value")
        macro_pivot = macro_pivot.sort_index().ffill()

        macro_pivot.index = macro_pivot.index.to_period("M").to_timestamp()
        if "date_month" in df.columns:
            df = df.merge(macro_pivot, left_on="date_month", right_index=True, how="left")

        numeric_macro = macro_pivot.select_dtypes(include=["number"])
        if not numeric_macro.empty:
            z = (numeric_macro - numeric_macro.mean()) / numeric_macro.std(ddof=0)
            macro_index = z.mean(axis=1)
            df = df.merge(macro_index.rename("macro_risk_index"), left_on="date_month", right_index=True, how="left")
        else:
            df["macro_risk_index"] = 0.0
    else:
        df["macro_risk_index"] = 0.0

    if not hpi.empty and "date" in hpi.columns and "value" in hpi.columns:
        hpi["date"] = pd.to_datetime(hpi["date"], errors="coerce")
        hpi = hpi.sort_values("date")
        hpi["housing_price_trend"] = hpi["value"].pct_change().rolling(3).mean()
        hpi_index = hpi.set_index(hpi["date"].dt.to_period("M").dt.to_timestamp())
        if "date_month" in df.columns:
            df = df.merge(hpi_index[["housing_price_trend"]], left_on="date_month", right_index=True, how="left")
    else:
        df["housing_price_trend"] = 0.0

    if "mortgage_rate" in df.columns and "treasury_rate" in df.columns:
        df["interest_rate_spread"] = df["mortgage_rate"] - df["treasury_rate"]
    else:
        df["interest_rate_spread"] = 0.0

    if "default" in df.columns:
        df["target_default"] = (df["default"] > 0).astype(int)
    else:
        df["target_default"] = 0

    if "foreclosure" in df.columns:
        df["target_foreclosure"] = (df["foreclosure"] > 0).astype(int)
    else:
        df["target_foreclosure"] = 0

    if "delinquency" in df.columns:
        df["target_delinquency"] = (df["delinquency"] > 0).astype(int)
    else:
        df["target_delinquency"] = 0

    log_event(logger, "features_done", rows=len(df))
    return df
