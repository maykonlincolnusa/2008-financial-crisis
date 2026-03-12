from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def compute_macro_risk(macro: pd.DataFrame) -> pd.DataFrame:
    """Compute country-level financial stability index from macro indicators."""
    logger = setup_logging("risk.macro")
    if macro.empty:
        return pd.DataFrame()

    df = macro.copy()
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    else:
        df["value"] = 0

    if "country" not in df.columns:
        df["country"] = "GLOBAL"

    out = df.groupby("country")["value"].mean().reset_index()
    out = out.rename(columns={"value": "macro_risk_index"})
    log_event(logger, "macro_risk_done", rows=len(out))
    return out
