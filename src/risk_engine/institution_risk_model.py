from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def compute_institution_risk(loans: pd.DataFrame) -> pd.DataFrame:
    """Compute institution-level vulnerability index from loan metrics."""
    logger = setup_logging("risk.institution")
    if loans.empty:
        return pd.DataFrame()

    group_col = "institution" if "institution" in loans.columns else None
    if group_col is None:
        loans["institution"] = "UNKNOWN"
        group_col = "institution"

    df = loans.copy()
    if "delinquency" in df.columns:
        df["delinquency"] = pd.to_numeric(df["delinquency"], errors="coerce").fillna(0)
    else:
        df["delinquency"] = 0

    out = df.groupby(group_col)["delinquency"].mean().reset_index()
    out = out.rename(columns={"delinquency": "institution_vulnerability_index"})
    log_event(logger, "institution_risk_done", rows=len(out))
    return out
