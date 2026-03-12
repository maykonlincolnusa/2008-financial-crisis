from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def resolve_entities(df: pd.DataFrame, name_col: str = "name") -> pd.DataFrame:
    """Resolve entity names using simple normalization rules."""
    logger = setup_logging("kg.entity")
    out = df.copy()
    out["normalized_name"] = out[name_col].str.upper().str.replace("[^A-Z0-9 ]", "", regex=True)
    log_event(logger, "entities_resolved", rows=len(out))
    return out
