from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def map_entities(df: pd.DataFrame, name_col: str = "name") -> pd.DataFrame:
    """Normalize entity names for KG insertion."""
    logger = setup_logging("kg.entity_mapper")
    out = df.copy()
    out["normalized_name"] = out[name_col].str.upper().str.replace("[^A-Z0-9 ]", "", regex=True)
    log_event(logger, "entities_mapped", rows=len(out))
    return out
