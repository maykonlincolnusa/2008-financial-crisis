from __future__ import annotations

import pandas as pd

from src.global_data.financial_api_connectors import fetch_world_bank_indicator
from src.utils.logging_setup import setup_logging, log_event


def collect_macro_indicators(indicators: list[str], country: str = "all") -> pd.DataFrame:
    """Collect multiple macro indicators and return a concatenated DataFrame."""
    logger = setup_logging("global_data.macro_collectors")
    frames = []
    for ind in indicators:
        df = fetch_world_bank_indicator(ind, country=country)
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    log_event(logger, "macro_collected", indicators=len(indicators), rows=len(out))
    return out
