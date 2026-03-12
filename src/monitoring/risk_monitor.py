from __future__ import annotations

import pandas as pd

from src.risk_engine.risk_engine import run_risk_engine
from src.utils.logging_setup import setup_logging, log_event


def monitor_risk(loans: pd.DataFrame, macro: pd.DataFrame, exposures: pd.DataFrame) -> dict:
    """Compute risk outputs for monitoring."""
    logger = setup_logging("monitor.risk")
    result = run_risk_engine(loans, macro, exposures)
    log_event(logger, "monitor_done")
    return result
