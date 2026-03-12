from __future__ import annotations

import pandas as pd

from src.systemic_risk.network_builder import build_financial_network, compute_network_metrics, systemic_risk_score
from src.utils.logging_setup import setup_logging, log_event


def compute_systemic_risk(exposures: pd.DataFrame) -> float:
    """Compute systemic risk score using network metrics and exposures."""
    logger = setup_logging("risk.systemic")
    if exposures.empty:
        return 0.0
    g = build_financial_network(exposures)
    metrics = compute_network_metrics(g)
    score = systemic_risk_score(metrics, exposures)
    log_event(logger, "systemic_risk_done", score=f"{score:.4f}")
    return float(score)
