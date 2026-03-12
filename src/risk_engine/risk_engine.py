from __future__ import annotations

import pandas as pd

from src.risk_engine.institution_risk_model import compute_institution_risk
from src.risk_engine.macro_risk_model import compute_macro_risk
from src.risk_engine.systemic_risk_model import compute_systemic_risk
from src.utils.logging_setup import setup_logging, log_event


def run_risk_engine(loans: pd.DataFrame, macro: pd.DataFrame, exposures: pd.DataFrame) -> dict:
    """Run multi-layer risk calculations and return a consolidated dict."""
    logger = setup_logging("risk.engine")
    inst = compute_institution_risk(loans)
    macro_risk = compute_macro_risk(macro)
    systemic = compute_systemic_risk(exposures)

    result = {
        "global_risk_score": float((inst["institution_vulnerability_index"].mean() + macro_risk["macro_risk_index"].mean()) / 2)
        if not inst.empty and not macro_risk.empty else 0.0,
        "country_financial_stability_index": macro_risk,
        "institution_vulnerability_index": inst,
        "systemic_risk_score": systemic,
    }

    log_event(logger, "risk_engine_done")
    return result
