from __future__ import annotations

from src.utils.logging_setup import setup_logging, log_event


def generate_alerts(global_risk_score: float, fraud_count: int) -> list[dict]:
    """Generate alerts based on thresholds."""
    logger = setup_logging("monitor.alerts")
    alerts = []
    if global_risk_score > 0.7:
        alerts.append({"type": "systemic_risk", "severity": "high", "score": global_risk_score})
    if fraud_count > 10:
        alerts.append({"type": "fraud", "severity": "medium", "count": fraud_count})
    log_event(logger, "alerts_generated", count=len(alerts))
    return alerts
