from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def check_stream_health(df: pd.DataFrame) -> dict:
    """Basic health checks for incoming data streams."""
    logger = setup_logging("monitor.stream")
    health = {
        "rows": len(df),
        "missing_ratio": float(df.isna().mean().mean()) if not df.empty else 1.0,
    }
    log_event(logger, "stream_health", rows=health["rows"], missing_ratio=f"{health['missing_ratio']:.4f}")
    return health
