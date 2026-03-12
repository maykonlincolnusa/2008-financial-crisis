from __future__ import annotations

import pandas as pd
from sklearn.ensemble import IsolationForest

from src.utils.logging_setup import setup_logging, log_event


def detect_fraud_isolation_forest(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Detect suspicious patterns using Isolation Forest."""
    logger = setup_logging("security.fraud")
    X = df[feature_cols].fillna(0)
    model = IsolationForest(contamination=0.02, random_state=42)
    preds = model.fit_predict(X)
    df = df.copy()
    df["fraud_flag"] = (preds == -1).astype(int)
    log_event(logger, "fraud_detection_done", flagged=int(df["fraud_flag"].sum()))
    return df
