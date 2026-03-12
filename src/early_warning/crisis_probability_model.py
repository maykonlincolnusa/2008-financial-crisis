from __future__ import annotations

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier

from src.utils.logging_setup import setup_logging, log_event


def build_crisis_probability(df: pd.DataFrame, target_col: str) -> pd.DataFrame:
    """Train a gradient boosting model and output crisis probability score."""
    logger = setup_logging("early_warning.probability")
    if target_col not in df.columns:
        raise ValueError("target_col not found")

    y = df[target_col]
    X = df.drop(columns=[target_col], errors="ignore")
    for col in X.columns:
        if X[col].dtype == object:
            X[col] = pd.to_numeric(X[col], errors="coerce")
    X = X.fillna(0)

    model = GradientBoostingClassifier()
    model.fit(X, y)
    probs = model.predict_proba(X)[:, 1]

    out = df.copy()
    out["crisis_probability_score"] = probs
    out["risk_level"] = pd.cut(
        out["crisis_probability_score"],
        bins=[-0.01, 0.3, 0.6, 1.01],
        labels=["low", "medium", "high"],
    )
    log_event(logger, "probabilities_built", rows=len(out))
    return out
