from __future__ import annotations

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network import MLPClassifier

from src.utils.logging_setup import setup_logging, log_event


def train_early_warning_models(df: pd.DataFrame, target_col: str) -> dict:
    """Train multiple early warning models and return metrics."""
    logger = setup_logging("early_warning.models")
    if target_col not in df.columns:
        raise ValueError("target_col not found")

    y = df[target_col]
    X = df.drop(columns=[target_col], errors="ignore")
    for col in X.columns:
        if X[col].dtype == object:
            X[col] = pd.to_numeric(X[col], errors="coerce")
    X = X.fillna(0)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    models = {
        "logistic": LogisticRegression(max_iter=500),
        "random_forest": RandomForestClassifier(n_estimators=200, random_state=42),
        "gradient_boosting": GradientBoostingClassifier(),
        "mlp": MLPClassifier(hidden_layer_sizes=(32, 16), max_iter=300),
    }

    metrics = {}
    for name, model in models.items():
        model.fit(X_train, y_train)
        probs = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, probs) if y_test.nunique() > 1 else float("nan")
        metrics[name] = {"auc": auc}
        log_event(logger, "model_trained", model=name, auc=f"{auc:.4f}")

    return metrics
