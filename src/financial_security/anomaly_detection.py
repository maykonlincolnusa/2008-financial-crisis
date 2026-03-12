from __future__ import annotations

import pandas as pd

try:
    import torch
    from torch import nn
except Exception:  # pragma: no cover
    torch = None

from src.utils.logging_setup import setup_logging, log_event


def detect_anomalies_autoencoder(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    """Detect anomalies using a simple autoencoder."""
    logger = setup_logging("security.anomaly")
    if torch is None:
        log_event(logger, "torch_missing")
        return df

    X = df[feature_cols].fillna(0).values.astype("float32")
    X_t = torch.tensor(X)

    model = nn.Sequential(
        nn.Linear(X_t.shape[1], 16),
        nn.ReLU(),
        nn.Linear(16, 4),
        nn.ReLU(),
        nn.Linear(4, 16),
        nn.ReLU(),
        nn.Linear(16, X_t.shape[1]),
    )

    loss_fn = nn.MSELoss()
    optim = torch.optim.Adam(model.parameters(), lr=1e-3)

    model.train()
    for _ in range(10):
        optim.zero_grad()
        recon = model(X_t)
        loss = loss_fn(recon, X_t)
        loss.backward()
        optim.step()

    model.eval()
    with torch.no_grad():
        recon = model(X_t)
        err = ((recon - X_t) ** 2).mean(dim=1).numpy()

    df = df.copy()
    df["anomaly_score"] = err
    df["anomaly_flag"] = (df["anomaly_score"] > df["anomaly_score"].quantile(0.98)).astype(int)
    log_event(logger, "anomaly_detection_done", flagged=int(df["anomaly_flag"].sum()))
    return df
