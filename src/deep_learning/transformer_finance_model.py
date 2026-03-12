from __future__ import annotations

import pandas as pd

try:
    import torch
    from torch import nn
except Exception:  # pragma: no cover
    torch = None

try:
    import tensorflow as tf
except Exception:  # pragma: no cover
    tf = None

from src.utils.logging_setup import setup_logging, log_event


def train_transformer_forecaster(series: pd.Series, steps: int = 12) -> pd.Series:
    """Train a tiny transformer-like model and return forecasts."""
    logger = setup_logging("deep_learning.transformer")
    if torch is None:
        log_event(logger, "torch_missing")
        return pd.Series()

    data = series.fillna(method="ffill").values.astype("float32")
    seq_len = min(12, len(data) - 1)
    X = []
    y = []
    for i in range(len(data) - seq_len):
        X.append(data[i:i+seq_len])
        y.append(data[i+seq_len])

    X = torch.tensor(X)
    y = torch.tensor(y)

    model = nn.TransformerEncoder(
        nn.TransformerEncoderLayer(d_model=seq_len, nhead=2, dim_feedforward=32, batch_first=True),
        num_layers=1,
    )
    head = nn.Linear(seq_len, 1)
    optim = torch.optim.Adam(list(model.parameters()) + list(head.parameters()), lr=1e-2)
    loss_fn = nn.MSELoss()

    for _ in range(20):
        optim.zero_grad()
        out = model(X)
        pred = head(out[:, -1, :])
        loss = loss_fn(pred.squeeze(), y)
        loss.backward()
        optim.step()

    last_seq = torch.tensor(data[-seq_len:]).unsqueeze(0)
    preds = []
    cur_seq = last_seq
    for _ in range(steps):
        out = model(cur_seq)
        next_val = head(out[:, -1, :])
        preds.append(float(next_val.item()))
        cur_seq = torch.cat([cur_seq[:, 1:], next_val.unsqueeze(0)], dim=1)

    log_event(logger, "transformer_done", steps=steps)
    return pd.Series(preds)


def build_tf_transformer(series: pd.Series, steps: int = 12) -> pd.Series:
    """Optional TensorFlow transformer stub for compatibility."""
    logger = setup_logging("deep_learning.transformer_tf")
    if tf is None:
        log_event(logger, "tf_missing")
        return pd.Series()

    values = series.fillna(method="ffill").values.astype("float32")
    preds = values[-steps:]
    log_event(logger, "tf_stub_done", steps=steps)
    return pd.Series(preds)
