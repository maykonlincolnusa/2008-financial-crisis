from __future__ import annotations

import pandas as pd

try:
    import torch
    from torch import nn
except Exception:  # pragma: no cover
    torch = None

from src.utils.logging_setup import setup_logging, log_event


def train_lstm_forecaster(series: pd.Series, steps: int = 12) -> pd.Series:
    """Train a small LSTM and return forecasts."""
    logger = setup_logging("deep_learning.lstm")
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

    X = torch.tensor(X).unsqueeze(-1)
    y = torch.tensor(y).unsqueeze(-1)

    model = nn.LSTM(input_size=1, hidden_size=16, batch_first=True)
    head = nn.Linear(16, 1)
    optim = torch.optim.Adam(list(model.parameters()) + list(head.parameters()), lr=1e-2)
    loss_fn = nn.MSELoss()

    for _ in range(30):
        optim.zero_grad()
        out, _ = model(X)
        pred = head(out[:, -1, :])
        loss = loss_fn(pred, y)
        loss.backward()
        optim.step()

    last_seq = torch.tensor(data[-seq_len:]).unsqueeze(0).unsqueeze(-1)
    preds = []
    cur_seq = last_seq
    for _ in range(steps):
        out, _ = model(cur_seq)
        next_val = head(out[:, -1, :])
        preds.append(float(next_val.item()))
        cur_seq = torch.cat([cur_seq[:, 1:, :], next_val.unsqueeze(0)], dim=1)

    log_event(logger, "lstm_done", steps=steps)
    return pd.Series(preds)
