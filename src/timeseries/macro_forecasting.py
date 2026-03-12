from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

try:
    from statsmodels.tsa.arima.model import ARIMA
except Exception:  # pragma: no cover
    ARIMA = None

try:
    import torch
    from torch import nn
except Exception:  # pragma: no cover
    torch = None

from src.utils.logging_setup import setup_logging, log_event


def forecast_arima(series: pd.Series, steps: int = 12):
    if ARIMA is None:
        raise RuntimeError("statsmodels not installed")
    model = ARIMA(series, order=(1, 1, 1))
    fit = model.fit()
    return fit.forecast(steps=steps)


def forecast_lstm(series: pd.Series, steps: int = 12):
    if torch is None:
        raise RuntimeError("torch not installed")

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

    return pd.Series(preds)


def forecast_tft(series: pd.Series, steps: int = 12):
    try:
        from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
        from pytorch_forecasting.data import GroupNormalizer
        import pytorch_lightning as pl
    except Exception as e:  # pragma: no cover
        raise RuntimeError("pytorch-forecasting not installed") from e

    df = pd.DataFrame({"value": series.fillna(method="ffill").values})
    df["time_idx"] = range(len(df))
    df["series_id"] = 0

    max_encoder = min(24, max(5, len(df) - steps))
    training = TimeSeriesDataSet(
        df,
        time_idx="time_idx",
        target="value",
        group_ids=["series_id"],
        max_encoder_length=max_encoder,
        max_prediction_length=steps,
        time_varying_unknown_reals=["value"],
        target_normalizer=GroupNormalizer(groups=["series_id"]),
    )

    train_dataloader = training.to_dataloader(train=True, batch_size=32, num_workers=0)
    model = TemporalFusionTransformer.from_dataset(training, learning_rate=1e-3, hidden_size=8)

    trainer = pl.Trainer(max_epochs=1, enable_checkpointing=False, logger=False)
    trainer.fit(model, train_dataloader)

    preds = model.predict(train_dataloader, mode="prediction")
    if hasattr(preds, "numpy"):
        preds = preds.numpy()
    forecast = preds[-1].ravel()[:steps]
    return pd.Series(forecast)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--date-col", default="date")
    p.add_argument("--target-col", required=True)
    p.add_argument("--model", choices=["arima", "lstm", "tft"], default="arima")
    p.add_argument("--steps", type=int, default=12)
    p.add_argument("--output", default="data/processed/forecasts.parquet")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("timeseries")

    path = Path(args.input)
    if path.suffix == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    df[args.date_col] = pd.to_datetime(df[args.date_col], errors="coerce")
    series = df.sort_values(args.date_col)[args.target_col]

    if args.model == "arima":
        forecast = forecast_arima(series, args.steps)
    elif args.model == "lstm":
        forecast = forecast_lstm(series, args.steps)
    else:
        forecast = forecast_tft(series, args.steps)

    out = pd.DataFrame({"horizon": list(range(1, args.steps + 1)), "forecast": forecast})
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    log_event(logger, "forecast_saved", path=str(out_path))


if __name__ == "__main__":
    main()
