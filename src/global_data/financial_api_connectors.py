from __future__ import annotations

import datetime as dt
from typing import Optional

import pandas as pd
import requests

from src.utils.logging_setup import setup_logging, log_event


def fetch_world_bank_indicator(indicator: str, country: str = "all", start: int = 1990, end: int = 2025) -> pd.DataFrame:
    """Fetch World Bank indicator data as a DataFrame."""
    logger = setup_logging("global_data.world_bank")
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": 20000, "date": f"{start}:{end}"}
    log_event(logger, "request", url=url, indicator=indicator, country=country)

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) < 2:
        return pd.DataFrame()

    records = data[1]
    rows = []
    for rec in records:
        rows.append({
            "country": rec.get("country", {}).get("value"),
            "country_id": rec.get("country", {}).get("id"),
            "indicator": rec.get("indicator", {}).get("id"),
            "date": rec.get("date"),
            "value": rec.get("value"),
        })
    df = pd.DataFrame(rows)
    log_event(logger, "response", rows=len(df))
    return df


def fetch_fred_series(series_id: str, api_key: Optional[str], start: str = "1990-01-01") -> pd.DataFrame:
    """Fetch FRED series observations."""
    logger = setup_logging("global_data.fred")
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }
    log_event(logger, "request", series=series_id)
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    df = pd.DataFrame(obs)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["series"] = series_id
    log_event(logger, "response", rows=len(df))
    return df


def fetch_market_index_mock(symbol: str = "GLOBAL_EQ") -> pd.DataFrame:
    """Generate a mock market index series for offline demos."""
    logger = setup_logging("global_data.market_mock")
    dates = pd.date_range(end=dt.date.today(), periods=200, freq="B")
    values = pd.Series(range(len(dates))).astype(float).pct_change().fillna(0).cumsum() + 100
    df = pd.DataFrame({"date": dates, "symbol": symbol, "value": values})
    log_event(logger, "mock_generated", rows=len(df))
    return df
