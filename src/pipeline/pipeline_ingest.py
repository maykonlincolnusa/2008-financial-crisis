from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

try:
    import great_expectations as ge
except Exception:  # pragma: no cover
    ge = None

from src.utils.config import RAW_DIR, load_config
from src.utils.logging_setup import setup_logging, log_event


def _today_dir() -> Path:
    return RAW_DIR / dt.datetime.utcnow().strftime("%Y%m%d")


def download_url(url: str, dest: Path, auth: Optional[tuple[str, str]] = None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logging("pipeline.ingest")
    log_event(logger, "download_start", url=url, dest=str(dest))

    with requests.get(url, auth=auth, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        chunk_size = 1024 * 1024
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    log_event(logger, "download_progress", percent=f"{pct:.1f}")

    log_event(logger, "download_done", path=str(dest), bytes=downloaded)


def download_fred_series(series_id: str, start: str, end: Optional[str]) -> Path:
    cfg = load_config()
    logger = setup_logging("pipeline.ingest.fred")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": cfg.fred_api_key,
        "file_type": "json",
        "observation_start": start,
    }
    if end:
        params["observation_end"] = end

    log_event(logger, "fred_request", series=series_id, start=start, end=end or "")
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    df = pd.DataFrame(obs)

    out_dir = _today_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"fred_{series_id}.csv"
    df.to_csv(dest, index=False)
    log_event(logger, "fred_saved", path=str(dest), rows=len(df))
    return dest


def validate_dataframe(df: pd.DataFrame, schema: Optional[dict[str, str]] = None) -> bool:
    logger = setup_logging("pipeline.validate")

    if ge is None:
        log_event(logger, "ge_missing_skip")
        return True

    validator = ge.from_pandas(df)

    # missing values
    for col in df.columns:
        validator.expect_column_values_to_not_be_null(col)

    # schema validation
    if schema:
        for col, expected in schema.items():
            if col in df.columns:
                validator.expect_column_values_to_be_of_type(col, expected)

    # outlier detection based on p01-p99
    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        q1 = df[col].quantile(0.01)
        q99 = df[col].quantile(0.99)
        validator.expect_column_values_to_be_between(col, min_value=q1, max_value=q99)

    # date consistency: no future dates
    today = pd.Timestamp.utcnow().normalize()
    for col in df.columns:
        if "date" in col.lower() or col.lower().endswith("_dt"):
            validator.expect_column_values_to_be_between(col, max_value=today)

    result = validator.validate()
    log_event(logger, "validation_done", success=result["success"])  # type: ignore[index]
    return bool(result["success"])  # type: ignore[index]


def _default_filename(url: str) -> str:
    return url.split("/")[-1] or "download.bin"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--freddie-url", default=None)
    p.add_argument("--fannie-url", default=None)
    p.add_argument("--fred-series", default=None, help="Comma-separated list")
    p.add_argument("--start", default="1990-01-01")
    p.add_argument("--end", default=None)
    p.add_argument("--validate", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config()
    logger = setup_logging("pipeline.ingest")

    out_dir = _today_dir()
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.freddie_url:
        auth = None
        if cfg.freddie.user and cfg.freddie.password:
            auth = (cfg.freddie.user, cfg.freddie.password)
        dest = out_dir / _default_filename(args.freddie_url)
        download_url(args.freddie_url, dest, auth=auth)

    if args.fannie_url:
        auth = None
        if cfg.fannie.user and cfg.fannie.password:
            auth = (cfg.fannie.user, cfg.fannie.password)
        dest = out_dir / _default_filename(args.fannie_url)
        download_url(args.fannie_url, dest, auth=auth)

    if args.fred_series:
        series_ids = [s.strip() for s in args.fred_series.split(",") if s.strip()]
        for sid in series_ids:
            download_fred_series(sid, args.start, args.end)

    if args.validate:
        csvs = list(out_dir.rglob("*.csv"))
        for path in csvs:
            df = pd.read_csv(path)
            ok = validate_dataframe(df)
            log_event(logger, "validation_file", path=str(path), ok=ok)


if __name__ == "__main__":
    main()
