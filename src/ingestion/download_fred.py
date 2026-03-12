from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd
import requests

from src.utils.config import RAW_DIR, load_config
from src.utils.logging_setup import setup_logging


def _today_dir() -> Path:
    return RAW_DIR / dt.datetime.utcnow().strftime("%Y%m%d")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--series", required=True, help="ID da s?rie FRED")
    p.add_argument("--start", default="1990-01-01", help="Data in?cio YYYY-MM-DD")
    p.add_argument("--end", default=None, help="Data fim YYYY-MM-DD")
    p.add_argument("--out", default=None, help="Nome do arquivo de sa?da")
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config()
    logger = setup_logging("fred")

    if not cfg.fred_api_key:
        logger.warning("FRED_API_KEY n?o definido; usando endpoint sem autentica??o (pode limitar).")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": args.series,
        "api_key": cfg.fred_api_key,
        "file_type": "json",
        "observation_start": args.start,
    }
    if args.end:
        params["observation_end"] = args.end

    logger.info("Baixando s?rie %s", args.series)
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    obs = data.get("observations", [])

    df = pd.DataFrame(obs)
    out_dir = _today_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = args.out or f"fred_{args.series}.csv"
    dest = out_dir / filename
    df.to_csv(dest, index=False)
    logger.info("Salvo em %s", dest)


if __name__ == "__main__":
    main()
