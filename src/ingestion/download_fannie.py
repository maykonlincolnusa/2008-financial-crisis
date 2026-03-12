from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Optional

import requests

from src.utils.config import RAW_DIR, load_config
from src.utils.logging_setup import setup_logging


def _today_dir() -> Path:
    return RAW_DIR / dt.datetime.utcnow().strftime("%Y%m%d")


def _download(url: str, dest: Path, auth: Optional[tuple[str, str]] = None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger = setup_logging("ingestion")
    logger.info("Downloading %s", url)

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
                    logger.info("Progress: %.1f%%", pct)
    logger.info("Saved to %s", dest)


def _default_filename(url: str) -> str:
    return url.split("/")[-1] or "download.bin"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True, help="URL do arquivo a baixar")
    p.add_argument("--out", default=None, help="Nome do arquivo de sa?da")
    return p.parse_args()
def main():
    args = parse_args()
    cfg = load_config()

    auth = None
    if cfg.fannie.user and cfg.fannie.password:
        auth = (cfg.fannie.user, cfg.fannie.password)

    out_dir = _today_dir()
    filename = args.out or _default_filename(args.url)
    dest = out_dir / filename
    _download(args.url, dest, auth=auth)


if __name__ == "__main__":
    main()
