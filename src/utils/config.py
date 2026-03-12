from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STAGING_DIR = DATA_DIR / "staging"
PROCESSED_DIR = DATA_DIR / "processed"


@dataclass(frozen=True)
class Credentials:
    user: str | None
    password: str | None


@dataclass(frozen=True)
class AppConfig:
    freddie: Credentials
    fannie: Credentials
    fred_api_key: str | None


def load_config() -> AppConfig:
    return AppConfig(
        freddie=Credentials(
            user=os.getenv("FREDDIE_USER"),
            password=os.getenv("FREDDIE_PASS"),
        ),
        fannie=Credentials(
            user=os.getenv("FANNIE_USER"),
            password=os.getenv("FANNIE_PASS"),
        ),
        fred_api_key=os.getenv("FRED_API_KEY"),
    )
