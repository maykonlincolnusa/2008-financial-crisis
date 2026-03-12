from __future__ import annotations

import hashlib
import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def _hash_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def mask_pii(df: pd.DataFrame, columns: list[str], method: str = "hash") -> pd.DataFrame:
    logger = setup_logging("security.masking")
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            continue
        if method == "hash":
            out[col] = out[col].astype(str).apply(_hash_value)
        elif method == "redact":
            out[col] = "REDACTED"
        else:
            raise ValueError("Unknown method")

    log_event(logger, "masking_done", columns=",".join(columns), method=method)
    return out
