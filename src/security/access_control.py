from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

from src.utils.logging_setup import setup_logging, log_event


@dataclass(frozen=True)
class AccessContext:
    user_role: str


def load_secure_config(path: Optional[str] = None) -> dict:
    logger = setup_logging("security.config")
    cfg = {"FREDDIE_USER": os.getenv("FREDDIE_USER"), "FANNIE_USER": os.getenv("FANNIE_USER")}

    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            file_cfg = json.load(f)
        cfg.update(file_cfg)

    log_event(logger, "config_loaded", has_file=bool(path))
    return cfg


def check_access(ctx: AccessContext, required_role: str) -> bool:
    logger = setup_logging("security.access")
    ok = ctx.user_role == required_role
    log_event(logger, "access_check", role=ctx.user_role, required=required_role, ok=ok)
    return ok
