from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from src.utils.logging_setup import setup_logging, log_event


def write_audit_log(event: str, actor: str, details: dict, out: str = "data/governance/audit.json") -> None:
    """Write audit log entry to file."""
    logger = setup_logging("governance.audit")
    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)

    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "event": event,
        "actor": actor,
        "details": details,
    }

    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        data = []
    data.append(entry)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    log_event(logger, "audit_logged", event=event)
