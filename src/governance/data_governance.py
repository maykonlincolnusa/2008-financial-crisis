from __future__ import annotations

import json
from pathlib import Path

from src.utils.logging_setup import setup_logging, log_event


def record_lineage(dataset: str, sources: list[str], out: str = "data/governance/lineage.json") -> None:
    """Record dataset lineage information."""
    logger = setup_logging("governance.lineage")
    path = Path(out)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
    else:
        data = {}
    data[dataset] = {"sources": sources}
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    log_event(logger, "lineage_recorded", dataset=dataset)
