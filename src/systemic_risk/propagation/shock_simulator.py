from __future__ import annotations

import networkx as nx

from src.utils.logging_setup import setup_logging, log_event


def propagate_shock(g: nx.DiGraph, initial_shock: dict, decay: float = 0.9, steps: int = 5) -> dict:
    """Propagate shocks across a network with decay."""
    logger = setup_logging("systemic.shock")
    risk = dict(initial_shock)
    for _ in range(steps):
        new_risk = dict(risk)
        for u, v, data in g.edges(data=True):
            w = data.get("weight", 0.0)
            new_risk[v] = new_risk.get(v, 0) + risk.get(u, 0) * w * decay
        risk = new_risk
    log_event(logger, "shock_done", steps=steps)
    return risk
