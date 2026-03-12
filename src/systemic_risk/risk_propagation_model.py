from __future__ import annotations

import numpy as np
import networkx as nx

from src.utils.logging_setup import setup_logging, log_event


def propagate_risk(g: nx.DiGraph, initial_risk: dict, steps: int = 5, decay: float = 0.85):
    logger = setup_logging("risk.propagation")
    nodes = list(g.nodes())
    idx = {n: i for i, n in enumerate(nodes)}

    if not nodes:
        return {}

    A = np.zeros((len(nodes), len(nodes)))
    for u, v, data in g.edges(data=True):
        A[idx[u], idx[v]] = data.get("weight", 0.0)

    row_sum = A.sum(axis=1, keepdims=True) + 1e-9
    A = A / row_sum

    r = np.zeros(len(nodes))
    for n, val in initial_risk.items():
        if n in idx:
            r[idx[n]] = val

    for _ in range(steps):
        r = decay * (A.T @ r) + (1 - decay) * r

    result = {n: float(r[idx[n]]) for n in nodes}
    log_event(logger, "propagation_done", steps=steps)
    return result
