from __future__ import annotations

import networkx as nx
import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def simulate_contagion(g: nx.DiGraph, threshold: float = 0.3) -> pd.DataFrame:
    """Simulate contagion based on exposure thresholds."""
    logger = setup_logging("systemic.contagion")
    results = []
    for node in g.nodes():
        exposure = sum(g[node][nbr].get("weight", 0.0) for nbr in g.successors(node))
        fragile = int(exposure >= threshold)
        results.append({"institution": node, "fragile": fragile})
    df = pd.DataFrame(results)
    log_event(logger, "contagion_done", rows=len(df))
    return df
