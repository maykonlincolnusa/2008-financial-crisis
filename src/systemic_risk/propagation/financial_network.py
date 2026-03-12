from __future__ import annotations

import pandas as pd
import networkx as nx

from src.utils.logging_setup import setup_logging, log_event


def build_financial_network(exposures: pd.DataFrame) -> nx.DiGraph:
    """Build a directed financial exposure network."""
    logger = setup_logging("systemic.financial_network")
    g = nx.DiGraph()
    for _, row in exposures.iterrows():
        g.add_edge(row["source"], row["target"], weight=float(row["exposure"]))
    log_event(logger, "network_built", nodes=g.number_of_nodes(), edges=g.number_of_edges())
    return g
