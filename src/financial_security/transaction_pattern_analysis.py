from __future__ import annotations

import pandas as pd
import networkx as nx

from src.utils.logging_setup import setup_logging, log_event


def graph_anomaly_detection(transactions: pd.DataFrame) -> pd.DataFrame:
    """Detect anomalous nodes using degree-based outliers in a transaction graph."""
    logger = setup_logging("security.graph_anomaly")
    required = {"source", "target", "amount"}
    if not required.issubset(transactions.columns):
        return pd.DataFrame()

    g = nx.DiGraph()
    for _, row in transactions.iterrows():
        g.add_edge(row["source"], row["target"], weight=float(row["amount"]))

    degree = dict(g.degree())
    deg_values = pd.Series(degree)
    threshold = deg_values.quantile(0.98)
    anomalies = deg_values[deg_values >= threshold].reset_index()
    anomalies.columns = ["entity", "degree"]
    log_event(logger, "graph_anomaly_done", count=len(anomalies))
    return anomalies
