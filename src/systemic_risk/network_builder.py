from __future__ import annotations

import pandas as pd
import networkx as nx

from src.utils.logging_setup import setup_logging, log_event


def build_financial_network(exposures: pd.DataFrame) -> nx.DiGraph:
    logger = setup_logging("risk.network")
    g = nx.DiGraph()

    required = {"source", "target", "exposure"}
    if not required.issubset(exposures.columns):
        log_event(logger, "missing_columns")
        return g

    for _, row in exposures.iterrows():
        g.add_edge(row["source"], row["target"], weight=float(row["exposure"]))

    log_event(logger, "graph_built", nodes=g.number_of_nodes(), edges=g.number_of_edges())
    return g


def compute_network_metrics(g: nx.DiGraph) -> pd.DataFrame:
    if g.number_of_nodes() == 0:
        return pd.DataFrame()

    degree_c = nx.degree_centrality(g)
    betw_c = nx.betweenness_centrality(g)
    eigen_c = nx.eigenvector_centrality_numpy(g, weight="weight")

    df = pd.DataFrame({
        "institution": list(g.nodes()),
        "degree_centrality": [degree_c[n] for n in g.nodes()],
        "betweenness_centrality": [betw_c[n] for n in g.nodes()],
        "eigenvector_centrality": [eigen_c[n] for n in g.nodes()],
    })
    return df


def compute_exposure_metrics(exposures: pd.DataFrame) -> pd.DataFrame:
    if exposures.empty:
        return pd.DataFrame()
    out = exposures.groupby("source")["exposure"].sum().rename("exposure_out")
    inc = exposures.groupby("target")["exposure"].sum().rename("exposure_in")
    df = pd.concat([out, inc], axis=1).fillna(0).reset_index().rename(columns={"index": "institution"})
    return df


def systemic_risk_score(metrics: pd.DataFrame, exposures: pd.DataFrame) -> float:
    if metrics.empty or exposures.empty:
        return 0.0
    exp = compute_exposure_metrics(exposures)
    merged = metrics.merge(exp, on="institution", how="left").fillna(0)
    score = (
        merged["degree_centrality"] * 0.4
        + merged["betweenness_centrality"] * 0.3
        + (merged["exposure_out"] / (merged["exposure_out"].max() + 1e-9)) * 0.3
    )
    return float(score.mean())
