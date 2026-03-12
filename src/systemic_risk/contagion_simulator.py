from __future__ import annotations

import networkx as nx

from src.utils.logging_setup import setup_logging, log_event


def simulate_contagion(g: nx.DiGraph, initial_failed: list[str], threshold: float = 0.3, max_steps: int = 10):
    logger = setup_logging("risk.contagion")

    failed = set(initial_failed)
    history = [set(failed)]

    for _ in range(max_steps):
        new_failed = set(failed)
        for node in g.nodes():
            if node in failed:
                continue
            incoming = 0.0
            total = 0.0
            for u in g.predecessors(node):
                w = g[u][node].get("weight", 0.0)
                total += w
                if u in failed:
                    incoming += w
            if total > 0 and (incoming / total) >= threshold:
                new_failed.add(node)

        if new_failed == failed:
            break
        failed = new_failed
        history.append(set(failed))

    log_event(logger, "contagion_done", steps=len(history))
    return history
