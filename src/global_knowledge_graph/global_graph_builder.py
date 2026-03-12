from __future__ import annotations

from typing import Iterable

try:
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover
    GraphDatabase = None

from src.utils.logging_setup import setup_logging, log_event


def build_graph(uri: str, user: str, password: str, statements: Iterable[str]) -> None:
    """Execute Cypher statements to build a knowledge graph in Neo4j."""
    logger = setup_logging("kg.builder")
    if GraphDatabase is None:
        log_event(logger, "neo4j_missing")
        return

    stmts = list(statements)
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        for stmt in stmts:
            session.run(stmt)
    driver.close()
    log_event(logger, "graph_built", statements=len(stmts))
