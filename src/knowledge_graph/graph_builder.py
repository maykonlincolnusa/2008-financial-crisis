from __future__ import annotations

from typing import Iterable

try:
    from neo4j import GraphDatabase
except Exception:  # pragma: no cover
    GraphDatabase = None

from src.utils.logging_setup import setup_logging, log_event


def build_graph(uri: str, user: str, password: str, statements: Iterable[str]) -> None:
    """Build a Neo4j knowledge graph using Cypher statements."""
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


def run_queries(uri: str, user: str, password: str, queries: Iterable[str]) -> list[list[dict]]:
    """Run Cypher queries and return results as list of lists of dicts."""
    logger = setup_logging("kg.query_runner")
    if GraphDatabase is None:
        log_event(logger, "neo4j_missing")
        return []

    qlist = list(queries)
    results = []
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session() as session:
        for q in qlist:
            res = session.run(q)
            results.append([dict(r) for r in res])
    driver.close()
    log_event(logger, "queries_run", count=len(qlist))
    return results
