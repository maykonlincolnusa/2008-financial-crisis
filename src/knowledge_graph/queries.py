from __future__ import annotations


def q_systemic_exposure() -> str:
    """Return Cypher query to compute systemic exposure."""
    return """
    MATCH (a:Entity)-[r:EXPOSURE]->(b:Entity)
    RETURN a.name AS source, b.name AS target, r.amount AS exposure
    """


def q_institution_dependencies() -> str:
    """Return Cypher query to compute institution dependencies."""
    return """
    MATCH (a:Entity)-[:EXPOSURE]->(b:Entity)
    RETURN a.name AS institution, collect(b.name) AS dependencies
    """


def q_contagion_paths() -> str:
    """Return Cypher query for contagion paths up to length 3."""
    return """
    MATCH p=(a:Entity)-[:EXPOSURE*1..3]->(b:Entity)
    RETURN a.name AS source, b.name AS target, length(p) AS hops
    """
