from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def extract_relationships(df: pd.DataFrame, source: str, target: str, rel: str) -> list[str]:
    """Extract relationships and return Cypher statements."""
    logger = setup_logging("kg.relationships")
    stmts = []
    for _, row in df.iterrows():
        stmt = (
            f"MERGE (a:Entity {{name: '{row[source]}'}}) "
            f"MERGE (b:Entity {{name: '{row[target]}'}}) "
            f"MERGE (a)-[:{rel}]->(b)"
        )
        stmts.append(stmt)
    log_event(logger, "relationships_extracted", count=len(stmts))
    return stmts
