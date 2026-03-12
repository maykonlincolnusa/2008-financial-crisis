from __future__ import annotations

import pandas as pd

from src.utils.logging_setup import setup_logging, log_event


def map_relationships(df: pd.DataFrame, source: str, target: str, rel: str) -> list[str]:
    """Create Cypher MERGE statements to map relationships."""
    logger = setup_logging("kg.relationship")
    stmts = []
    for _, row in df.iterrows():
        stmt = (
            f"MERGE (a:Entity {{name: '{row[source]}'}}) "
            f"MERGE (b:Entity {{name: '{row[target]}'}}) "
            f"MERGE (a)-[:{rel}]->(b)"
        )
        stmts.append(stmt)
    log_event(logger, "relationships_mapped", count=len(stmts))
    return stmts
