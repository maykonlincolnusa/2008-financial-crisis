from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Scenario:
    name: str
    shock_magnitude: float


def generate_scenarios() -> list[Scenario]:
    """Generate a list of stress scenarios."""
    return [
        Scenario("bank_failures", 0.3),
        Scenario("liquidity_crisis", 0.25),
        Scenario("credit_crunch", 0.2),
        Scenario("housing_collapse", 0.35),
        Scenario("sovereign_debt", 0.28),
    ]
