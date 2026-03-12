from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MacroScenario:
    name: str
    shock_profile: list[float]


def build_scenarios() -> list[MacroScenario]:
    """Build a set of macro shock scenarios."""
    return [
        MacroScenario("housing_crash", [0.3, 0.25, 0.2, 0.15, 0.1]),
        MacroScenario("rate_spike", [0.2, 0.2, 0.15, 0.1, 0.05]),
        MacroScenario("liquidity_freeze", [0.25, 0.25, 0.2, 0.15, 0.1]),
        MacroScenario("global_recession", [0.35, 0.3, 0.25, 0.2, 0.15]),
    ]
