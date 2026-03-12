from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MarketAgent:
    volatility: float

    def step(self, shock: float) -> None:
        """Adjust volatility based on shocks."""
        self.volatility = self.volatility * (1 + shock)
