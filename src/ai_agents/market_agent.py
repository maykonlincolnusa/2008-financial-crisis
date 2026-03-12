from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MarketAgent:
    volatility: float

    def react(self, shock: float) -> float:
        """Market reacts to shocks by adjusting volatility."""
        return self.volatility * (1 + shock)
