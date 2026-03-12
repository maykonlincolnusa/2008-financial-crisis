from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PolicyAgent:
    rate_cut: float

    def enact(self) -> float:
        """Return policy rate change."""
        return -abs(self.rate_cut)
