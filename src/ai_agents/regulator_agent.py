from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RegulatorAgent:
    policy_strength: float

    def apply_policy(self, shock: float) -> float:
        """Apply macro-prudential policy to dampen shocks."""
        return shock * (1 - self.policy_strength)
