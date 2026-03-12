from __future__ import annotations

from dataclasses import dataclass

from src.simulation.agent_model import AgentState, apply_shock


@dataclass
class BankAgent:
    state: AgentState

    def step(self, shock: float) -> None:
        """Update bank state for a simulation step."""
        self.state = apply_shock(self.state, shock)
