from __future__ import annotations

from dataclasses import dataclass

from src.ai_agents.economic_agents import AgentState, step_agent


@dataclass
class BankAgent:
    state: AgentState

    def decide_lending(self, shock: float) -> float:
        """Decide lending based on risk appetite and shock."""
        self.state = step_agent(self.state, shock)
        return self.state.cash * self.state.risk_appetite
