from __future__ import annotations

from dataclasses import dataclass

from src.utils.logging_setup import setup_logging, log_event


@dataclass
class AgentState:
    cash: float
    risk_appetite: float


def step_agent(state: AgentState, shock: float) -> AgentState:
    """Generic agent step responding to a shock."""
    logger = setup_logging("agents.base")
    new_cash = max(0.0, state.cash * (1 - shock * (1 - state.risk_appetite)))
    new_state = AgentState(cash=new_cash, risk_appetite=state.risk_appetite)
    log_event(logger, "agent_step", cash=new_cash)
    return new_state
