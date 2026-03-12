from __future__ import annotations

import pandas as pd

from src.ai_agents.bank_agent import BankAgent
from src.ai_agents.regulator_agent import RegulatorAgent
from src.ai_agents.economic_agents import AgentState


def simulate_crisis(shock: float, steps: int = 5) -> pd.DataFrame:
    """Simulate a simple crisis with bank and regulator agents."""
    bank = BankAgent(AgentState(cash=1000.0, risk_appetite=0.5))
    regulator = RegulatorAgent(policy_strength=0.3)

    rows = []
    cur_shock = shock
    for t in range(steps):
        cur_shock = regulator.apply_policy(cur_shock)
        lending = bank.decide_lending(cur_shock)
        rows.append({"step": t, "shock": cur_shock, "lending": lending})
    return pd.DataFrame(rows)
