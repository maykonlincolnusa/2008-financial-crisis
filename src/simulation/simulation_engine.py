from __future__ import annotations

import pandas as pd

from src.simulation.agent_model import AgentState, BalanceSheet
from src.simulation.bank_agent import BankAgent
from src.simulation.household_agent import HouseholdAgent
from src.simulation.market_agent import MarketAgent
from src.utils.logging_setup import setup_logging, log_event


def run_simulation(shock_series: list[float], steps: int = 5) -> pd.DataFrame:
    """Run multi-agent simulation over a shock series."""
    logger = setup_logging("simulation.engine")

    bank = BankAgent(AgentState(BalanceSheet(1000, 800, 200), risk_exposure=0.2, liquidity=200))
    household = HouseholdAgent(AgentState(BalanceSheet(200, 120, 80), risk_exposure=0.1, liquidity=50))
    market = MarketAgent(volatility=0.2)

    rows = []
    for t in range(steps):
        shock = shock_series[min(t, len(shock_series) - 1)]
        bank.step(shock)
        household.step(shock)
        market.step(shock)
        rows.append({
            "step": t,
            "shock": shock,
            "bank_equity": bank.state.balance_sheet.equity,
            "household_equity": household.state.balance_sheet.equity,
            "market_volatility": market.volatility,
        })

    df = pd.DataFrame(rows)
    log_event(logger, "simulation_done", rows=len(df))
    return df
