from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BalanceSheet:
    assets: float
    liabilities: float
    equity: float


@dataclass
class AgentState:
    balance_sheet: BalanceSheet
    risk_exposure: float
    liquidity: float


def apply_shock(state: AgentState, shock: float) -> AgentState:
    """Apply a shock to balance sheet and liquidity."""
    assets = max(0.0, state.balance_sheet.assets * (1 - shock))
    equity = max(0.0, assets - state.balance_sheet.liabilities)
    return AgentState(
        balance_sheet=BalanceSheet(assets=assets, liabilities=state.balance_sheet.liabilities, equity=equity),
        risk_exposure=state.risk_exposure + shock,
        liquidity=max(0.0, state.liquidity * (1 - shock)),
    )
