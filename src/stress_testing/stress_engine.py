from __future__ import annotations

import pandas as pd

from src.stress_testing.scenario_generator import generate_scenarios
from src.stress_testing.crisis_simulation import simulate_crisis
from src.utils.logging_setup import setup_logging, log_event


def run_stress_tests() -> pd.DataFrame:
    """Run stress scenarios and return a consolidated report."""
    logger = setup_logging("stress.engine")
    scenarios = generate_scenarios()
    frames = []
    for s in scenarios:
        df = simulate_crisis(s.shock_magnitude)
        df["scenario"] = s.name
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    log_event(logger, "stress_done", rows=len(out))
    return out
