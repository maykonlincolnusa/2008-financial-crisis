from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.macro_shocks.scenario_builder import build_scenarios
from src.simulation.simulation_engine import run_simulation
from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/simulation_results.parquet")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.simulation")

    frames = []
    for scenario in build_scenarios():
        df = run_simulation(scenario.shock_profile, steps=len(scenario.shock_profile))
        df["scenario"] = scenario.name
        frames.append(df)

    out = pd.concat(frames, ignore_index=True)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    log_event(logger, "simulation_saved", path=str(out_path), rows=len(out))


if __name__ == "__main__":
    main()
