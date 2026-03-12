from __future__ import annotations

import argparse
from pathlib import Path

import mlflow

from src.utils.logging_setup import setup_logging, log_event


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model-path", required=True)
    p.add_argument("--name", required=True)
    p.add_argument("--mlflow", default="./mlflow")
    return p.parse_args()


def main():
    args = parse_args()
    logger = setup_logging("mlops.register")

    mlflow.set_tracking_uri(f"file:{Path(args.mlflow).resolve()}")

    with mlflow.start_run():
        mlflow.log_artifact(args.model_path)
        try:
            result = mlflow.register_model(
                f"runs:/{mlflow.active_run().info.run_id}/{Path(args.model_path).name}",
                args.name,
            )
            log_event(logger, "model_registered", name=args.name, version=result.version)
        except Exception as e:
            log_event(logger, "registry_unavailable", error=str(e))


if __name__ == "__main__":
    main()
