import pandas as pd
import pytest

from src.models.model_xgboost import train_xgboost
from src.models.model_lightgbm import train_lightgbm
from src.models.model_deep_nn import train_deep_nn
from src.models.model_survival import train_survival


def _sample_df():
    return pd.DataFrame(
        {
            "f1": [0, 1, 0, 1, 0, 1, 0, 1],
            "f2": [1, 2, 1, 2, 1, 2, 1, 2],
            "target_default": [0, 1, 0, 1, 0, 1, 0, 1],
            "time_to_event": [5, 4, 6, 3, 7, 2, 8, 1],
            "event": [0, 1, 0, 1, 0, 1, 0, 1],
        }
    )


def test_xgboost_train(tmp_path):
    pytest.importorskip("xgboost")
    df = _sample_df()
    out = train_xgboost(df, "target_default", tmp_path, str(tmp_path / "mlflow"))
    assert out is not None
    assert out.exists()


def test_lightgbm_train(tmp_path):
    pytest.importorskip("lightgbm")
    df = _sample_df()
    out = train_lightgbm(df, "target_default", tmp_path, str(tmp_path / "mlflow"))
    assert out is not None
    assert out.exists()


def test_deep_nn_train(tmp_path):
    pytest.importorskip("torch")
    df = _sample_df()
    out = train_deep_nn(df, "target_default", tmp_path, str(tmp_path / "mlflow"))
    assert out is not None
    assert out.exists()


def test_survival_train(tmp_path):
    pytest.importorskip("lifelines")
    df = _sample_df()
    out = train_survival(df, "time_to_event", "event", tmp_path, str(tmp_path / "mlflow"))
    assert out is not None
    assert out.exists()
