import pandas as pd

from src.features.risk_features import build_risk_features


def test_risk_features_basic():
    loans = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-02-01"],
            "loan_amount": [100, 200],
            "property_value": [200, 400],
            "debt": [10, 20],
            "income": [100, 200],
            "credit_score": [600, 700],
            "mortgage_rate": [5.0, 4.5],
            "treasury_rate": [2.0, 2.0],
            "default": [0, 1],
            "foreclosure": [0, 0],
            "delinquency": [0, 1],
        }
    )
    macro = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-02-01"],
            "series": ["UNRATE", "UNRATE"],
            "value": [3.5, 3.6],
        }
    )
    hpi = pd.DataFrame(
        {
            "date": ["2020-01-01", "2020-02-01"],
            "value": [100, 102],
        }
    )

    out = build_risk_features(loans, macro, hpi)
    assert "loan_to_value_ratio" in out.columns
    assert "debt_to_income_ratio" in out.columns
    assert "credit_risk_score" in out.columns
    assert "macro_risk_index" in out.columns
    assert "housing_price_trend" in out.columns
    assert "interest_rate_spread" in out.columns
    assert "target_default" in out.columns
