import pandas as pd
from src.etl.transform_loans import _normalize


def test_normalize_dates():
    df = pd.DataFrame({"orig_date": ["2020-01-01", "2020-02-01"]})
    out = _normalize(df)
    assert pd.api.types.is_datetime64_any_dtype(out["orig_date"])
