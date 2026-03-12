import pandas as pd

from src.pipeline.pipeline_ingest import validate_dataframe
from src.pipeline.pipeline_transform import main as transform_main


def test_validate_dataframe_basic():
    df = pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})
    ok = validate_dataframe(df)
    assert ok is True


def test_pipeline_transform(tmp_path):
    raw = tmp_path / "raw"
    out = tmp_path / "staging"
    raw.mkdir(parents=True)

    df = pd.DataFrame({"date": ["2020-01-01"], "value": [1.0]})
    df.to_csv(raw / "sample.csv", index=False)

    import sys
    old = sys.argv
    try:
        sys.argv = ["pipeline_transform", "--input", str(raw), "--output", str(out)]
        transform_main()
    finally:
        sys.argv = old

    assert (out / "loans_parquet").exists()
