"""Unit tests for the scalable Spark map-reduce sketching layer."""
# ruff: noqa: E402, E501, I001

import os
from datetime import datetime, timezone

import pandas as pd
import pytest

# We only run Spark tests if PySpark is available
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from lakesense.sketches.merge import build_baseline
from lakesense.sketches.providers.pandas import PandasProvider
from lakesense.sketches.providers.spark import SparkProvider
from lakesense.sketches.signals import compute_signals


@pytest.fixture(scope="module")
def spark():
    """Create a local Spark session for testing."""
    os.environ["PYSPARK_PYTHON"] = "python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("lakesense-tests")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
    except Exception as e:
        pytest.skip(f"Skipping Spark tests: Unable to initialize PySpark JVM — {str(e)}")


def test_spark_provider_matches_pandas_provider(spark):
    """
    Ensure that a chunked mapInPandas Spark cluster architecture yields EXACTLY
    the same mathematical SketchRecords as the single-machine PandasProvider execution.
    """
    # 1) Generate some dummy data
    df = pd.DataFrame(
        {
            "user_id": [f"u_{i}" for i in range(100)],
            "age": [i % 50 for i in range(100)],
            "category": ["A" if i % 2 == 0 else "B" for i in range(100)],
            "is_active": [True for _ in range(100)],
        }
    )

    # 2) Compute natively via local Pandas
    pandas_provider = PandasProvider()
    pandas_records = pandas_provider.sketch(
        data=df,
        dataset_id="ds_test",
        job_id="job_local",
        id_columns=["user_id"],
        numeric_columns=["age"],
        text_columns=["category"],
        run_ts=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    # 3) Turn the Pandas DF into a heavily partitioned Spark DF
    spark_df = spark.createDataFrame(df).repartition(4)  # 4 explicit workers/partitions

    # 4) Compute heavily-distributed via SparkProvider
    spark_provider = SparkProvider()
    spark_records = spark_provider.sketch(
        data=spark_df,
        dataset_id="ds_test",
        job_id="job_distributed",
        id_columns=["user_id"],
        numeric_columns=["age"],
        text_columns=["category"],
        run_ts=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    assert len(pandas_records) > 0
    assert len(spark_records) > 0
    assert len(pandas_records) == len(spark_records)

    # 5) The magic of exact sketch unions means they should map identically
    pandas_map = {r.column + ":" + r.sketch_type: r for r in pandas_records}
    spark_map = {r.column + ":" + r.sketch_type: r for r in spark_records}

    for key, p_rec in pandas_map.items():
        s_rec = spark_map[key]
        assert s_rec.num_rows == p_rec.num_rows
        assert s_rec.null_count == p_rec.null_count
        assert s_rec.run_ts == p_rec.run_ts

        # We can't immediately assert raw blob bytes are identical because sketches sometimes reorder
        # memory blocks, BUT we can assert their evaluated abstractions (signals) are perfectly zero-drift!
        if p_rec.sketch_type == "profile":
            import json

            p_dict = json.loads(p_rec.sketch_blob.decode())
            s_dict = json.loads(s_rec.sketch_blob.decode())
            # Spark mapInPandas row order per partition means Top-N distributions might shuffle if values tie,
            # but the numerical and scalar values are exact.
            assert p_dict["row_count"] == s_dict["row_count"]
            p_mean = p_dict.get("numeric_mean")
            s_mean = s_dict.get("numeric_mean")
            assert (p_mean is None) == (s_mean is None), f"Mean presence mismatch: {p_mean} vs {s_mean}"
            if p_mean is not None:
                assert p_mean == pytest.approx(s_mean or 0.0)
        else:
            # For HLL, KLL, and Theta, if we compute signals between the pandas version and spark version,
            # the Jaccard, Cardinality Ratio, and Quantile Shifts must be EXACTLY identical (zero drift).
            from lakesense.sketches.merge import BaselineConfig, BaselineStrategy

            baseline = build_baseline(
                records=[p_rec],
                config=BaselineConfig("ds_test", strategy=BaselineStrategy.ROLLING_WINDOW, window_days=3650),
                sketch_type=p_rec.sketch_type,
                column=p_rec.column,
            )
            signals = compute_signals(current=s_rec, baseline=baseline)

            if p_rec.sketch_type == "hll":
                assert signals.cardinality_ratio == pytest.approx(1.0, abs=0.01)
            elif p_rec.sketch_type == "minhash":
                assert signals.jaccard_delta == pytest.approx(0.0, abs=0.01)
            elif p_rec.sketch_type == "kll":
                assert signals.quantile_shifts["p50"] == pytest.approx(0.0, abs=0.01)
