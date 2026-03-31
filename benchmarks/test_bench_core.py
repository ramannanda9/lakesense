"""
Performance benchmarks for lakesense hot paths.

Run with:
    pytest benchmarks/ --benchmark-only -v
    pytest benchmarks/ --benchmark-only --benchmark-sort=fullname
    pytest benchmarks/ --benchmark-only --benchmark-compare

These target the CPU-bound paths that run on every job:
    1. Sketch compute (MinHash, HLL, KLL) — per-column cost
    2. Sketch merge (baseline building) — scales with window size
    3. Column profiling — per-dataframe cost
    4. Parquet storage I/O — write + read round-trip
"""

import random
import string

import numpy as np
import pandas as pd
import pytest

from lakesense.sketches.compute import (
    SketchRecord,
    compute_hll,
    compute_kll,
    compute_minhash,
)
from lakesense.sketches.merge import (
    BaselineConfig,
    BaselineStrategy,
    build_baseline,
    merge_hll_records,
    merge_minhash_records,
)
from lakesense.sketches.profile import profile_dataframe
from lakesense.sketches.providers.pandas import PandasProvider
from lakesense.storage.parquet import ParquetBackend

# ---------------------------------------------------------------------------
# Fixtures: synthetic data at various scales
# ---------------------------------------------------------------------------


def _random_strings(n: int, length: int = 8) -> list[str]:
    """Generate n random strings."""
    return ["".join(random.choices(string.ascii_lowercase, k=length)) for _ in range(n)]


def _random_text(n: int, words_per_row: int = 10) -> list[str]:
    """Generate n rows of random text (multiple words per row)."""
    vocab = _random_strings(500, length=6)
    return [" ".join(random.choices(vocab, k=words_per_row)) for _ in range(n)]


def _make_dataframe(n_rows: int) -> pd.DataFrame:
    """Create a synthetic dataframe with mixed column types."""
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "user_id": [f"user_{i}" for i in range(n_rows)],
            "session_count": rng.integers(0, 1000, size=n_rows),
            "revenue": rng.normal(100.0, 25.0, size=n_rows),
            "description": _random_text(n_rows),
            "category": rng.choice(["A", "B", "C", "D", "E"], size=n_rows),
            "is_active": rng.choice([True, False], size=n_rows),
        }
    )


def _make_sketch_records(n_records: int, sketch_type: str = "minhash") -> list[SketchRecord]:
    """Create n sketch records for merge benchmarks."""
    records = []
    for i in range(n_records):
        if sketch_type == "minhash":
            blob, _ = compute_minhash(_random_strings(1000))
        elif sketch_type == "hll":
            blob, _ = compute_hll(_random_strings(1000))
        else:
            blob, _ = compute_kll(np.random.default_rng(i).normal(100, 25, 1000).tolist())
        records.append(
            SketchRecord(
                dataset_id="bench_ds",
                job_id=f"job_{i}",
                column="col",
                sketch_type=sketch_type,
                sketch_blob=blob,
                num_perm=128 if sketch_type == "minhash" else None,
            )
        )
    return records


# ---------------------------------------------------------------------------
# 1. Sketch Compute Benchmarks
# ---------------------------------------------------------------------------


class TestSketchCompute:
    """Benchmark individual sketch computations at various row counts."""

    @pytest.mark.parametrize("n_values", [1_000, 10_000, 100_000])
    def test_compute_minhash(self, benchmark, n_values):
        values = _random_strings(n_values)
        benchmark(compute_minhash, values)

    @pytest.mark.parametrize("n_values", [1_000, 10_000, 100_000])
    def test_compute_hll(self, benchmark, n_values):
        values = _random_strings(n_values)
        benchmark(compute_hll, values)

    @pytest.mark.parametrize("n_values", [1_000, 10_000, 100_000])
    def test_compute_kll(self, benchmark, n_values):
        values = np.random.default_rng(42).normal(100, 25, n_values).tolist()
        benchmark(compute_kll, values)


# ---------------------------------------------------------------------------
# 2. Sketch Merge Benchmarks
# ---------------------------------------------------------------------------


class TestSketchMerge:
    """Benchmark merge operations scaling with number of source sketches."""

    @pytest.mark.parametrize("n_records", [7, 30, 90])
    def test_merge_minhash(self, benchmark, n_records):
        records = _make_sketch_records(n_records, "minhash")
        benchmark(merge_minhash_records, records)

    @pytest.mark.parametrize("n_records", [7, 30, 90])
    def test_merge_hll(self, benchmark, n_records):
        records = _make_sketch_records(n_records, "hll")
        benchmark(merge_hll_records, records)

    @pytest.mark.parametrize("n_records", [7, 30, 90])
    def test_build_baseline_rolling(self, benchmark, n_records):
        records = _make_sketch_records(n_records, "minhash")
        config = BaselineConfig(
            dataset_id="bench_ds",
            strategy=BaselineStrategy.ROLLING_WINDOW,
            window_days=365,  # large window to include all records
        )
        benchmark(build_baseline, records, config, "minhash", "col")


# ---------------------------------------------------------------------------
# 3. Column Profiling Benchmarks
# ---------------------------------------------------------------------------


class TestProfiling:
    """Benchmark column profiling at various dataframe sizes."""

    @pytest.mark.parametrize("n_rows", [1_000, 10_000, 100_000])
    def test_profile_dataframe(self, benchmark, n_rows):
        df = _make_dataframe(n_rows)
        benchmark(profile_dataframe, df)


# ---------------------------------------------------------------------------
# 4. End-to-end Provider Benchmark
# ---------------------------------------------------------------------------


class TestProviderE2E:
    """Benchmark the full PandasProvider.sketch() pipeline."""

    @pytest.mark.parametrize("n_rows", [1_000, 10_000])
    def test_pandas_provider_sketch(self, benchmark, n_rows):
        df = _make_dataframe(n_rows)
        provider = PandasProvider()
        benchmark(
            provider.sketch,
            data=df,
            dataset_id="bench_ds",
            job_id="bench_job",
            text_columns=["description"],
            id_columns=["user_id"],
            numeric_columns=["session_count", "revenue"],
        )


# ---------------------------------------------------------------------------
# 5. Storage I/O Benchmarks
# ---------------------------------------------------------------------------


class TestStorageIO:
    """Benchmark Parquet write/read round-trips."""

    @pytest.fixture
    def backend(self, tmp_path):
        return ParquetBackend(tmp_path)

    @pytest.mark.parametrize("n_records", [10, 100, 500])
    def test_write_sketches(self, benchmark, backend, n_records):
        records = _make_sketch_records(n_records, "minhash")
        import asyncio

        def _write():
            asyncio.run(backend.write_sketches(records))

        benchmark(_write)

    @pytest.mark.parametrize("n_records", [10, 100, 500])
    def test_read_sketches(self, benchmark, backend, n_records):
        records = _make_sketch_records(n_records, "minhash")
        import asyncio

        asyncio.run(backend.write_sketches(records))

        from_ts = records[0].run_ts.replace(hour=0, minute=0, second=0)

        def _read():
            asyncio.run(backend.read_sketches("bench_ds", after_ts=from_ts))

        benchmark(_read)
