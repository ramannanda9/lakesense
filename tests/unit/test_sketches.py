"""Unit tests for lakesense.sketches"""
# ruff: noqa: I001

import pytest

from lakesense.core.result import DriftSignals
from lakesense.sketches.compute import SketchRecord, compute_hll, compute_kll, compute_minhash
from lakesense.sketches.merge import (
    BaselineConfig,
    BaselineStrategy,
    _ewma_weights,
    build_baseline,
    merge_minhash_records,
)
from lakesense.sketches.signals import aggregate_signals, compute_signals


class TestMinHash:
    def test_identical_sets_jaccard_one(self):
        tokens = ["the cat sat", "on the mat"]
        blob1, m1 = compute_minhash(tokens)
        blob2, m2 = compute_minhash(tokens)
        from datasketches import theta_jaccard_similarity

        jaccard = theta_jaccard_similarity.jaccard(m1, m2)[1]
        assert jaccard == pytest.approx(1.0, abs=0.01)

    def test_disjoint_sets_low_jaccard(self):
        blob1, m1 = compute_minhash(["apple pie is great", "banana split dessert"])
        blob2, m2 = compute_minhash(["dog walks in park", "elephant runs fast fox"])
        from datasketches import theta_jaccard_similarity

        jaccard = theta_jaccard_similarity.jaccard(m1, m2)[1]
        assert jaccard < 0.2

    def test_blob_is_bytes(self):
        blob, _ = compute_minhash(["a", "b", "c"])
        assert isinstance(blob, bytes)
        assert len(blob) > 0  # theta sketches compactness varies

    def test_custom_num_perm(self):
        blob, _ = compute_minhash(["x"], num_perm=64)
        assert len(blob) > 0

    def test_word_ngram_more_sensitive_than_whitespace(self):
        # "payment failed" vs "failed payment" — same bag-of-words, different bigrams
        from datasketches import theta_jaccard_similarity

        blob1, m1 = compute_minhash(["payment failed"], tokenizer="word_ngram")
        blob2, m2 = compute_minhash(["failed payment"], tokenizer="word_ngram")
        jaccard_ngram = theta_jaccard_similarity.jaccard(m1, m2)[1]

        blob3, m3 = compute_minhash(["payment failed"], tokenizer="whitespace")
        blob4, m4 = compute_minhash(["failed payment"], tokenizer="whitespace")
        jaccard_whitespace = theta_jaccard_similarity.jaccard(m3, m4)[1]

        # word_ngram should detect more difference (lower jaccard) due to bigrams
        assert jaccard_ngram < jaccard_whitespace

    def test_char_shingle_catches_id_format_drift(self):
        # user_id_123 vs usr_id_123 — structurally similar but char shingles differ
        from datasketches import theta_jaccard_similarity

        ids_v1 = [f"user_id_{i}" for i in range(50)]
        ids_v2 = [f"usr_id_{i}" for i in range(50)]  # prefix changed

        blob1, m1 = compute_minhash(ids_v1, tokenizer="char_shingle")
        blob2, m2 = compute_minhash(ids_v2, tokenizer="char_shingle")
        jaccard = theta_jaccard_similarity.jaccard(m1, m2)[1]
        assert jaccard < 0.95  # detects structural drift (not identical)

    def test_whitespace_tokenizer_legacy_behaviour(self):
        from datasketches import theta_jaccard_similarity

        tokens = ["hello world", "hello world"]
        blob1, m1 = compute_minhash(tokens, tokenizer="whitespace")
        blob2, m2 = compute_minhash(tokens, tokenizer="whitespace")
        jaccard = theta_jaccard_similarity.jaccard(m1, m2)[1]
        assert jaccard == pytest.approx(1.0, abs=0.01)


class TestHyperLogLog:
    def test_cardinality_estimate(self):
        values = [str(i) for i in range(1000)]
        blob, hll = compute_hll(values, p=12)
        assert abs(hll.get_estimate() - 1000) < 50  # within 5%

    def test_blob_is_bytes(self):
        blob, _ = compute_hll(["a", "b"])
        assert isinstance(blob, bytes)


class TestKLL:
    def test_quantiles_present(self):
        values = list(range(100))
        blob, quantiles = compute_kll(values)
        assert "p50" in quantiles
        assert "p99" in quantiles
        assert quantiles["p50"] == pytest.approx(49.5, abs=5)

    def test_blob_is_bytes(self):
        blob, _ = compute_kll([1.0, 2.0, 3.0])
        assert isinstance(blob, bytes)


class TestMerge:
    def _make_record(self, tokens, column="col", sketch_type="minhash", **kwargs):
        blob, _ = compute_minhash(tokens)
        return SketchRecord(
            dataset_id="ds",
            job_id="job",
            column=column,
            sketch_type=sketch_type,
            sketch_blob=blob,
            num_perm=128,
            **kwargs,
        )

    def test_merge_identical_records(self):
        tokens = ["a", "b", "c"]
        r1 = self._make_record(tokens)
        r2 = self._make_record(tokens)
        merged = merge_minhash_records([r1, r2])
        assert isinstance(merged, bytes)

    def test_build_baseline_rolling(self):
        from datetime import datetime, timedelta, timezone

        records = []
        base_ts = datetime.now(timezone.utc) - timedelta(days=3)
        for i in range(5):
            r = self._make_record(
                [f"token{i}", "common"],
                run_ts=base_ts + timedelta(hours=i * 6),
            )
            records.append(r)

        config = BaselineConfig(
            dataset_id="ds",
            strategy=BaselineStrategy.ROLLING_WINDOW,
            window_days=7,
        )
        baseline = build_baseline(records, config, "minhash", "col")
        assert baseline is not None
        assert baseline.source_count == 5

    def test_ewma_weights_sum_to_one(self):
        weights = _ewma_weights(5, decay=0.9)
        assert sum(weights) == pytest.approx(1.0, abs=1e-9)
        assert weights[-1] > weights[0]  # most recent has highest weight


class TestSignals:
    def test_jaccard_delta_identical(self):
        tokens = ["hello", "world"]
        blob, _ = compute_minhash(tokens)
        rec = SketchRecord("ds", "job", "col", "minhash", blob, num_perm=128)

        from datetime import datetime, timezone

        from lakesense.sketches.merge import BaselineSketch, BaselineStrategy

        baseline = BaselineSketch(
            dataset_id="ds",
            column="col",
            sketch_type="minhash",
            sketch_blob=blob,
            source_count=1,
            strategy=BaselineStrategy.ROLLING_WINDOW,
            merged_at=datetime.now(timezone.utc),
        )
        signals = compute_signals(rec, baseline)
        assert signals.jaccard_delta == pytest.approx(0.0, abs=0.05)

    def test_aggregate_takes_worst_jaccard(self):
        s1 = DriftSignals(jaccard_delta=-0.1)
        s2 = DriftSignals(jaccard_delta=-0.5)
        s3 = DriftSignals(jaccard_delta=-0.2)
        agg = aggregate_signals({"col_a": s1, "col_b": s2, "col_c": s3})
        assert agg.jaccard_delta == -0.5
        assert agg.jaccard_worst_column == "col_b"

    def test_ks_distribution_shift(self):
        import random

        from lakesense.sketches.compute import compute_kll

        # 1. Base distribution: Normal(0, 1)
        base_vals = [random.normalvariate(0, 1) for _ in range(1000)]
        base_blob, _ = compute_kll(base_vals)

        # 2. Current distribution (Divergent): Normal(10, 1)
        cur_vals = [random.normalvariate(10, 1) for _ in range(1000)]
        cur_blob, _ = compute_kll(cur_vals)

        # 3. Current distribution (Identical): Normal(0, 1)
        cur_vals2 = [random.normalvariate(0, 1) for _ in range(1000)]
        cur_blob2, _ = compute_kll(cur_vals2)

        from datetime import datetime, timezone

        from lakesense.sketches.merge import BaselineSketch, BaselineStrategy

        baseline = BaselineSketch(
            dataset_id="ds",
            column="col",
            sketch_type="kll",
            sketch_blob=base_blob,
            source_count=1,
            strategy=BaselineStrategy.ROLLING_WINDOW,
            merged_at=datetime.now(timezone.utc),
        )

        rec_divergent = SketchRecord("ds", "job", "col", "kll", cur_blob)
        sig_divergent = compute_signals(rec_divergent, baseline)
        assert sig_divergent.ks_test_divergent is True

        rec_identical = SketchRecord("ds", "job", "col", "kll", cur_blob2)
        sig_identical = compute_signals(rec_identical, baseline)
        assert sig_identical.ks_test_divergent is False


class TestStreamingInputs:
    """
    compute_* accept any Iterable, and StreamingProvider documents "strictly O(1)
    memory" over file-backed generators. These pin both halves of that contract:
    generators must produce the same sketches as materialized input, and must not
    make peak memory grow with row count.
    """

    def test_generator_matches_list_kll(self):
        vals = [float(i % 977) for i in range(50_000)]
        _, from_list = compute_kll(vals)
        _, from_gen = compute_kll(v for v in vals)
        for key in ("mean", "std", "min", "max"):
            assert from_gen[key] == pytest.approx(from_list[key], rel=1e-9)

    def test_generator_matches_list_minhash(self):
        vals = [f"row {i % 300} text" for i in range(50_000)]
        for tokenizer in ("word_ngram", "char_shingle", "whitespace"):
            _, from_list = compute_minhash(vals, tokenizer=tokenizer)
            _, from_gen = compute_minhash((v for v in vals), tokenizer=tokenizer)
            assert from_gen.get_estimate() == pytest.approx(from_list.get_estimate(), rel=1e-9)

    def test_kll_moments_match_numpy_across_block_boundary(self):
        # _iter_float_blocks chunks one-shot iterables; the parallel-variance
        # combination across those blocks must match a single-pass computation.
        np = pytest.importorskip("numpy")
        arr = np.random.default_rng(0).normal(50, 12, 200_000)  # spans several blocks
        _, q = compute_kll(v for v in arr)
        assert q["mean"] == pytest.approx(float(arr.mean()), rel=1e-9)
        assert q["std"] == pytest.approx(float(arr.std()), rel=1e-9)

    def test_nans_and_nones_are_dropped(self):
        _, q = compute_kll(iter([1.0, None, 2.0, float("nan"), 3.0]))
        assert q["mean"] == pytest.approx(2.0)
        assert q["min"] == pytest.approx(1.0)
        assert q["max"] == pytest.approx(3.0)

    @pytest.mark.parametrize("n_rows", [500_000, 2_000_000])
    def test_peak_memory_does_not_scale_with_rows(self, n_rows):
        # Regression guard: materializing the input made these O(n) — 2M distinct
        # strings peaked at 210 MB. The bound is deliberately loose so this fails
        # only on a real return to O(n), not on allocator noise.
        import tracemalloc

        tracemalloc.start()
        compute_kll(float(i % 1000) for i in range(n_rows))
        _, kll_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        tracemalloc.start()
        compute_minhash(f"event {i} distinct" for i in range(n_rows))
        _, minhash_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert kll_peak < 32 * 1024 * 1024, f"kll peak {kll_peak / 1e6:.1f} MB scales with rows"
        assert minhash_peak < 32 * 1024 * 1024, f"minhash peak {minhash_peak / 1e6:.1f} MB scales with rows"


class TestStreamingProvider:
    """StreamingProvider had no coverage at all; these are its first tests."""

    def test_sketches_from_generators(self):
        from lakesense.sketches.providers.streaming import StreamingProvider

        n = 5_000
        data = {
            "user_id": (f"u_{i}" for i in range(n)),
            "amount": (float(i % 100) for i in range(n)),
            "note": (f"note {i % 20} body" for i in range(n)),
        }
        records = StreamingProvider().sketch(
            data=data,
            dataset_id="ds",
            job_id="job",
            id_columns=["user_id"],
            numeric_columns=["amount"],
            text_columns=["note"],
            include_profiles=False,
        )
        by_type = {r.sketch_type for r in records}
        assert by_type == {"hll", "kll", "minhash"}
        assert all(isinstance(r.sketch_blob, bytes) and r.sketch_blob for r in records)
