"""Unit tests for lakesense.sketches"""

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
        tokens = ["the", "cat", "sat", "on", "the", "mat"]
        blob1, m1 = compute_minhash(tokens)
        blob2, m2 = compute_minhash(tokens)
        from datasketches import theta_jaccard_similarity

        jaccard = theta_jaccard_similarity.jaccard(m1, m2)[1]
        assert jaccard == pytest.approx(1.0, abs=0.01)

    def test_disjoint_sets_low_jaccard(self):
        blob1, m1 = compute_minhash(["apple", "banana", "cherry"])
        blob2, m2 = compute_minhash(["dog", "elephant", "fox"])
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
        agg = aggregate_signals([s1, s2, s3])
        assert agg.jaccard_delta == -0.5

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
