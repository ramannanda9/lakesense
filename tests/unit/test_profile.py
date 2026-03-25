"""Unit tests for lakesense.sketches.profile"""
# ruff: noqa: I001

import pandas as pd
import pytest

from lakesense.sketches.profile import (
    ColumnProfile,
    profile_column,
    profile_dataframe,
    profiles_to_sketch_records,
    sketch_record_to_profile,
)
from lakesense.sketches.signals import compute_profile_signals


class TestProfileColumn:
    def test_null_rate(self):
        s = pd.Series([1.0, None, 3.0, None], name="col")
        p = profile_column(s, str(s.name), str(s.dtype))
        assert p.null_count == 2
        assert p.row_count == 4
        assert p.null_rate == pytest.approx(0.5)

    def test_float_numeric(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0], name="score")
        p = profile_column(s, str(s.name), str(s.dtype))
        assert p.numeric_min == pytest.approx(1.0)
        assert p.numeric_max == pytest.approx(5.0)
        assert p.numeric_mean == pytest.approx(3.0)
        assert p.numeric_zeros == 0
        assert p.int_negative_count is None  # float, not int

    def test_integer_range(self):
        s = pd.Series([0, 1, -5, 3, 10], dtype="int64", name="count")
        p = profile_column(s, str(s.name), str(s.dtype))
        assert p.numeric_min == -5.0
        assert p.int_negative_count == 1
        assert p.numeric_zeros == 1

    def test_integer_no_negatives(self):
        s = pd.Series([1, 2, 3, 100], dtype="int64", name="id")
        p = profile_column(s, str(s.name), str(s.dtype))
        assert p.int_negative_count == 0

    def test_boolean(self):
        s = pd.Series([True, True, False, True, False], name="is_active")
        p = profile_column(s, str(s.name), str(s.dtype))
        assert p.bool_true_count == 3
        assert p.bool_false_count == 2
        assert p.bool_true_rate == pytest.approx(0.6)

    def test_string_lengths(self):
        s = pd.Series(["hi", "hello", "", "world!"], name="text")
        p = profile_column(s, str(s.name), str(s.dtype))
        assert p.str_min_len == 0
        assert p.str_max_len == 6
        assert p.str_empty_count == 1
        assert p.str_mean_len == pytest.approx(3.25)

    def test_categorical_top_values(self):
        s = pd.Series(["a", "b", "a", "a", "b", "c"], name="status")
        p = profile_column(s, str(s.name), str(s.dtype), top_n=3)
        values = [v for v, _ in p.top_values]
        assert "a" in values
        assert "b" in values
        assert p.distinct_count == 0

    def test_all_null_column(self):
        s = pd.Series([None, None, None], name="empty")
        p = profile_column(s, str(s.name), str(s.dtype))
        assert p.null_rate == pytest.approx(1.0)
        assert p.distinct_count == 0

    def test_round_trip_serialization(self):
        s = pd.Series([1, 2, -3, 0, 5], dtype="int64", name="val")
        p = profile_column(s, str(s.name), str(s.dtype))
        restored = ColumnProfile.from_dict(p.to_dict())
        assert restored.numeric_min == p.numeric_min
        assert restored.int_negative_count == p.int_negative_count
        assert restored.null_rate == p.null_rate


class TestProfileDataframe:
    def test_profiles_all_columns(self):
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "score": [0.1, 0.9, None, 0.5, 0.7],
                "label": ["a", "b", "a", "c", "a"],
                "is_active": [True, False, True, True, None],
            }
        )
        profiles = profile_dataframe(df)
        assert len(profiles) == 4
        cols = [p.column for p in profiles]
        assert "user_id" in cols
        assert "score" in cols
        assert "label" in cols
        assert "is_active" in cols

    def test_null_rate_in_profile(self):
        df = pd.DataFrame({"x": [1.0, None, None, 4.0]})
        [p] = profile_dataframe(df, columns=["x"])
        assert p.null_rate == pytest.approx(0.5)

    def test_to_sketch_records(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        profiles = profile_dataframe(df)
        records = profiles_to_sketch_records(profiles, "ds", "job1")
        assert len(records) == 2
        assert all(r.sketch_type == "profile" for r in records)
        assert all(r.dataset_id == "ds" for r in records)

    def test_sketch_record_round_trip(self):
        df = pd.DataFrame({"score": [1.0, 2.0, None, 4.0]})
        [profile] = profile_dataframe(df, columns=["score"])
        [record] = profiles_to_sketch_records([profile], "ds", "j1")
        restored = sketch_record_to_profile(record)
        assert restored.column == "score"
        assert restored.null_count == 1
        assert restored.numeric_mean == pytest.approx(7.0 / 3, abs=0.01)


class TestProfileSignals:
    def _make_profiles(
        self, null_rate=0.0, bool_true=0.6, top_vals=None, neg_count=0
    ) -> list[ColumnProfile]:
        n = 100
        return [
            ColumnProfile(
                column="score",
                dtype="float64",
                row_count=n,
                null_count=int(n * null_rate),
                numeric_min=-1.0 if neg_count > 0 else 0.0,
                numeric_max=10.0,
                numeric_mean=5.0,
                int_negative_count=neg_count,
            ),
            ColumnProfile(
                column="is_active",
                dtype="bool",
                row_count=n,
                null_count=0,
                bool_true_count=int(n * bool_true),
                bool_false_count=int(n * (1 - bool_true)),
            ),
            ColumnProfile(
                column="status",
                dtype="object",
                row_count=n,
                null_count=0,
                top_values=top_vals or [("active", 80), ("inactive", 20)],
            ),
        ]

    def test_null_rate_delta(self):
        baseline = self._make_profiles(null_rate=0.02)
        current = self._make_profiles(null_rate=0.40)  # spike
        signals = compute_profile_signals(current, baseline)
        assert signals.max_null_rate_delta == pytest.approx(0.38, abs=0.01)

    def test_bool_rate_delta(self):
        baseline = self._make_profiles(bool_true=0.60)
        current = self._make_profiles(bool_true=0.05)  # nearly all False
        signals = compute_profile_signals(current, baseline)
        assert signals.bool_true_rate_delta == pytest.approx(0.55, abs=0.01)

    def test_categorical_shift(self):
        baseline = self._make_profiles(top_vals=[("active", 80), ("inactive", 20)])
        current = self._make_profiles(top_vals=[("unknown", 90), ("error", 10)])
        signals = compute_profile_signals(current, baseline)
        assert signals.categorical_top_shift == pytest.approx(1.0, abs=0.01)

    def test_missing_column_detected(self):
        baseline = self._make_profiles()
        # remove "status" from current
        current = [p for p in self._make_profiles() if p.column != "status"]
        signals = compute_profile_signals(current, baseline)
        assert "status" in signals.missing_columns

    def test_new_column_detected(self):
        baseline = self._make_profiles()
        current = self._make_profiles() + [
            ColumnProfile(column="new_feature", dtype="float64", row_count=100, null_count=0)
        ]
        signals = compute_profile_signals(current, baseline)
        assert "new_feature" in signals.new_columns

    def test_integer_negative_spike(self):
        baseline = self._make_profiles(neg_count=0)
        current = self._make_profiles(neg_count=15)  # negatives appeared
        signals = compute_profile_signals(current, baseline)
        assert signals.range_min_delta is not None
        assert signals.range_min_delta < 0

    def test_no_drift_produces_small_signals(self):
        profiles = self._make_profiles()
        signals = compute_profile_signals(profiles, profiles)
        assert (signals.max_null_rate_delta or 0.0) == pytest.approx(0.0)
        assert (signals.bool_true_rate_delta or 0.0) == pytest.approx(0.0)
        assert signals.missing_columns == []
