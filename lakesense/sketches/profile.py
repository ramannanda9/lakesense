"""
Column profiling — scalar metrics that complement sketch-based signals.

ColumnProfile captures everything you'd check manually:
    - Null rate (most common real data quality signal)
    - Row count
    - Integer ranges (min/max/mean for int columns)
    - Categorical distribution (value counts, top-N)
    - String length distribution (min/max/mean length)
    - Boolean ratio (fraction True)
    - Schema presence (column existed this run)

These are cheap to compute, deterministic (no probabilistic error),
and mergeable in a weaker sense — we store the raw scalars and let
the signal layer compute deltas.

ColumnProfile is stored as a SketchRecord with sketch_type="profile"
and sketch_blob = msgpack/json-encoded scalar dict. This keeps the
storage layer uniform — one table, one schema.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from lakesense.sketches.compute import SketchRecord


@dataclass
class ColumnProfile:
    """
    Scalar profile for a single column in a single run.
    All fields are optional — populated based on column dtype.
    """

    column: str
    dtype: str  # pandas dtype string e.g. "int64", "object", "float64"

    # Universal
    row_count: int = 0
    null_count: int = 0
    distinct_count: int = 0  # exact count (not HLL) — useful for low-cardinality cols

    # Numeric (int + float)
    numeric_min: float | None = None
    numeric_max: float | None = None
    numeric_mean: float | None = None
    numeric_std: float | None = None
    numeric_zeros: int | None = None  # count of exact zeros — useful for sparse features

    # Integer-specific
    int_negative_count: int | None = None  # values < 0

    # Categorical / object
    top_values: list[tuple[str, int]] = field(default_factory=list)  # [(value, count), ...]
    top_n: int = 10

    # String-specific
    str_min_len: int | None = None
    str_max_len: int | None = None
    str_mean_len: float | None = None
    str_empty_count: int | None = None  # "" (not null)

    # Boolean
    bool_true_count: int | None = None
    bool_false_count: int | None = None

    @property
    def null_rate(self) -> float:
        return self.null_count / self.row_count if self.row_count > 0 else 0.0

    @property
    def bool_true_rate(self) -> float | None:
        if self.bool_true_count is None or self.bool_false_count is None:
            return None
        total = self.bool_true_count + self.bool_false_count
        return self.bool_true_count / total if total > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "dtype": self.dtype,
            "row_count": self.row_count,
            "null_count": self.null_count,
            "null_rate": self.null_rate,
            "distinct_count": self.distinct_count,
            "numeric_min": self.numeric_min,
            "numeric_max": self.numeric_max,
            "numeric_mean": self.numeric_mean,
            "numeric_std": self.numeric_std,
            "numeric_zeros": self.numeric_zeros,
            "int_negative_count": self.int_negative_count,
            "top_values": self.top_values,
            "str_min_len": self.str_min_len,
            "str_max_len": self.str_max_len,
            "str_mean_len": self.str_mean_len,
            "str_empty_count": self.str_empty_count,
            "bool_true_count": self.bool_true_count,
            "bool_false_count": self.bool_false_count,
        }

    def merge(self, other: ColumnProfile) -> None:
        """Merge another profile into this one (inplace)."""
        if self.column != other.column:
            raise ValueError(f"cannot merge profiles for different columns: {self.column} vs {other.column}")

        # Update n_val (non-null observations) for weighted means
        n_self = self.row_count - self.null_count
        n_other = other.row_count - other.null_count
        n_total = n_self + n_other

        # Counts
        self.row_count += other.row_count
        self.null_count += other.null_count
        self.distinct_count = 0  # Still delegated to HLL

        # Numeric extremums
        def _merge_min(a: float | None, b: float | None) -> float | None:
            if a is None:
                return b
            if b is None:
                return a
            return min(a, b)

        def _merge_max(a: float | None, b: float | None) -> float | None:
            if a is None:
                return b
            if b is None:
                return a
            return max(a, b)

        self.numeric_min = _merge_min(self.numeric_min, other.numeric_min)
        self.numeric_max = _merge_max(self.numeric_max, other.numeric_max)

        # Weighted Numeric Mean
        if n_total > 0 and (self.numeric_mean is not None or other.numeric_mean is not None):
            s_mean = self.numeric_mean or 0.0
            o_mean = other.numeric_mean or 0.0
            self.numeric_mean = (s_mean * n_self + o_mean * n_other) / n_total

        # Other numeric counts
        if other.numeric_zeros is not None:
            self.numeric_zeros = (self.numeric_zeros or 0) + other.numeric_zeros
        if other.int_negative_count is not None:
            self.int_negative_count = (self.int_negative_count or 0) + other.int_negative_count

        # Booleans
        if other.bool_true_count is not None:
            self.bool_true_count = (self.bool_true_count or 0) + other.bool_true_count
        if other.bool_false_count is not None:
            self.bool_false_count = (self.bool_false_count or 0) + other.bool_false_count

        # Strings
        self.str_min_len = _merge_min(self.str_min_len, other.str_min_len)
        if self.str_min_len is not None:
            self.str_min_len = int(self.str_min_len)

        self.str_max_len = _merge_max(self.str_max_len, other.str_max_len)
        if self.str_max_len is not None:
            self.str_max_len = int(self.str_max_len)

        if n_total > 0 and (self.str_mean_len is not None or other.str_mean_len is not None):
            s_len = self.str_mean_len or 0.0
            o_len = other.str_mean_len or 0.0
            self.str_mean_len = (s_len * n_self + o_len * n_other) / n_total
        if other.str_empty_count is not None:
            self.str_empty_count = (self.str_empty_count or 0) + other.str_empty_count

        # Top Values (Approximate merge of Top N)
        if other.top_values:
            combined = {}
            for val, count in self.top_values:
                combined[val] = combined.get(val, 0) + count
            for val, count in other.top_values:
                combined[val] = combined.get(val, 0) + count

            merged_top = sorted(combined.items(), key=lambda x: x[1], reverse=True)
            self.top_values = merged_top[: self.top_n]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ColumnProfile:
        top = d.get("top_values") or []
        # stored as list of [value, count] pairs (JSON doesn't have tuples)
        top_values = [tuple(v) for v in top] if top else []
        return cls(
            column=d["column"],
            dtype=d.get("dtype", "unknown"),
            row_count=d.get("row_count", 0),
            null_count=d.get("null_count", 0),
            distinct_count=d.get("distinct_count", 0),
            numeric_min=d.get("numeric_min"),
            numeric_max=d.get("numeric_max"),
            numeric_mean=d.get("numeric_mean"),
            numeric_std=d.get("numeric_std"),
            numeric_zeros=d.get("numeric_zeros"),
            int_negative_count=d.get("int_negative_count"),
            top_values=top_values,
            str_min_len=d.get("str_min_len"),
            str_max_len=d.get("str_max_len"),
            str_mean_len=d.get("str_mean_len"),
            str_empty_count=d.get("str_empty_count"),
            bool_true_count=d.get("bool_true_count"),
            bool_false_count=d.get("bool_false_count"),
        )


def profile_column(values: Iterable[Any], col_name: str, dtype_name: str = "unknown", top_n: int = 10) -> ColumnProfile:
    """
    Compute a ColumnProfile for an iterable of values in a single pass (O(1) memory).
    Auto-detects values and populates the appropriate fields dynamically.

    Args:
        values:     iterable of any values
        col_name:   name of the column
        dtype_name: dtype string (optional, e.g. from upstream schema)
        top_n:      number of top values to capture for categoricals

    Returns:
        ColumnProfile with all applicable fields populated.
    """
    import math

    from datasketches import frequent_items_error_type, frequent_strings_sketch

    profile = ColumnProfile(
        column=col_name,
        dtype=dtype_name,
    )

    n_rows = 0
    nulls = 0

    # numeric accumulators
    n_num = 0
    num_min = float("inf")
    num_max = float("-inf")
    num_mean = 0.0
    num_m2 = 0.0
    num_zeros = 0
    int_neg = 0

    # bool accumulators
    bool_t = 0
    bool_f = 0

    # string accumulators
    str_min_len = float("inf")
    str_max_len = float("-inf")
    str_sum_len = 0
    str_empty = 0
    n_str = 0

    # We use a size of lg_k=10 for the frequent strings sketch which gives 1024 counters.
    freq_sk = frequent_strings_sketch(10)

    for val in values:
        n_rows += 1

        # null check
        if val is None or (isinstance(val, float) and math.isnan(val)):
            nulls += 1
            continue

        # booleans (bool is a subclass of int, so check it first)
        if isinstance(val, bool):
            if val:
                bool_t += 1
            else:
                bool_f += 1
            continue

        # numerics (including datetimes as timestamps)
        v = None
        is_int = False
        if isinstance(val, int | float):
            v = float(val)
            is_int = isinstance(val, int)
        else:
            # check for datetime
            if hasattr(val, "timestamp") and callable(val.timestamp):
                v = float(val.timestamp())  # type: ignore

        if v is not None:
            n_num += 1
            if v < num_min:
                num_min = v
            if v > num_max:
                num_max = v

            delta = v - num_mean
            num_mean += delta / n_num
            num_m2 += delta * (v - num_mean)

            if v == 0.0:
                num_zeros += 1

            if is_int and val < 0:
                int_neg += 1

        # string & frequent items processing
        sval = str(val)
        if isinstance(val, str):
            n_str += 1
            slen = len(sval)
            if slen < str_min_len:
                str_min_len = slen
            if slen > str_max_len:
                str_max_len = slen
            str_sum_len += slen
            if slen == 0:
                str_empty += 1

        freq_sk.update(sval)

    profile.row_count = n_rows
    profile.null_count = nulls

    if n_rows == 0 or nulls == n_rows:
        return profile

    # Population decisions based on what was observed:
    if (bool_t + bool_f) > 0:
        profile.bool_true_count = bool_t
        profile.bool_false_count = bool_f

    if n_num > 0:
        profile.numeric_min = num_min
        profile.numeric_max = num_max
        profile.numeric_mean = num_mean
        profile.numeric_std = math.sqrt(num_m2 / n_num) if n_num > 1 else 0.0
        profile.numeric_zeros = num_zeros
        if int_neg > 0 or dtype_name.startswith("int"):
            profile.int_negative_count = int_neg

    if n_str > 0:
        profile.str_min_len = int(str_min_len)
        profile.str_max_len = int(str_max_len)
        profile.str_mean_len = float(str_sum_len) / n_str
        profile.str_empty_count = str_empty

    # Categoricals / Top N
    if not freq_sk.is_empty():
        items = freq_sk.get_frequent_items(frequent_items_error_type.NO_FALSE_POSITIVES)
        # sort by estimate descending
        items.sort(key=lambda x: x[1], reverse=True)
        profile.top_values = [(item, int(count)) for item, count, _, _ in items[:top_n]]
        profile.top_n = top_n

    # We leave distinct_count as 0 because HLL accomplishes this perfectly in lakesense sketches.
    profile.distinct_count = 0

    return profile


def profile_dataframe(
    df: Any,
    columns: list[str] | None = None,
    top_n: int = 10,
) -> list[ColumnProfile]:
    """
    Profile every column in a DataFrame (or a specified subset).

    Args:
        df:      pandas DataFrame
        columns: subset of columns to profile (default: all)
        top_n:   top-N values to capture for categoricals

    Returns:
        List of ColumnProfile, one per column.
    """
    cols = columns or list(df.columns)
    results = []

    import pandas as pd

    n_rows = len(df)

    for c in cols:
        if c not in df.columns:
            continue

        series = df[c]
        dtype_name = str(series.dtype)

        # Exact null/row counts are instant in pandas
        null_count = int(series.isna().sum())
        non_null_series = series.dropna()
        n_val = n_rows - null_count

        profile = ColumnProfile(
            column=c,
            dtype=dtype_name,
            row_count=n_rows,
            null_count=null_count,
            distinct_count=0,  # delegated to HLL sketch
        )

        if n_val == 0:
            results.append(profile)
            continue

        # Booleans
        if pd.api.types.is_bool_dtype(series):
            profile.bool_true_count = int(non_null_series.sum())
            profile.bool_false_count = n_val - profile.bool_true_count

        # Numerics
        elif pd.api.types.is_numeric_dtype(series):
            profile.numeric_min = float(non_null_series.min())
            profile.numeric_max = float(non_null_series.max())
            profile.numeric_mean = float(non_null_series.mean())
            # Pandas std uses N-1 by default. We mimic Welford's
            # N-dependent std calculation (ddof=0)
            # logic.
            std = non_null_series.std(ddof=0)
            profile.numeric_std = float(std) if pd.notna(std) else 0.0
            profile.numeric_zeros = int((non_null_series == 0).sum())
            if "int" in dtype_name:
                profile.int_negative_count = int((non_null_series < 0).sum())

        # Strings
        elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
            # Check if this object column actually has strings
            str_mask = non_null_series.apply(lambda x: isinstance(x, str))
            if str_mask.any():
                str_series = non_null_series[str_mask].astype(str)
                lengths = str_series.str.len()
                if not lengths.empty:
                    profile.str_min_len = int(lengths.min())
                    profile.str_max_len = int(lengths.max())
                    profile.str_mean_len = float(lengths.mean())
                    profile.str_empty_count = int((lengths == 0).sum())

        # Top N categoricals (works across strings, ints, etc)
        head_values = non_null_series.value_counts(dropna=True).head(top_n)
        if not head_values.empty:
            profile.top_values = [(str(k), int(v)) for k, v in head_values.items()]
            profile.top_n = top_n

        results.append(profile)

    return results


def profiles_to_sketch_records(
    profiles: list[ColumnProfile],
    dataset_id: str,
    job_id: str,
    run_ts: datetime | None = None,
) -> list[SketchRecord]:
    """
    Convert ColumnProfiles into SketchRecords for unified storage.
    sketch_type = "profile", sketch_blob = JSON-encoded scalar dict.
    """
    ts = run_ts or datetime.now(timezone.utc)
    records = []
    for p in profiles:
        blob = json.dumps(p.to_dict()).encode("utf-8")
        records.append(
            SketchRecord(
                dataset_id=dataset_id,
                job_id=job_id,
                column=p.column,
                sketch_type="profile",
                sketch_blob=blob,
                run_ts=ts,
                num_rows=p.row_count,
                null_count=p.null_count,
                sketch_config={"dtype": p.dtype},
            )
        )
    return records


def sketch_record_to_profile(record: SketchRecord) -> ColumnProfile:
    """Deserialize a profile SketchRecord back into a ColumnProfile."""
    d = json.loads(record.sketch_blob.decode("utf-8"))
    return ColumnProfile.from_dict(d)
