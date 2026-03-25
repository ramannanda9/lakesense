"""
Drift signal computation.

Compares a current SketchRecord against a BaselineSketch
and returns a populated DriftSignals object.

These structured signals are what gets passed to the LLM interpreter —
the LLM reasons over derived metrics, not raw bytes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakesense.core.result import DriftSignals
from lakesense.sketches.compute import SketchRecord, hll_from_blob
from lakesense.sketches.merge import BaselineSketch

if TYPE_CHECKING:
    from lakesense.sketches.profile import ColumnProfile


def _deserialize_minhash(blob: bytes):
    from datasketches import compact_theta_sketch

    return compact_theta_sketch.deserialize(blob)


def _deserialize_hll(blob: bytes):
    return hll_from_blob(blob)


def _deserialize_kll_sketch(blob: bytes):
    from datasketches import kll_doubles_sketch

    return kll_doubles_sketch.deserialize(blob)


def compute_signals(
    current: SketchRecord,
    baseline: BaselineSketch,
) -> DriftSignals:
    """
    Compute drift signals by comparing current sketch to baseline.

    Args:
        current:  SketchRecord from the current job run
        baseline: merged BaselineSketch from the baseline engine

    Returns:
        DriftSignals with all applicable fields populated.
    """
    signals = DriftSignals()

    # null rate delta vs baseline null_rate (if baseline carries metadata)
    # populated by caller if baseline scalar metadata is available

    if current.sketch_type == "minhash":
        from datasketches import theta_jaccard_similarity

        cur_mh = _deserialize_minhash(current.sketch_blob)
        base_mh = _deserialize_minhash(baseline.sketch_blob)

        jaccard = float(theta_jaccard_similarity.jaccard(cur_mh, base_mh)[1])
        signals.jaccard_current = jaccard
        signals.jaccard_baseline = 1.0  # baseline is the reference
        signals.jaccard_delta = jaccard - 1.0  # 0.0 = identical, -1.0 = no overlap

    elif current.sketch_type == "hll":
        cur_hll = _deserialize_hll(current.sketch_blob)
        base_hll = _deserialize_hll(baseline.sketch_blob)

        cur_card = int(cur_hll.get_estimate())
        base_card = int(base_hll.get_estimate())

        signals.cardinality_current = cur_card
        signals.cardinality_baseline = base_card
        signals.cardinality_ratio = cur_card / base_card if base_card > 0 else float("inf")

    elif current.sketch_type == "kll":
        from datasketches import ks_test

        sk_cur = _deserialize_kll_sketch(current.sketch_blob)
        sk_base = _deserialize_kll_sketch(baseline.sketch_blob)

        import contextlib

        # 1. Evaluate Probability Distribution Space exactly
        with contextlib.suppress(Exception):
            signals.ks_test_divergent = bool(ks_test(sk_cur, sk_base, 0.05))

        # 2. Extract specific fixed latency shifts
        shifts: dict[str, float] = {}
        if not sk_cur.is_empty() and not sk_base.is_empty():
            cq = sk_cur.get_quantiles([0.5, 0.9, 0.99])
            bq = sk_base.get_quantiles([0.5, 0.9, 0.99])
            base_q = {"p50": float(bq[0]), "p90": float(bq[1]), "p99": float(bq[2])}
            cur_q = {"p50": float(cq[0]), "p90": float(cq[1]), "p99": float(cq[2])}

            for pct in ("p50", "p90", "p99"):
                if base_q[pct] != 0:
                    shifts[pct] = (cur_q[pct] - base_q[pct]) / abs(base_q[pct])

        signals.quantile_shifts = shifts

    return signals


def aggregate_signals(signal_list: list[DriftSignals]) -> DriftSignals:
    """
    Aggregate signals across multiple columns into a single summary.
    Takes the worst (most drifted) value for each metric.
    Used when interpreting multi-column datasets at the job level.
    """
    agg = DriftSignals()

    jaccard_deltas = [s.jaccard_delta for s in signal_list if s.jaccard_delta is not None]
    if jaccard_deltas:
        agg.jaccard_delta = min(jaccard_deltas)  # most negative = worst drift

    card_ratios = [s.cardinality_ratio for s in signal_list if s.cardinality_ratio is not None]
    if card_ratios:
        # deviation from 1.0 — furthest from 1.0 is worst
        agg.cardinality_ratio = max(card_ratios, key=lambda r: abs(r - 1.0))

    all_shifts: dict[str, list[float]] = {}
    for s in signal_list:
        for k, v in s.quantile_shifts.items():
            all_shifts.setdefault(k, []).append(v)
    agg.quantile_shifts = {k: max(vs, key=abs) for k, vs in all_shifts.items()}

    if any(s.ks_test_divergent for s in signal_list):
        agg.ks_test_divergent = True

    null_rates = [s.null_rate for s in signal_list if s.null_rate is not None]
    if null_rates:
        agg.null_rate = max(null_rates)

    # propagate profile-level signals (already aggregated upstream)
    for s in signal_list:
        if s.max_null_rate_delta is not None:
            curr = agg.max_null_rate_delta or 0.0
            agg.max_null_rate_delta = max(curr, s.max_null_rate_delta, key=abs)
        if s.row_count_delta is not None:
            agg.row_count_delta = s.row_count_delta
        if s.missing_columns:
            agg.missing_columns = s.missing_columns
        if s.new_columns:
            agg.new_columns = s.new_columns
        if s.bool_true_rate_delta is not None:
            curr = agg.bool_true_rate_delta or 0.0
            agg.bool_true_rate_delta = max(curr, s.bool_true_rate_delta, key=abs)
        if s.categorical_top_shift is not None:
            curr = agg.categorical_top_shift or 0.0
            agg.categorical_top_shift = max(curr, s.categorical_top_shift)

    return agg


def compute_profile_signals(
    current_profiles: list[ColumnProfile],
    baseline_profiles: list[ColumnProfile],
) -> DriftSignals:
    """
    Compute drift signals by comparing current ColumnProfiles to baseline.
    Returns a single DriftSignals with the worst values across all columns.

    Args:
        current_profiles:  profiles from the current run
        baseline_profiles: profiles from the merged baseline

    Returns:
        DriftSignals populated with profile-based metrics.
    """
    from lakesense.sketches.profile import ColumnProfile  # noqa: F401 — type ref

    signals = DriftSignals()

    baseline_by_col: dict[str, ColumnProfile] = {p.column: p for p in baseline_profiles}
    current_by_col: dict[str, ColumnProfile] = {p.column: p for p in current_profiles}

    # schema drift
    signals.missing_columns = [c for c in baseline_by_col if c not in current_by_col]
    signals.new_columns = [c for c in current_by_col if c not in baseline_by_col]

    # row count ratio across dataset (use first common column as proxy)
    common_cols = [c for c in current_by_col if c in baseline_by_col]
    if common_cols:
        col0 = common_cols[0]
        cur_rows = current_by_col[col0].row_count
        base_rows = baseline_by_col[col0].row_count
        if base_rows > 0:
            signals.row_count_delta = cur_rows / base_rows

    # per-column metrics — collect worst across all columns
    null_rate_deltas: list[float] = []
    bool_rate_deltas: list[float] = []
    cat_shifts: list[float] = []

    for col in common_cols:
        cur = current_by_col[col]
        base = baseline_by_col[col]

        # null rate delta
        null_delta = cur.null_rate - base.null_rate
        null_rate_deltas.append(abs(null_delta))

        # boolean ratio delta
        if cur.bool_true_rate is not None and base.bool_true_rate is not None:
            bool_rate_deltas.append(abs(cur.bool_true_rate - base.bool_true_rate))

        # categorical top-N overlap
        if cur.top_values and base.top_values:
            cur_vals = {v for v, _ in cur.top_values}
            base_vals = {v for v, _ in base.top_values}
            overlap = len(cur_vals & base_vals) / max(len(base_vals), 1)
            cat_shifts.append(1.0 - overlap)  # 0 = identical, 1 = no overlap

        # integer range violations — negatives appeared where there were none
        if (
            cur.int_negative_count is not None
            and base.int_negative_count == 0
            and cur.int_negative_count > 0
        ):
            signals.range_min_delta = float(cur.numeric_min or 0)

    if null_rate_deltas:
        signals.max_null_rate_delta = max(null_rate_deltas)
    if bool_rate_deltas:
        signals.bool_true_rate_delta = max(bool_rate_deltas)
    if cat_shifts:
        signals.categorical_top_shift = max(cat_shifts)

    return signals
