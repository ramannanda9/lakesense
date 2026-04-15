"""
Drift signal computation.

Compares a current SketchRecord against a BaselineSketch
and returns a populated DriftSignals object.

These structured signals are what gets passed to the LLM interpreter —
the LLM reasons over derived metrics, not raw bytes.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lakesense.core.result import DatasetDriftSummary, DriftSignals
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
    Compute per-column drift signals by comparing current sketch to baseline.

    Args:
        current:  SketchRecord from the current job run
        baseline: merged BaselineSketch from the baseline engine

    Returns:
        DriftSignals with all applicable fields populated.
    """
    signals = DriftSignals()

    if current.sketch_type == "minhash":
        from datasketches import theta_jaccard_similarity

        cur_tok = current.sketch_config.get("tokenizer")
        base_tok = baseline.sketch_config.get("tokenizer")
        if cur_tok and base_tok and cur_tok != base_tok:
            raise ValueError(
                f"Tokenizer mismatch: current sketch uses '{cur_tok}' but baseline uses '{base_tok}'. "
                "Rebuild the baseline with the same tokenizer before comparing."
            )

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

        with contextlib.suppress(Exception):
            signals.ks_test_divergent = bool(ks_test(sk_cur, sk_base, 0.05))

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


def aggregate_signals(signals: dict[str, DriftSignals]) -> DatasetDriftSummary:
    """
    Aggregate per-column signals into a dataset-level summary.
    Records which column produced the worst value for each metric.

    Args:
        signals: mapping of column_name -> DriftSignals
    """
    summary = DatasetDriftSummary()

    jaccard_pairs = [(col, s.jaccard_delta) for col, s in signals.items() if s.jaccard_delta is not None]
    if jaccard_pairs:
        worst_col, worst_val = min(jaccard_pairs, key=lambda p: p[1])
        summary.jaccard_delta = worst_val
        summary.jaccard_worst_column = worst_col

    card_pairs = [(col, s.cardinality_ratio) for col, s in signals.items() if s.cardinality_ratio is not None]
    if card_pairs:
        worst_col, worst_val = max(card_pairs, key=lambda p: abs(p[1] - 1.0))
        summary.cardinality_ratio = worst_val
        summary.cardinality_worst_column = worst_col

    all_shifts: dict[str, list[float]] = {}
    for s in signals.values():
        for k, v in s.quantile_shifts.items():
            all_shifts.setdefault(k, []).append(v)
    summary.quantile_shifts = {k: max(vs, key=abs) for k, vs in all_shifts.items()}

    if any(s.ks_test_divergent for s in signals.values()):
        summary.ks_test_divergent = True

    null_pairs = [(col, s.max_null_rate_delta) for col, s in signals.items() if s.max_null_rate_delta is not None]
    if null_pairs:
        worst_col, worst_val = max(null_pairs, key=lambda p: abs(p[1]))
        summary.max_null_rate_delta = worst_val
        summary.null_rate_worst_column = worst_col

    bool_pairs = [(col, s.bool_true_rate_delta) for col, s in signals.items() if s.bool_true_rate_delta is not None]
    if bool_pairs:
        worst_col, worst_val = max(bool_pairs, key=lambda p: abs(p[1]))
        summary.bool_true_rate_delta = worst_val
        summary.bool_rate_worst_column = worst_col

    cat_pairs = [(col, s.categorical_top_shift) for col, s in signals.items() if s.categorical_top_shift is not None]
    if cat_pairs:
        worst_col, worst_val = max(cat_pairs, key=lambda p: p[1])
        summary.categorical_top_shift = worst_val
        summary.categorical_worst_column = worst_col

    range_pairs = [(col, s.range_min_delta) for col, s in signals.items() if s.range_min_delta is not None]
    if range_pairs:
        worst_col, worst_val = max(range_pairs, key=lambda p: abs(p[1]))
        summary.range_min_delta = worst_val
        summary.range_worst_column = worst_col

    return summary


def compute_profile_signals(
    current_profiles: list[ColumnProfile],
    baseline_profiles: list[ColumnProfile],
) -> dict[str, DriftSignals]:
    """
    Compute per-column drift signals from ColumnProfiles.
    Returns one DriftSignals per common column — no dataset-level signals
    (schema drift and row count are computed in base_interpret directly).

    Args:
        current_profiles:  profiles from the current run
        baseline_profiles: profiles from the merged baseline

    Returns:
        dict mapping column_name -> DriftSignals
    """
    from lakesense.sketches.profile import ColumnProfile  # noqa: F401 — type ref

    baseline_by_col: dict[str, ColumnProfile] = {p.column: p for p in baseline_profiles}
    current_by_col: dict[str, ColumnProfile] = {p.column: p for p in current_profiles}

    common_cols = [c for c in current_by_col if c in baseline_by_col]
    per_column: dict[str, DriftSignals] = {}

    for col in common_cols:
        cur = current_by_col[col]
        base = baseline_by_col[col]
        col_signals = DriftSignals()

        col_signals.max_null_rate_delta = abs(cur.null_rate - base.null_rate)

        if cur.bool_true_rate is not None and base.bool_true_rate is not None:
            col_signals.bool_true_rate_delta = abs(cur.bool_true_rate - base.bool_true_rate)

        if cur.top_values and base.top_values:
            cur_vals = {v for v, _ in cur.top_values}
            base_vals = {v for v, _ in base.top_values}
            overlap = len(cur_vals & base_vals) / max(len(base_vals), 1)
            col_signals.categorical_top_shift = 1.0 - overlap

        if cur.int_negative_count is not None and base.int_negative_count == 0 and cur.int_negative_count > 0:
            col_signals.range_min_delta = float(cur.numeric_min or 0)

        per_column[col] = col_signals

    return per_column
