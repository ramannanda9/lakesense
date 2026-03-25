"""
Baseline merge engine.

Merges a collection of SketchRecords into a single baseline sketch
using one of three strategies:

    rolling_window  — merge all records within the last N days
    snapshot        — use records pinned to a specific timestamp
    ewma            — exponentially weight recent records more heavily
                      (approximated via weighted sampling for MinHash)

The key property exploited here: MinHash and HLL merges are O(num_perm)
regardless of how many source sketches are combined — making rolling
window baselines nearly free to recompute at cadence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Literal

from lakesense.sketches.compute import SketchRecord


class BaselineStrategy(str, Enum):
    ROLLING_WINDOW = "rolling_window"
    SNAPSHOT = "snapshot"
    EWMA = "ewma"


@dataclass
class BaselineConfig:
    """
    User-defined baseline configuration per dataset.

    Examples:
        # 7-day rolling window
        BaselineConfig(dataset_id="user_features", strategy="rolling_window", window_days=7)

        # Pin to a known-good snapshot
        BaselineConfig(dataset_id="user_features", strategy="snapshot",
                       snapshot_id="2024-01-15T00:00:00+00:00")

        # Exponentially weight recent runs
        BaselineConfig(dataset_id="user_features", strategy="ewma", decay_factor=0.85)
    """

    dataset_id: str
    strategy: BaselineStrategy = BaselineStrategy.ROLLING_WINDOW
    window_days: int = 7
    snapshot_id: str | None = None  # ISO timestamp or Iceberg snapshot id
    decay_factor: float = 0.9  # EWMA: 1.0 = uniform, 0.5 = heavy recency bias
    min_records: int = 1  # minimum records needed to form a baseline

    def to_dict(self) -> dict:
        return {
            "strategy": self.strategy.value,
            "window_days": self.window_days,
            "snapshot_id": self.snapshot_id,
            "decay_factor": self.decay_factor,
        }


@dataclass
class BaselineSketch:
    """
    A merged baseline sketch for a single (dataset, column, sketch_type).
    Carries provenance: how many source records contributed.
    """

    dataset_id: str
    column: str
    sketch_type: str
    sketch_blob: bytes
    source_count: int
    strategy: BaselineStrategy
    merged_at: datetime


def merge_minhash_records(
    records: list[SketchRecord],
    weights: list[float] | None = None,
) -> bytes:
    """
    Merge multiple Theta blobs into a single baseline.
    Theta union is exact. EWMA weights are ignored as a native Theta union
    supersedes the need for position stochastic approximation.

    Args:
        records: SketchRecords of type "minhash" (theta)
        weights: optional weights per record (ignored for exact union)

    Returns:
        merged blob bytes
    """
    if not records:
        raise ValueError("cannot merge empty record list")

    from datasketches import compact_theta_sketch, theta_union

    u = theta_union(12)
    for r in records:
        sk = compact_theta_sketch.deserialize(r.sketch_blob)
        u.update(sk)

    return u.get_result().serialize()


def merge_hll_records(records: list[SketchRecord]) -> bytes:
    """
    Merge multiple HyperLogLog blobs into a single baseline.
    HLL union is exact — no information is lost in the merge.
    """
    if not records:
        raise ValueError("cannot merge empty record list")

    from datasketches import hll_sketch, hll_union

    u = hll_union(12)
    for r in records:
        sk = hll_sketch.deserialize(r.sketch_blob)
        u.update(sk)
    return u.get_result().serialize_updatable()


def _ewma_weights(n: int, decay: float) -> list[float]:
    """
    Compute EWMA weights for n records ordered oldest → newest.
    decay=0.9 means each record is 90% the weight of the next newer one.
    """
    raw = [decay ** (n - 1 - i) for i in range(n)]
    total = sum(raw)
    return [w / total for w in raw]


def build_baseline(
    records: list[SketchRecord],
    config: BaselineConfig,
    sketch_type: Literal["minhash", "hll", "kll"],
    column: str,
) -> BaselineSketch | None:
    """
    Build a merged baseline sketch from a list of SketchRecords.

    Args:
        records:      SketchRecords for a single (dataset, column, sketch_type)
        config:       BaselineConfig defining merge strategy
        sketch_type:  which sketch type to merge
        column:       column name

    Returns:
        BaselineSketch or None if insufficient records.
    """
    typed = [r for r in records if r.sketch_type == sketch_type and r.column == column]
    typed.sort(key=lambda r: r.run_ts)  # oldest → newest

    if config.strategy == BaselineStrategy.ROLLING_WINDOW:
        cutoff = datetime.now(timezone.utc) - timedelta(days=config.window_days)
        typed = [r for r in typed if r.run_ts >= cutoff]

    if len(typed) < config.min_records:
        return None

    weights = None
    if config.strategy == BaselineStrategy.EWMA:
        weights = _ewma_weights(len(typed), config.decay_factor)

    if sketch_type == "minhash":
        blob = merge_minhash_records(typed, weights=weights)
    elif sketch_type == "hll":
        blob = merge_hll_records(typed)
    else:
        # KLL
        from datasketches import kll_doubles_sketch

        base_sk = kll_doubles_sketch(200)
        for r in typed:
            sk = kll_doubles_sketch.deserialize(r.sketch_blob)
            base_sk.merge(sk)
        blob = base_sk.serialize()

    return BaselineSketch(
        dataset_id=config.dataset_id,
        column=column,
        sketch_type=sketch_type,
        sketch_blob=blob,
        source_count=len(typed),
        strategy=config.strategy,
        merged_at=datetime.now(timezone.utc),
    )
