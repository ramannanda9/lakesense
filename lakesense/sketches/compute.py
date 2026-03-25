"""
Sketch computation — wraps datasketch primitives into a unified SketchRecord.

Each SketchRecord captures one job/dataset/column snapshot.
Sketches are stored as raw bytes so they remain mergeable after deserialization.

Supported sketch types:
    MinHash      — Jaccard similarity / near-duplicate detection
    HyperLogLog  — cardinality estimation
    KLL          — quantile sketches (numerical columns)
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from datasketches import (
    compact_theta_sketch,
    hll_sketch,
    kll_doubles_sketch,
    update_theta_sketch,
)


@dataclass
class SketchRecord:
    """
    A single sketch snapshot for one (job, dataset, column, run_ts) tuple.

    sketch_blob is the raw serialized bytes of the sketch — always store
    the blob, not the deserialized object, so merges work across processes.
    """

    dataset_id: str
    job_id: str
    column: str
    sketch_type: str  # "minhash" | "hll" | "kll"
    sketch_blob: bytes
    run_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    num_perm: int | None = None  # MinHash: number of permutations
    num_rows: int | None = None
    null_count: int | None = None
    sketch_config: dict[str, Any] = field(default_factory=dict)

    @property
    def null_rate(self) -> float | None:
        if self.num_rows and self.null_count is not None:
            return self.null_count / self.num_rows
        return None


def hll_from_blob(blob: bytes) -> hll_sketch:
    """Deserialize a HyperLogLog from a datasketches struct."""
    return hll_sketch.deserialize(blob)


def hll_to_blob(h: hll_sketch) -> bytes:
    """Serialize a HyperLogLog to bytes."""
    return h.serialize_updatable()


def compute_minhash(
    values: Iterable[str],
    num_perm: int = 128,
) -> tuple[bytes, compact_theta_sketch]:
    """
    Compute a Theta sketch over a list of string values (kept name as minhash for compatibility).
    Tokenizes by whitespace — suitable for text columns and set-valued features.

    Args:
        values:   iterable of string values (nulls pre-filtered by caller)
        num_perm: kept for signature compatibility.

    Returns:
        (blob, compact_theta) — blob for storage, theta sketch for immediate comparison
    """
    m = update_theta_sketch(12)  # lg_k=12 gives similar space/accuracy to num_perm=128
    for val in values:
        for token in str(val).lower().split():
            m.update(token)
    compact = m.compact()
    return compact.serialize(), compact


def compute_hll(
    values: Iterable[Any],
    p: int = 12,
) -> tuple[bytes, hll_sketch]:
    """
    Compute a HyperLogLog sketch for cardinality estimation.

    Args:
        values: iterable of any hashable values
        p:      precision parameter (10–18)

    Returns:
        (blob, hll) — blob for storage, hll for immediate .count()
    """
    h = hll_sketch(p)
    for val in values:
        h.update(str(val))
    return h.serialize_updatable(), h


def compute_kll(
    values: Iterable[float],
    k: int = 200,
) -> tuple[bytes, dict[str, float]]:
    """
    Compute a KLL quantile sketch using Apache DataSketches.
    Returns both a serialized blob and a quantile dict.

    Args:
        values: list of floats (nulls pre-filtered)
        k:      sketch size (number of retained samples)

    Returns:
        (blob, quantiles) — blob is a datastructures KLL blob
    """
    sk = kll_doubles_sketch(k)

    n = 0
    mean = 0.0
    m2 = 0.0

    for val in values:
        if val is None or math.isnan(val):
            continue
        v = float(val)
        sk.update(v)
        n += 1
        delta = v - mean
        mean += delta / n
        m2 += delta * (v - mean)

    std = math.sqrt(m2 / n) if n > 0 else 0.0

    if sk.is_empty():
        return sk.serialize(), {}

    # KLL quantiles are 0-1
    q = sk.get_quantiles([0.5, 0.9, 0.99])
    quantiles = {
        "p50": float(q[0]),
        "p90": float(q[1]),
        "p99": float(q[2]),
        "min": float(sk.get_min_value()),
        "max": float(sk.get_max_value()),
        "mean": float(mean),
        "std": float(std),
    }
    blob = sk.serialize()
    return blob, quantiles
