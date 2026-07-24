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
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice
from typing import Any, Literal

import numpy as np
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


def _word_ngram_tokens(val: str, n: int = 2) -> list[str]:
    """Unigrams + bigrams from whitespace-split words. Better Jaccard sensitivity than plain bag-of-words."""
    words = val.lower().split()
    tokens = words[:]
    for i in range(len(words) - n + 1):
        tokens.append(" ".join(words[i : i + n]))
    return tokens


def _char_shingle_tokens(val: str, k: int = 3) -> list[str]:
    """Character k-shingles — catches format/structure drift in short strings and IDs."""
    s = val.lower()
    if len(s) <= k:
        return [s]
    return [s[i : i + k] for i in range(len(s) - k + 1)]


def _distinct_lowered(values: Iterable[str]) -> Iterator[str]:
    """
    Yield lowercased values, skipping ones already seen.

    Theta is a set sketch — updating with a value already seen is a no-op — so
    tokenizing each distinct value once produces an identical sketch for a
    fraction of the work. All three tokenizers lowercase, so the str()/lower()
    coercion folds into this pass.

    The seen-set is capped and cleared on overflow rather than grown without
    bound: StreamingProvider may hand us a generator over a high-cardinality
    column, where remembering every value would be O(n) memory. Clearing costs
    only redundant re-tokenization of an evicted value — the sketch is
    idempotent, so the result is unchanged either way.

    dict (not set) is deliberate: it preserves first-occurrence order, which is
    deterministic across processes. Theta's retained sample IS insertion-order
    dependent, and set iteration order for strings varies with PYTHONHASHSEED,
    which would make blobs irreproducible across Spark workers.
    """
    seen: dict[str, None] = {}
    for val in values:
        s = str(val).lower()
        if s in seen:
            continue
        if len(seen) >= _DEDUP_CAP:
            seen.clear()
        seen[s] = None
        yield s


def compute_minhash(
    values: Iterable[str],
    num_perm: int = 128,
    tokenizer: Literal["word_ngram", "char_shingle", "whitespace"] = "word_ngram",
) -> tuple[bytes, compact_theta_sketch]:
    """
    Compute a Theta sketch over a list of string values.

    Args:
        values:    iterable of string values (nulls pre-filtered by caller)
        num_perm:  kept for signature compatibility.
        tokenizer: tokenization strategy:
                   - "word_ngram"   (default) unigrams + bigrams; best for free-text columns
                   - "char_shingle" 3-char shingles; best for IDs and short structured strings
                   - "whitespace"   legacy whitespace split; plain bag-of-words

    Returns:
        (blob, compact_theta) — blob for storage, theta sketch for immediate comparison
    """
    m = update_theta_sketch(12)  # lg_k=12 gives similar space/accuracy to num_perm=128

    # Branch resolved once outside the loop — avoids per-iteration dispatch overhead.
    if tokenizer == "word_ngram":

        def _emit(val: str) -> None:
            words = val.split()
            for w in words:
                m.update(w)
            for i in range(len(words) - 1):
                m.update(words[i] + " " + words[i + 1])
    elif tokenizer == "char_shingle":

        def _emit(val: str) -> None:
            for token in _char_shingle_tokens(val):
                m.update(token)
    else:  # whitespace (legacy)

        def _emit(val: str) -> None:
            for token in val.split():
                m.update(token)

    for val in _distinct_lowered(values):
        _emit(val)

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


# Bounds the memory the vectorized paths may hold for a one-shot iterable.
# 64Ki float64 is 512 KiB per block — large enough that the per-block overhead
# is amortized, small enough to keep StreamingProvider's O(1) contract.
_BLOCK = 1 << 16

# Caps the minhash dedup memo. 64Ki short strings is a few MiB; beyond that the
# column is high-cardinality enough that dedup was never going to pay anyway.
_DEDUP_CAP = 1 << 16


def _clean(arr: np.ndarray) -> np.ndarray:
    """Contiguous float64 view with NaNs dropped."""
    arr = np.ascontiguousarray(arr.astype(np.float64, copy=False).ravel())
    return arr[~np.isnan(arr)]


def _iter_float_blocks(values: Iterable[float]) -> Iterator[np.ndarray]:
    """
    Yield contiguous float64 blocks with nulls/NaNs dropped.

    Already-materialized inputs (ndarray, pandas Series, list) are yielded as a
    single block — nothing is copied that wasn't already resident. One-shot
    iterables are consumed in fixed-size chunks so that StreamingProvider keeps
    iterating a file-backed generator in bounded memory; materializing them
    would make peak memory O(n).

    compute.py stays pandas-free (pandas is an optional extra), hence the
    duck-typed to_numpy check rather than an import.
    """
    if isinstance(values, np.ndarray):
        yield _clean(values)
    elif hasattr(values, "to_numpy"):  # pandas Series/Index
        yield _clean(values.to_numpy(copy=False))
    elif isinstance(values, list | tuple):
        yield _clean(np.asarray(values, dtype=np.float64))
    else:
        it = iter(values)
        while True:
            block = np.fromiter(islice(it, _BLOCK), dtype=np.float64)
            if block.size == 0:
                return
            yield _clean(block)
            if block.size < _BLOCK:
                return


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

    # kll_doubles_sketch.update has a native ndarray overload that ingests a
    # whole block in C, and numpy computes the moments in C too — together ~10x
    # faster than the per-row loop plus scalar Welford this replaces.
    # Block-wise (not whole-column) so a one-shot iterable stays O(1) memory;
    # see _iter_float_blocks.
    n = 0
    mean = 0.0
    m2 = 0.0
    for block in _iter_float_blocks(values):
        if not block.size:
            continue
        sk.update(block)
        # Chan et al. parallel variance — combines this block's moments with the
        # running ones without a second pass over the data.
        b_n = block.size
        b_mean = float(block.mean())
        b_m2 = float(((block - b_mean) ** 2).sum())
        if n == 0:
            n, mean, m2 = b_n, b_mean, b_m2
        else:
            total = n + b_n
            delta = b_mean - mean
            mean += delta * b_n / total
            m2 += b_m2 + delta * delta * n * b_n / total
            n = total

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
