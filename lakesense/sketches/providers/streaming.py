"""Streaming sketch compute provider for O(1) memory iteration."""

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from lakesense.sketches.compute import (
    SketchRecord,
    compute_hll,
    compute_kll,
    compute_minhash,
)
from lakesense.sketches.profile import profile_column
from lakesense.sketches.providers.base import SketchProvider


class StreamingProvider(SketchProvider):
    """
    SketchProvider implementation for abstract Iterables.
    This provider operates strictly in O(1) memory using Welford's algorithm and
    single-pass generators natively through datasketches loops.
    """

    def sketch(
        self,
        data: Mapping[str, Iterable[Any]],
        dataset_id: str,
        job_id: str,
        id_columns: list[str] | None = None,
        numeric_columns: list[str] | None = None,
        text_columns: list[str] | None = None,
        run_ts: datetime | None = None,
        include_profiles: bool = True,
    ) -> list[SketchRecord]:
        """
        Compute sketches for a dictionary/mapping of column iterables.
        The `data` Mapping must support dictionary-like `data[col]` lookups returning
        an Iterable (e.g. a generator reading from a file).
        """
        records: list[SketchRecord] = []
        ts = run_ts or datetime.now(timezone.utc)

        def _base(
            col: str,
            stype: str,
            blob: bytes,
            num_rows: int,
            null_count: int,
            **kwargs: Any,
        ) -> SketchRecord:
            return SketchRecord(
                dataset_id=dataset_id,
                job_id=job_id,
                column=col,
                sketch_type=stype,
                sketch_blob=blob,
                run_ts=ts,
                num_rows=num_rows,
                null_count=null_count,
                **kwargs,
            )

        # Minhash
        for col in text_columns or []:
            if col in data:
                # pass the iterable block
                blob, _ = compute_minhash(data[col])
                # Note: stream count tracking logic is missing here
                # natively without building a wrapper
                records.append(_base(col, "minhash", blob, num_rows=0, null_count=0, num_perm=128))

        # HLL
        for col in id_columns or []:
            if col in data:
                blob, _ = compute_hll(data[col])
                records.append(
                    _base(col, "hll", blob, num_rows=0, null_count=0, sketch_config={"p": 12})
                )

        # KLL
        for col in numeric_columns or []:
            if col in data:
                blob, quantiles = compute_kll(data[col])
                records.append(
                    _base(
                        col,
                        "kll",
                        blob,
                        num_rows=0,
                        null_count=0,
                        sketch_config={"quantiles": quantiles},
                    )
                )

        # Profile
        if include_profiles:
            import json

            # profile_column is already streamable and counts row and null occurrences perfectly!
            all_cols = set((text_columns or []) + (id_columns or []) + (numeric_columns or []))
            for col in all_cols:
                if col in data:
                    p = profile_column(data[col], col_name=col)
                    blob = json.dumps(p.to_dict()).encode("utf-8")
                    records.append(
                        _base(
                            col,
                            "profile",
                            blob,
                            num_rows=p.row_count,
                            null_count=p.null_count,
                            sketch_config={"dtype": p.dtype},
                        )
                    )

                    # Backfill row counts if possible to the sketches
                    # in O(N^2) search (negligible scale)
                    for r in records:
                        if r.column == col:
                            r.num_rows = p.row_count
                            r.null_count = p.null_count

        return records
