"""Pandas sketch compute provider."""

from datetime import datetime, timezone
from typing import Any

from lakesense.sketches.compute import (
    SketchRecord,
    compute_hll,
    compute_kll,
    compute_minhash,
)
from lakesense.sketches.profile import profile_dataframe, profiles_to_sketch_records
from lakesense.sketches.providers.base import SketchProvider


class PandasProvider(SketchProvider):
    """
    SketchProvider implementation for local pandas DataFrames.
    Extensively relies on vectorized pandas C-api operations where available
    (e.g., in profile.py or null counts) to maximize single-machine performance.
    """

    def sketch(
        self,
        data: Any,
        dataset_id: str,
        job_id: str,
        id_columns: list[str] | None = None,
        numeric_columns: list[str] | None = None,
        text_columns: list[str] | None = None,
        run_ts: datetime | None = None,
        include_profiles: bool = True,
    ) -> list[SketchRecord]:
        """
        Compute sketches for a local pandas dataframe.
        """
        records: list[SketchRecord] = []
        ts = run_ts or datetime.now(timezone.utc)
        n_rows = len(data)

        def _base(col: str, stype: str, blob: bytes, **kwargs: Any) -> SketchRecord:
            # highly-vectorized pandas null checking
            null_count = int(data[col].isna().sum())
            return SketchRecord(
                dataset_id=dataset_id,
                job_id=job_id,
                column=col,
                sketch_type=stype,
                sketch_blob=blob,
                run_ts=ts,
                num_rows=n_rows,
                null_count=null_count,
                **kwargs,
            )

        for col in text_columns or []:
            vals = data[col].dropna().tolist()
            blob, _ = compute_minhash(vals, tokenizer="word_ngram")
            records.append(_base(col, "minhash", blob, num_perm=128, sketch_config={"tokenizer": "word_ngram"}))

        for col in id_columns or []:
            vals = data[col].dropna().tolist()
            blob, _ = compute_hll(vals)
            records.append(_base(col, "hll", blob, sketch_config={"p": 12}))

        for col in numeric_columns or []:
            vals = data[col].dropna().tolist()
            if vals:
                blob, quantiles = compute_kll(vals)
                records.append(_base(col, "kll", blob, sketch_config={"quantiles": quantiles}))

        if include_profiles:
            profiles = profile_dataframe(data)
            records.extend(profiles_to_sketch_records(profiles, dataset_id, job_id, run_ts=ts))

        return records
