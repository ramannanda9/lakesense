"""
ParquetBackend — zero-infrastructure storage using plain Parquet files.

Layout on disk:
    {base_path}/
        sketches/
            dataset={dataset_id}/
                {run_ts_date}.parquet
        interpretations/
            dataset={dataset_id}/
                {run_ts_date}.parquet

No catalog, no metastore, no cluster required.
Suitable for local development, single-node deployments, and CI.

For production lakehouses, use IcebergBackend (v0.2).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lakesense.core.plugin import StorageBackend
from lakesense.core.result import InterpretationResult
from lakesense.sketches.compute import SketchRecord


def _ensure_datetime(val: str | datetime) -> datetime:
    """Normalize a str or datetime into a timezone-aware datetime."""
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val
    return datetime.fromisoformat(val)


_SKETCH_SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string()),
        pa.field("job_id", pa.string()),
        pa.field("column", pa.string()),
        pa.field("sketch_type", pa.string()),
        pa.field("sketch_blob", pa.binary()),
        pa.field("run_ts", pa.timestamp("us", tz="UTC")),
        pa.field("num_perm", pa.int32()),
        pa.field("num_rows", pa.int64()),
        pa.field("null_count", pa.int64()),
        pa.field("sketch_config", pa.string()),  # JSON
    ]
)

_INTERP_SCHEMA = pa.schema(
    [
        pa.field("dataset_id", pa.string()),
        pa.field("job_id", pa.string()),
        pa.field("run_ts", pa.timestamp("us", tz="UTC")),
        pa.field("executed_at", pa.timestamp("us", tz="UTC")),
        pa.field("severity", pa.string()),
        pa.field("summary", pa.string()),
        pa.field("jaccard_delta", pa.float64()),
        pa.field("cardinality_ratio", pa.float64()),
        pa.field("null_delta", pa.float64()),
        pa.field("root_cause", pa.string()),
        pa.field("affected_urns", pa.string()),
        pa.field("owners", pa.string()),
        pa.field("baseline_config", pa.string()),
        pa.field("metadata", pa.string()),
    ]
)


class ParquetBackend(StorageBackend):
    """
    Parquet-based storage backend. Zero dependencies beyond pyarrow.

    Args:
        base_path: directory for all lakesense data (created if absent)

    Usage:
        backend = ParquetBackend("./lakesense_data")
        framework = SketchFramework(storage=backend)
    """

    def __init__(self, base_path: str | Path) -> None:
        self._base = Path(base_path)
        self._sketches_root = self._base / "sketches"
        self._interp_root = self._base / "interpretations"

    def _sketch_job_path(self, dataset_id: str, run_ts: datetime, job_id: str) -> Path:
        p = self._sketches_root / f"dataset={dataset_id}" / f"date={run_ts.date()}"
        p.mkdir(parents=True, exist_ok=True)
        safe_job = job_id.replace("/", "_").replace(":", "_")
        ts = run_ts.strftime("%H%M%S%f")
        return p / f"run_{safe_job}_{ts}.parquet"

    def _interp_path(self, dataset_id: str, run_ts: datetime, job_id: str) -> Path:
        p = self._interp_root / f"dataset={dataset_id}" / f"date={run_ts.date()}"
        p.mkdir(parents=True, exist_ok=True)
        safe_job = job_id.replace("/", "_").replace(":", "_")
        return p / f"{safe_job}_{run_ts.strftime('%H%M%S%f')}.parquet"

    def _write_parquet(self, path: Path, table: pa.Table) -> None:
        """Write a single-run Parquet file. Each run gets its own file."""
        pq.write_table(table, path, compression="snappy")

    async def write_sketches(self, records: list[SketchRecord]) -> None:
        if not records:
            return

        first = records[0]
        row = pa.table(
            {
                "dataset_id": [r.dataset_id for r in records],
                "job_id": [r.job_id for r in records],
                "column": [r.column for r in records],
                "sketch_type": [r.sketch_type for r in records],
                "sketch_blob": [r.sketch_blob for r in records],
                "run_ts": [r.run_ts for r in records],
                "num_perm": [r.num_perm for r in records],
                "num_rows": [r.num_rows for r in records],
                "null_count": [r.null_count for r in records],
                "sketch_config": [json.dumps(r.sketch_config) for r in records],
            },
            schema=_SKETCH_SCHEMA,
        )

        self._write_parquet(
            self._sketch_job_path(first.dataset_id, first.run_ts, first.job_id),
            row,
        )

    async def read_sketches(
        self,
        dataset_id: str,
        after_ts: str | datetime,
        before_ts: str | datetime | None = None,
    ) -> list[SketchRecord]:
        dataset_dir = self._sketches_root / f"dataset={dataset_id}"
        if not dataset_dir.exists():
            return []

        records: list[SketchRecord] = []
        after_dt = _ensure_datetime(after_ts)
        before_dt = _ensure_datetime(before_ts) if before_ts else datetime.now(timezone.utc)

        # glob all parquet files across date= subdirectories
        for pfile in sorted(dataset_dir.rglob("*.parquet")):
            tbl = pq.read_table(pfile, schema=_SKETCH_SCHEMA)
            for row in tbl.to_pylist():
                run_ts = row["run_ts"]
                # PyArrow returns datetime from timestamp columns
                if isinstance(run_ts, str):
                    run_ts = datetime.fromisoformat(run_ts)
                if after_dt <= run_ts <= before_dt:
                    records.append(
                        SketchRecord(
                            dataset_id=row["dataset_id"],
                            job_id=row["job_id"],
                            column=row["column"],
                            sketch_type=row["sketch_type"],
                            sketch_blob=bytes(row["sketch_blob"]),
                            run_ts=run_ts,
                            num_perm=row.get("num_perm"),
                            num_rows=row.get("num_rows"),
                            null_count=row.get("null_count"),
                            sketch_config=json.loads(row.get("sketch_config") or "{}"),
                        )
                    )
        return records

    async def write_interpretation(self, result: InterpretationResult) -> None:
        row = pa.table(
            {
                "dataset_id": [result.dataset_id],
                "job_id": [result.job_id],
                "run_ts": [result.run_ts],
                "executed_at": [result.executed_at],
                "severity": [result.severity.value],
                "summary": [result.summary],
                "jaccard_delta": [result.dataset_drift_summary.jaccard_delta],
                "cardinality_ratio": [result.dataset_drift_summary.cardinality_ratio],
                "null_delta": [result.dataset_drift_summary.null_delta],
                "root_cause": [result.root_cause],
                "affected_urns": [json.dumps(result.affected_urns)],
                "owners": [json.dumps(result.owners)],
                "baseline_config": [json.dumps(result.baseline_config)],
                "metadata": [json.dumps(result.metadata)],
            },
            schema=_INTERP_SCHEMA,
        )
        self._write_parquet(self._interp_path(result.dataset_id, result.run_ts, result.job_id), row)

    async def read_interpretation_history(
        self,
        dataset_id: str,
        limit: int = 10,
    ) -> list[InterpretationResult]:
        dataset_dir = self._interp_root / f"dataset={dataset_id}"
        if not dataset_dir.exists():
            return []

        rows: list[dict] = []
        for pfile in sorted(dataset_dir.rglob("*.parquet"), reverse=True):
            tbl = pq.read_table(pfile, schema=_INTERP_SCHEMA)
            rows.extend(tbl.to_pylist())
            if len(rows) >= limit:
                break

        return [InterpretationResult.from_dict(r) for r in rows[:limit]]

    async def get_snapshot(
        self,
        dataset_id: str,
        snapshot_id: str,
    ) -> list[SketchRecord]:
        """For Parquet backend, snapshot_id is an ISO timestamp — returns exact-match records."""
        return await self.read_sketches(
            dataset_id=dataset_id,
            after_ts=snapshot_id,
            before_ts=snapshot_id,
        )
