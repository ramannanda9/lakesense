"""
IcebergBackend — storage backend using Apache Iceberg table formats.

Requires: pip install lakesense[iceberg]

Provides native time-travel for baselines and scalable append operations
via PyIceberg.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from lakesense.core.plugin import StorageBackend
from lakesense.core.result import InterpretationResult
from lakesense.sketches.compute import SketchRecord

logger = logging.getLogger(__name__)


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
        pa.field("sketch_config", pa.string()),
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


class IcebergBackend(StorageBackend):
    """
    Iceberg storage backend supporting PyIceberg writing.
    Uses native pa.timestamp columns for proper partition pruning and filter pushdown.
    """

    def __init__(
        self,
        catalog_name: str = "default",
        sketches_table: str = "default.lakesense_sketches",
        interpretations_table: str = "default.lakesense_interpretations",
        **catalog_properties: Any,
    ) -> None:
        try:
            from pyiceberg.catalog import load_catalog
            from pyiceberg.exceptions import NoSuchTableError
        except ImportError as e:
            raise ImportError("IcebergBackend requires pyiceberg. Install with: pip install lakesense[iceberg]") from e

        self.catalog = load_catalog(catalog_name, **catalog_properties)
        self._sketches_identifier = sketches_table
        self._interp_identifier = interpretations_table

        # Load or initialize tables
        try:
            self._sketches = self.catalog.load_table(self._sketches_identifier)
        except NoSuchTableError:
            logger.info("Creating sketches Iceberg table: %s", self._sketches_identifier)
            self._sketches = self.catalog.create_table(self._sketches_identifier, schema=_SKETCH_SCHEMA)

        try:
            self._interpretations = self.catalog.load_table(self._interp_identifier)
        except NoSuchTableError:
            logger.info("Creating interpretations Iceberg table: %s", self._interp_identifier)
            self._interpretations = self.catalog.create_table(self._interp_identifier, schema=_INTERP_SCHEMA)

    async def write_sketches(self, records: list[SketchRecord]) -> None:
        if not records:
            return

        table = pa.table(
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
        self._sketches.append(table)

    async def read_sketches(
        self,
        dataset_id: str,
        after_ts: str | datetime,
        before_ts: str | datetime | None = None,
    ) -> list[SketchRecord]:
        after_dt = _ensure_datetime(after_ts)
        before_dt = _ensure_datetime(before_ts) if before_ts else datetime.now(timezone.utc)

        # Push down filters to pyiceberg for proper partition pruning on timestamp columns
        try:
            from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

            expr = And(
                And(EqualTo("dataset_id", dataset_id), GreaterThanOrEqual("run_ts", after_dt)),
                LessThanOrEqual("run_ts", before_dt),
            )

            scan = self._sketches.scan(row_filter=expr)
            arrow_table = scan.to_arrow()
        except Exception as e:
            logger.warning("PyIceberg direct scan failed, falling back to full Arrow scan: %s", e)
            scan = self._sketches.scan()
            arrow_table = scan.to_arrow()
            # client side filtering using PyArrow compute
            mask = pc.and_(
                pc.and_(
                    pc.equal(arrow_table["dataset_id"], dataset_id),
                    pc.greater_equal(arrow_table["run_ts"], pa.scalar(after_dt, type=pa.timestamp("us", tz="UTC"))),
                ),
                pc.less_equal(arrow_table["run_ts"], pa.scalar(before_dt, type=pa.timestamp("us", tz="UTC"))),
            )
            arrow_table = arrow_table.filter(mask)

        records: list[SketchRecord] = []
        for row in arrow_table.to_pylist():
            run_ts = row["run_ts"]
            # PyArrow returns datetime objects from timestamp columns
            if isinstance(run_ts, str):
                run_ts = datetime.fromisoformat(run_ts)
            records.append(
                SketchRecord(
                    dataset_id=row["dataset_id"],
                    job_id=row["job_id"],
                    column=row["column"],
                    sketch_type=row["sketch_type"],
                    sketch_blob=row["sketch_blob"],
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
                "jaccard_delta": [result.drift_signals.jaccard_delta],
                "cardinality_ratio": [result.drift_signals.cardinality_ratio],
                "null_delta": [result.drift_signals.null_delta],
                "root_cause": [result.root_cause],
                "affected_urns": [json.dumps(result.affected_urns)],
                "owners": [json.dumps(result.owners)],
                "baseline_config": [json.dumps(result.baseline_config)],
                "metadata": [json.dumps(result.metadata)],
            },
            schema=_INTERP_SCHEMA,
        )
        self._interpretations.append(row)

    async def read_interpretation_history(
        self,
        dataset_id: str,
        limit: int = 10,
    ) -> list[InterpretationResult]:
        try:
            from pyiceberg.expressions import EqualTo

            expr = EqualTo("dataset_id", dataset_id)
            arrow_table = self._interpretations.scan(row_filter=expr).to_arrow()
        except Exception:
            arrow_table = self._interpretations.scan().to_arrow()
            mask = pc.equal(arrow_table["dataset_id"], dataset_id)
            arrow_table = arrow_table.filter(mask)

        pylist = arrow_table.to_pylist()

        # Sort by run_ts descending — works with both datetime and string
        pylist.sort(key=lambda x: x["run_ts"], reverse=True)

        return [InterpretationResult.from_dict(r) for r in pylist[:limit]]

    async def get_snapshot(
        self,
        dataset_id: str,
        snapshot_id: str,
    ) -> list[SketchRecord]:
        """
        Fetch records using Iceberg's native time-travel given an Iceberg snapshot ID.
        """
        # Parse the snapshot ID into an integer (Iceberg snapshots are int64)
        try:
            snap_int = int(snapshot_id)
            scan = self._sketches.scan(snapshot_id=snap_int)
            arrow_table = scan.to_arrow()
        except ValueError:
            logger.warning(
                "snapshot_id %s is not a valid Iceberg integer snapshot ID. Falling back to timestamp filter.",
                snapshot_id,
            )
            return await self.read_sketches(dataset_id, after_ts=snapshot_id, before_ts=snapshot_id)

        mask = pc.equal(arrow_table["dataset_id"], dataset_id)
        arrow_table = arrow_table.filter(mask)

        records: list[SketchRecord] = []
        for row in arrow_table.to_pylist():
            run_ts = row["run_ts"]
            if isinstance(run_ts, str):
                run_ts = datetime.fromisoformat(run_ts)
            records.append(
                SketchRecord(
                    dataset_id=row["dataset_id"],
                    job_id=row["job_id"],
                    column=row["column"],
                    sketch_type=row["sketch_type"],
                    sketch_blob=row["sketch_blob"],
                    run_ts=run_ts,
                    num_perm=row.get("num_perm"),
                    num_rows=row.get("num_rows"),
                    null_count=row.get("null_count"),
                    sketch_config=json.loads(row.get("sketch_config") or "{}"),
                )
            )
        return records
