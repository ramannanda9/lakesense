"""
Plugin interfaces for lakesense.

SketchPlugin  — Tier 2 action plugins (alerting, investigation, storage).
StorageBackend — pluggable persistence (Parquet, DuckDB, Iceberg, Delta).

Both follow the same pattern:
  - should_run() lets each implementation own its trigger condition.
  - run() receives an InterpretationResult and returns an enriched one.
  - Plugins are composable and ordered by registration sequence.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lakesense.core.result import InterpretationResult
    from lakesense.sketches.compute import SketchRecord


class SketchPlugin(ABC):
    """
    Base class for all Tier 2 plugins.

    Plugins are registered with SketchFramework and execute in order.
    Each plugin receives the result as enriched by all prior plugins.

    Example implementation:
        class MyPlugin(SketchPlugin):
            def should_run(self, result: InterpretationResult) -> bool:
                return result.severity == Severity.ALERT

            async def run(self, result: InterpretationResult) -> InterpretationResult:
                # do work, enrich result
                result.metadata["my_plugin"] = "ran"
                return result
    """

    @property
    def name(self) -> str:
        """Plugin name for logging. Defaults to class name."""
        return self.__class__.__name__

    @abstractmethod
    def should_run(self, result: InterpretationResult) -> bool:
        """Return True if this plugin should execute for this result."""
        ...

    @abstractmethod
    async def run(self, result: InterpretationResult) -> InterpretationResult:
        """
        Execute plugin logic. Must return the result (enriched or unchanged).
        Never mutate shared state outside of result.metadata.
        """
        ...


class StorageBackend(ABC):
    """
    Pluggable persistence layer for sketch metrics and interpretation results.

    Implementations:
        ParquetBackend  — plain Parquet files, zero infrastructure
        DuckDBBackend   — DuckDB on top of Parquet, adds SQL queries
        IcebergBackend  — Apache Iceberg, full time-travel + catalog (v0.2)
        DeltaBackend    — Delta Lake, for Databricks shops (v0.3)
    """

    @abstractmethod
    async def write_sketches(self, records: list[SketchRecord]) -> None:
        """Persist a batch of sketch records for a single job/dataset run."""
        ...

    @abstractmethod
    async def read_sketches(
        self,
        dataset_id: str,
        after_ts: str | datetime,
        before_ts: str | datetime | None = None,
    ) -> list[SketchRecord]:
        """
        Fetch sketch records for a dataset within a time range.
        Used by the baseline engine to build rolling window baselines.
        """
        ...

    @abstractmethod
    async def write_interpretation(self, result: InterpretationResult) -> None:
        """Persist a final InterpretationResult after all plugins have run."""
        ...

    @abstractmethod
    async def read_interpretation_history(
        self,
        dataset_id: str,
        limit: int = 10,
    ) -> list[InterpretationResult]:
        """
        Fetch recent interpretation results for a dataset.
        Passed to the LLM as trend context in Tier 1 interpretation.
        """
        ...

    @abstractmethod
    async def get_snapshot(
        self,
        dataset_id: str,
        snapshot_id: str,
    ) -> list[SketchRecord]:
        """
        Fetch sketch records pinned to a specific snapshot.
        Used by the snapshot baseline strategy.
        For Parquet/DuckDB: snapshot_id is an ISO timestamp.
        For Iceberg: snapshot_id maps to a table snapshot.
        """
        ...
