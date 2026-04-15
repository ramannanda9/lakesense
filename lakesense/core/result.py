"""
Central data contract for lakesense.

InterpretationResult flows through the entire pipeline:
  Tier 1 (base interpreter) populates severity + summary.
  Tier 2 plugins enrich it with root_cause, owners, agent_trace, etc.
  StorageBackend persists the final state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class Severity(str, Enum):
    OK = "ok"
    WARN = "warn"
    ALERT = "alert"

    def is_actionable(self) -> bool:
        return self in (Severity.WARN, Severity.ALERT)


@dataclass
class DriftSignals:
    """
    Per-column drift metrics computed by comparing a single column's sketch to baseline.
    All fields are optional — populated based on which sketch type was computed.
    """

    # MinHash / Jaccard
    jaccard_current: float | None = None
    jaccard_baseline: float | None = None
    jaccard_delta: float | None = None  # current - baseline; negative = more drift

    # HyperLogLog cardinality
    cardinality_current: int | None = None
    cardinality_baseline: int | None = None
    cardinality_ratio: float | None = None  # current / baseline

    # KLL quantile shifts (p50, p90, p99)
    quantile_shifts: dict[str, float] = field(default_factory=dict)

    # Kolmogorov-Smirnov boolean divergence test
    ks_test_divergent: bool | None = None

    # Count-Min Sketch heavy hitters delta
    heavy_hitter_overlap: float | None = None

    # Raw scalar metrics
    null_rate: float | None = None
    null_delta: float | None = None

    # Null rate change (from profile)
    max_null_rate_delta: float | None = None

    # Integer / float range violations (from profile)
    range_min_delta: float | None = None  # current_min - baseline_min
    range_max_delta: float | None = None  # current_max - baseline_max

    # Categorical distribution shift (from profile)
    categorical_top_shift: float | None = None

    # Boolean ratio change (from profile)
    bool_true_rate_delta: float | None = None

    # String length change (from profile)
    str_mean_len_delta: float | None = None


@dataclass
class DatasetDriftSummary:
    """
    Dataset-level drift summary produced by aggregate_signals.
    Carries the worst value per metric and which column it came from.
    Dataset-level signals (schema drift, row count) have no column attribution.
    """

    # Sketch signals — per-metric worst value + column attribution
    jaccard_delta: float | None = None
    jaccard_worst_column: str | None = None

    cardinality_ratio: float | None = None
    cardinality_worst_column: str | None = None

    quantile_shifts: dict[str, float] = field(default_factory=dict)
    ks_test_divergent: bool | None = None

    # Profile signals — per-metric worst value + column attribution
    max_null_rate_delta: float | None = None
    null_rate_worst_column: str | None = None

    bool_true_rate_delta: float | None = None
    bool_rate_worst_column: str | None = None

    categorical_top_shift: float | None = None
    categorical_worst_column: str | None = None

    range_min_delta: float | None = None
    range_worst_column: str | None = None

    # Dataset-level — no column attribution
    row_count_delta: float | None = None
    missing_columns: list[str] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)

    # Pass-through scalar (used by storage/agent)
    null_delta: float | None = None

    def worst_signal(self) -> str:
        """Human-readable summary of the most significant drift signals with column attribution."""
        parts = []
        if self.jaccard_delta is not None:
            col = f" on {self.jaccard_worst_column}" if self.jaccard_worst_column else ""
            parts.append(f"jaccard_delta={self.jaccard_delta:.3f}{col}")
        if self.cardinality_ratio is not None:
            col = f" on {self.cardinality_worst_column}" if self.cardinality_worst_column else ""
            parts.append(f"cardinality_ratio={self.cardinality_ratio:.2f}{col}")
        if self.quantile_shifts:
            worst_q = max(self.quantile_shifts, key=lambda k: abs(self.quantile_shifts[k]))
            parts.append(f"{worst_q}_shift={self.quantile_shifts[worst_q]:.3f}")
        if self.ks_test_divergent:
            parts.append("distribution_divergent=True")
        if self.max_null_rate_delta is not None and self.max_null_rate_delta > 0.01:
            col = f" on {self.null_rate_worst_column}" if self.null_rate_worst_column else ""
            parts.append(f"null_rate_delta={self.max_null_rate_delta:.3f}{col}")
        if self.row_count_delta is not None and abs(self.row_count_delta - 1.0) > 0.05:
            parts.append(f"row_count_ratio={self.row_count_delta:.2f}")
        if self.missing_columns:
            parts.append(f"missing_columns={self.missing_columns}")
        if self.bool_true_rate_delta is not None and self.bool_true_rate_delta > 0.05:
            col = f" on {self.bool_rate_worst_column}" if self.bool_rate_worst_column else ""
            parts.append(f"bool_rate_delta={self.bool_true_rate_delta:.3f}{col}")
        if self.categorical_top_shift is not None and self.categorical_top_shift > 0.2:
            col = f" on {self.categorical_worst_column}" if self.categorical_worst_column else ""
            parts.append(f"categorical_shift={self.categorical_top_shift:.2f}{col}")
        return ", ".join(parts) if parts else "no signals"


@dataclass
class InterpretationResult:
    """
    The single data contract that flows through the entire lakesense pipeline.

    Tier 1 (base interpreter) sets:
        dataset_id, job_id, run_ts, severity, summary, dataset_drift_summary, baseline_config

    Tier 2 plugins enrich:
        root_cause, affected_urns, owners, agent_trace, metadata
    """

    # Identity
    dataset_id: str
    job_id: str
    # data_interval_end — the period this data covers, used for partitioning + baseline
    run_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # executed_at — wall clock time the job ran; differs from run_ts during backfills
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # Tier 1 output — always populated after base interpretation
    severity: Severity = Severity.OK
    summary: str = ""
    dataset_drift_summary: DatasetDriftSummary = field(default_factory=DatasetDriftSummary)
    baseline_config: dict[str, Any] = field(default_factory=dict)

    # Tier 2 enrichment — populated by agent plugin
    root_cause: str | None = None
    affected_urns: list[str] = field(default_factory=list)
    owners: list[str] = field(default_factory=list)
    agent_trace: list[dict[str, Any]] = field(default_factory=list)

    # Sidecar metadata — plugins write here (e.g. slack_ts, jira_ticket)
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_actionable(self) -> bool:
        return self.severity.is_actionable()

    def is_agent_enriched(self) -> bool:
        return self.root_cause is not None

    def to_dict(self) -> dict[str, Any]:
        """Flat dict suitable for Parquet/DuckDB row insertion."""
        return {
            "dataset_id": self.dataset_id,
            "job_id": self.job_id,
            "run_ts": self.run_ts.isoformat(),
            "executed_at": self.executed_at.isoformat(),
            "severity": self.severity.value,
            "summary": self.summary,
            "jaccard_delta": self.dataset_drift_summary.jaccard_delta,
            "jaccard_worst_column": self.dataset_drift_summary.jaccard_worst_column,
            "cardinality_ratio": self.dataset_drift_summary.cardinality_ratio,
            "cardinality_worst_column": self.dataset_drift_summary.cardinality_worst_column,
            "quantile_shifts": self.dataset_drift_summary.quantile_shifts,
            "max_null_rate_delta": self.dataset_drift_summary.max_null_rate_delta,
            "null_rate_worst_column": self.dataset_drift_summary.null_rate_worst_column,
            "bool_true_rate_delta": self.dataset_drift_summary.bool_true_rate_delta,
            "bool_rate_worst_column": self.dataset_drift_summary.bool_rate_worst_column,
            "categorical_top_shift": self.dataset_drift_summary.categorical_top_shift,
            "categorical_worst_column": self.dataset_drift_summary.categorical_worst_column,
            "range_min_delta": self.dataset_drift_summary.range_min_delta,
            "range_worst_column": self.dataset_drift_summary.range_worst_column,
            "ks_test_divergent": self.dataset_drift_summary.ks_test_divergent,
            "null_delta": self.dataset_drift_summary.null_delta,
            "row_count_delta": self.dataset_drift_summary.row_count_delta,
            "missing_columns": self.dataset_drift_summary.missing_columns,
            "new_columns": self.dataset_drift_summary.new_columns,
            "root_cause": self.root_cause,
            "affected_urns": self.affected_urns,
            "owners": self.owners,
            "baseline_config": self.baseline_config,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> InterpretationResult:
        signals = DatasetDriftSummary(
            jaccard_delta=d.get("jaccard_delta"),
            jaccard_worst_column=d.get("jaccard_worst_column"),
            cardinality_ratio=d.get("cardinality_ratio"),
            cardinality_worst_column=d.get("cardinality_worst_column"),
            quantile_shifts=d.get("quantile_shifts") or {},
            max_null_rate_delta=d.get("max_null_rate_delta"),
            null_rate_worst_column=d.get("null_rate_worst_column"),
            bool_true_rate_delta=d.get("bool_true_rate_delta"),
            bool_rate_worst_column=d.get("bool_rate_worst_column"),
            categorical_top_shift=d.get("categorical_top_shift"),
            categorical_worst_column=d.get("categorical_worst_column"),
            range_min_delta=d.get("range_min_delta"),
            range_worst_column=d.get("range_worst_column"),
            ks_test_divergent=d.get("ks_test_divergent"),
            null_delta=d.get("null_delta"),
            row_count_delta=d.get("row_count_delta"),
            missing_columns=d.get("missing_columns") or [],
            new_columns=d.get("new_columns") or [],
        )

        def _parse_ts(val: Any) -> datetime:
            """Parse a timestamp value — handles both datetime objects and ISO strings."""
            if isinstance(val, datetime):
                if val.tzinfo is None:
                    return val.replace(tzinfo=timezone.utc)
                return val
            return datetime.fromisoformat(str(val))

        # executed_at falls back to run_ts for records written before this field existed
        executed_at_raw = d.get("executed_at") or d["run_ts"]
        return cls(
            dataset_id=d["dataset_id"],
            job_id=d["job_id"],
            run_ts=_parse_ts(d["run_ts"]),
            executed_at=_parse_ts(executed_at_raw),
            severity=Severity(d["severity"]),
            summary=d.get("summary", ""),
            dataset_drift_summary=signals,
            root_cause=d.get("root_cause"),
            affected_urns=d.get("affected_urns", []),
            owners=d.get("owners", []),
            baseline_config=d.get("baseline_config", {}),
            metadata=d.get("metadata", {}),
        )
