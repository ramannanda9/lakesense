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
    Derived metrics computed by comparing current sketches to baseline.
    All fields are optional — populated based on which sketch types are configured.
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
    row_count: int | None = None
    null_rate: float | None = None
    null_delta: float | None = None

    # Row count change (from profile)
    row_count_delta: float | None = None  # ratio: current / baseline

    # Null rate change per column (from profile) — worst across all columns
    max_null_rate_delta: float | None = None

    # Integer / float range violations (from profile)
    range_min_delta: float | None = None  # current_min - baseline_min
    range_max_delta: float | None = None  # current_max - baseline_max

    # Categorical distribution shift (from profile)
    # Proportion of top-N values that changed between baseline and current
    categorical_top_shift: float | None = None

    # Boolean ratio change (from profile)
    bool_true_rate_delta: float | None = None

    # String length change (from profile)
    str_mean_len_delta: float | None = None

    # Columns missing vs baseline (schema drift)
    missing_columns: list[str] = field(default_factory=list)
    new_columns: list[str] = field(default_factory=list)

    def worst_signal(self) -> str:
        """Human-readable summary of the most significant drift signal."""
        signals = []
        if self.jaccard_delta is not None:
            signals.append(f"jaccard_delta={self.jaccard_delta:.3f}")
        if self.cardinality_ratio is not None:
            signals.append(f"cardinality_ratio={self.cardinality_ratio:.2f}")
        if self.quantile_shifts:
            worst_q = max(self.quantile_shifts, key=lambda k: abs(self.quantile_shifts[k]))
            signals.append(f"{worst_q}_shift={self.quantile_shifts[worst_q]:.3f}")
        if self.ks_test_divergent:
            signals.append("distribution_divergent=True")
        if self.max_null_rate_delta is not None and self.max_null_rate_delta > 0.01:
            signals.append(f"null_rate_delta={self.max_null_rate_delta:.3f}")
        if self.row_count_delta is not None and abs(self.row_count_delta - 1.0) > 0.05:
            signals.append(f"row_count_ratio={self.row_count_delta:.2f}")
        if self.missing_columns:
            signals.append(f"missing_columns={self.missing_columns}")
        if self.bool_true_rate_delta is not None and self.bool_true_rate_delta > 0.05:
            signals.append(f"bool_rate_delta={self.bool_true_rate_delta:.3f}")
        if self.categorical_top_shift is not None and self.categorical_top_shift > 0.2:
            signals.append(f"categorical_shift={self.categorical_top_shift:.2f}")
        return ", ".join(signals) if signals else "no signals"


@dataclass
class InterpretationResult:
    """
    The single data contract that flows through the entire lakesense pipeline.

    Tier 1 (base interpreter) sets:
        dataset_id, job_id, run_ts, severity, summary, drift_signals, baseline_config

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
    drift_signals: DriftSignals = field(default_factory=DriftSignals)
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
            "jaccard_delta": self.drift_signals.jaccard_delta,
            "cardinality_ratio": self.drift_signals.cardinality_ratio,
            "ks_test_divergent": self.drift_signals.ks_test_divergent,
            "null_delta": self.drift_signals.null_delta,
            "root_cause": self.root_cause,
            "affected_urns": self.affected_urns,
            "owners": self.owners,
            "baseline_config": self.baseline_config,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> InterpretationResult:
        signals = DriftSignals(
            jaccard_delta=d.get("jaccard_delta"),
            cardinality_ratio=d.get("cardinality_ratio"),
            ks_test_divergent=d.get("ks_test_divergent"),
            null_delta=d.get("null_delta"),
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
            drift_signals=signals,
            root_cause=d.get("root_cause"),
            affected_urns=d.get("affected_urns", []),
            owners=d.get("owners", []),
            baseline_config=d.get("baseline_config", {}),
            metadata=d.get("metadata", {}),
        )
