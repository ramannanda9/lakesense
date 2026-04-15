# Changelog

All notable changes to lakesense will be documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.2.2] — 2026-04-14

### Added
- `DatasetDriftSummary` — new dataset-level aggregate type with per-metric column attribution (`jaccard_worst_column`, `cardinality_worst_column`, `null_rate_worst_column`, etc.)
- Schema drift (`missing_columns`, `new_columns`) and `row_count_delta` are now computed inline in `base_interpret` from column profiles and applied to the `DatasetDriftSummary`
- `DatasetDriftSummary` exported from the top-level `lakesense` package

### Changed
- `InterpretationResult.drift_signals` renamed to `dataset_drift_summary` (type `DatasetDriftSummary`)
- `aggregate_signals` signature changed from `list[DriftSignals]` to `dict[str, DriftSignals]` — callers key by column name
- `compute_profile_signals` now returns `dict[str, DriftSignals]` (per-column only); schema drift and row count are computed upstream in `base_interpret`
- `_heuristic_severity` and `_build_prompt` updated to accept `DatasetDriftSummary`
- `DriftSignals` is now strictly per-column (removed `worst_signal`, `missing_columns`, `new_columns`, `row_count_delta` — these live on `DatasetDriftSummary`)

## [0.1.0] — 2024-XX-XX

### Added
- `SketchFramework` — two-tier orchestrator (base interpret + plugin chain)
- `SketchPlugin` ABC — extensible plugin interface
- `StorageBackend` ABC — pluggable persistence interface
- `InterpretationResult` + `DriftSignals` + `DatasetDriftSummary` + `Severity` — core data contracts
- Sketch compute: MinHash, HyperLogLog, KLL (via datasketch + numpy)
- Sketch merge engine: rolling window, snapshot, EWMA baseline strategies
- Drift signal computation: Jaccard delta, cardinality ratio, quantile shifts
- `ParquetBackend` — zero-infra Parquet storage
- `DuckDBBackend` — DuckDB-powered Parquet with SQL query API
- Tier 1 LLM interpreter (Anthropic claude-sonnet) with heuristic fallback
- `StoragePlugin` — auto-registered terminal persistence plugin
- `SlackAlertPlugin` — formatted Slack alerts with owner @mentions
- `BaselineConfig` — user-defined baseline strategy per dataset
- Quickstart example (`examples/quickstart.py`)
- 30 unit tests across core, sketches, and storage layers
