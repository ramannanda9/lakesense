# Changelog

All notable changes to lakesense will be documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.1.0] — 2024-XX-XX

### Added
- `SketchFramework` — two-tier orchestrator (base interpret + plugin chain)
- `SketchPlugin` ABC — extensible plugin interface
- `StorageBackend` ABC — pluggable persistence interface
- `InterpretationResult` + `DriftSignals` + `Severity` — core data contracts
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
