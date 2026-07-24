# Changelog

All notable changes to lakesense will be documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.3.1] — 2026-07-24

### Changed
- `compute_kll` ingests values through the native `datasketches` ndarray overload
  instead of a per-row Python loop, with moments computed by numpy rather than a
  scalar Welford accumulator (~10x on 1M rows). **Quantile values may differ
  slightly from 0.3.0** — KLL's reservoir sampling depends on the order and
  batching of updates. The shift is within KLL's inherent rank error at k=200 and
  is unbiased, but a baseline computed under 0.3.0 may show a small quantile delta
  when compared against a 0.3.1 run. Recompute baselines to avoid a one-time
  false drift signal. `mean`/`std` are unaffected (identical to 1e-9).
- `compute_minhash` tokenizes each distinct value once instead of every row
  (1.45x–20x depending on how often values repeat). Theta sketch output is
  bit-identical to 0.3.0 — the sketch is a set, so deduplicating inputs cannot
  change the result.
- `PandasProvider` passes numpy arrays to the sketch functions rather than
  materializing intermediate Python lists.
- `compute_hll` is deliberately unchanged. `hll_sketch.update` has no array
  overload, and deduplicating inputs regresses ~21% on high-cardinality columns
  at Spark's default 10k-row batch size — precisely the `id_columns` case.

### Fixed
- `compute_kll` and `compute_minhash` keep peak memory constant for one-shot
  iterables, preserving `StreamingProvider`'s O(1) contract over file-backed
  generators. Values are consumed in bounded blocks and the minhash dedup memo is
  capped, rather than materializing the column.
- The Spark provider test no longer converts a JVM startup failure into a skip
  when running under CI, where a silent skip made a green run meaningless.
- `publish.yml` now runs the provider test suites before publishing; previously
  only `tests/unit/` gated a release.
- The benchmark CI job no longer fails the build when a performance alert fires —
  it lacked the `pull-requests`/`issues` permissions its alert comment requires.

### Added
- First test coverage for `StreamingProvider` and for generator inputs to the
  sketch functions, including a peak-memory regression guard.

## [0.3.0] — 2026-05-05

### Added
- `lakesense.lineage` — OpenLineage `DataQualityAssertionsDatasetFacet` converter for WAP gating
- `to_openlineage_facets(result)` / `to_openlineage_assertions(result)` — standalone functions returning plain dicts matching the OL JSON schema (no extra dependency required)
- `AssertionThresholds` — configurable pass/fail thresholds (defaults match `_heuristic_severity` warn level)
- Top-level `lakesense_quality_check` assertion driven by `result.severity` (captures heuristic + optional LLM verdict)
- Per-signal assertions with column attribution (jaccard, cardinality, null rate, KS test, etc.)
- `expected` / `actual` diagnostic fields on every assertion dict for richer OL event context

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
