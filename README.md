# lakesense

**Intelligent ML data observability for the lakehouse.**

lakesense profiles your datasets using mergeable probabilistic sketches (MinHash, HyperLogLog, KLL)
and deterministic column profiles, builds dynamic baselines per job, and uses an LLM agent to
investigate and explain drift signals — with pluggable alerting and storage.

[![CI](https://github.com/ramannanda9/lakesense/actions/workflows/ci.yml/badge.svg)](https://github.com/ramannanda9/lakesense/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## Why lakesense?

Existing tools stop at drift *detection* — they tell you a number changed.
lakesense adds an **interpretation layer**: a two-tier pipeline that runs a fast LLM
assessment on every job, and fires an investigative agent only when something is actually wrong.

Key properties:

- **Probabilistic sketches** — MinHash, HLL, KLL for O(1) memory profiling with mergeable baselines
- **Full column profiling** — null rates, int ranges, categorical distributions, boolean ratios, string lengths, schema drift
- **Distributed compute** — Spark provider for distributed sketch computation via `mapInPandas`
- **Zero-infra quickstart** — Parquet backend, no catalog or cluster required
- **Plugin architecture** — bring your own storage, alerting, and agent tools
- **Two-tier cost control** — heuristics always run free; LLM only invoked on warn/alert; expensive agent only on alert
- **No-network mode** — works 100% locally using heuristic rules when no API key is set

---

## Quickstart

```bash
pip install lakesense
```

```python
import asyncio
from datetime import datetime, timezone
import pandas as pd
from lakesense.core import SketchFramework
from lakesense.storage.parquet import ParquetBackend
from lakesense.sketches.providers.pandas import PandasProvider
from lakesense.sketches.merge import BaselineConfig

# run_ts = the data interval this run covers (e.g. Airflow's data_interval_end)
# The baseline window queries historical sketches using run_ts as the upper bound,
# so each run needs a distinct timestamp to see prior runs as history.
run_ts = datetime(2026, 3, 30, tzinfo=timezone.utc)

# 1. Compute sketches from your data
df = pd.read_parquet("features/latest.parquet")
provider = PandasProvider()
records = provider.sketch(
    data=df,
    dataset_id="user_features",
    job_id="train_job_42",
    text_columns=["description"],
    id_columns=["user_id"],
    numeric_columns=["session_count", "revenue"],
    run_ts=run_ts,
)

# 2. Persist sketches for baseline building
storage = ParquetBackend("./sketches")
asyncio.run(storage.write_sketches(records))

# 3. Run the interpretation pipeline
framework = SketchFramework(storage=storage)

result = asyncio.run(framework.run({
    "dataset_id": "user_features",
    "job_id":     "train_job_42",
    "sketch_records": records,
    "data_interval_end": run_ts,
    "baseline_config": BaselineConfig(dataset_id="user_features", window_days=7),
}))

print(result.severity)   # ok | warn | alert
print(result.summary)    # "Jaccard similarity dropped 34% vs 7-day baseline..."
```

Heuristic rules run on every job (free, instant). Set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`
to add LLM-powered interpretation — the LLM is only invoked when heuristics flag warn/alert,
so healthy runs never incur an API call.

Run the full quickstart example (no API key needed):

```bash
pip install lakesense[duckdb]
python examples/quickstart.py
```

---

## Architecture

```
Every run   →  Tier 1: sketch compute + baseline merge + LLM interpret  →  severity + summary
warn/alert  →  Tier 2: plugins (investigative agent, Slack, PagerDuty)  →  root cause + action
```

### Tier 1 — base interpretation (always runs)

1. Compute sketches (MinHash, HLL, KLL) and column profiles from the dataset
2. Merge historical sketches into a baseline (rolling window, snapshot, or EWMA)
3. Compute drift signals (Jaccard delta, cardinality ratio, quantile shifts, null rate, schema drift)
4. Run heuristic rules — if severity is `ok`, return immediately (no LLM cost)
5. On `warn`/`alert` — call the LLM for nuanced interpretation + summary (LLM can upgrade severity but not downgrade below the heuristic floor)

### Tier 2 — plugin chain (on warn/alert only)

Plugins run in registration order, each receiving the result enriched by prior plugins:

```python
framework = (
    SketchFramework(storage=ParquetBackend("./sketches"))
    .register(InvestigativeAgentPlugin())   # root cause analysis
    .register(SlackAlertPlugin(webhook=WEBHOOK))  # needs owners from agent
)
```

---

## Sketch providers

| Provider | Use case | Install |
|---|---|---|
| `PandasProvider` | Single-machine, local dev | `pip install lakesense` |
| `SparkProvider` | Distributed compute via `mapInPandas` | `pip install lakesense[spark]` |
| `StreamingProvider` | Incremental / micro-batch | `pip install lakesense` |

## Sketch types

| Sketch | Use case | Merge cost |
|---|---|---|
| MinHash (Theta) | Text/set similarity, near-duplicate detection | O(num_perm) |
| HyperLogLog | Cardinality estimation (unique users, items) | O(registers) |
| KLL | Quantile estimation, distribution shape shifts | approx via sorted sample |
| Profile | Deterministic column metrics (nulls, ranges, categoricals) | scalar comparison |

## Storage backends

| Backend | Use case | Install |
|---|---|---|
| `ParquetBackend` | Zero-infra, local dev | `pip install lakesense` |
| `DuckDBBackend` | Local + SQL queries | `pip install lakesense[duckdb]` |
| `IcebergBackend` | Production lakehouse (v0.2) | `pip install lakesense[iceberg]` |

---

## Baseline strategies

```python
from lakesense.sketches.merge import BaselineConfig, BaselineStrategy

# Rolling window — merge all runs in the last N days
BaselineConfig(dataset_id="ds", strategy=BaselineStrategy.ROLLING_WINDOW, window_days=7)

# Snapshot — pin a known-good run as reference
BaselineConfig(dataset_id="ds", strategy=BaselineStrategy.SNAPSHOT,
               snapshot_id="2024-01-15T00:00:00+00:00")

# EWMA — exponentially weight recent runs more
BaselineConfig(dataset_id="ds", strategy=BaselineStrategy.EWMA, decay_factor=0.85)
```

---

## Writing a custom plugin

```python
from lakesense.core import SketchPlugin, InterpretationResult, Severity

class PagerDutyPlugin(SketchPlugin):
    def __init__(self, routing_key: str):
        self._key = routing_key

    def should_run(self, result: InterpretationResult) -> bool:
        return result.severity == Severity.ALERT and result.is_agent_enriched()

    async def run(self, result: InterpretationResult) -> InterpretationResult:
        await self._page(result)
        result.metadata["pagerduty"] = "paged"
        return result
```

---

## Roadmap

- **v0.1** — core sketches, column profiles, Parquet + DuckDB storage, Tier 1 LLM interpret, Spark provider
- **v0.2** — agent plugin, DataHub lineage, Slack plugin, IcebergBackend
- **v0.3** — DeltaBackend, Airflow operator, OpenLineage support
- **v0.4** — JIRA plugin, column-level lineage

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). PRs welcome — especially new storage backends and plugins.

```bash
pip install -e ".[dev]"
pytest tests/unit/
ruff check .
mypy lakesense/
```

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
