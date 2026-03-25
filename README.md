# lakesense

**Intelligent ML data observability for the lakehouse.**

lakesense profiles your datasets using mergeable probabilistic sketches (MinHash, HyperLogLog, KLL),
builds dynamic baselines per job, and uses an LLM agent to investigate and explain drift signals —
with DataHub lineage traversal, pluggable alerting, and Apache Iceberg storage.

[![PyPI version](https://badge.fury.io/py/lakesense.svg)](https://badge.fury.io/py/lakesense)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

---

## Why lakesense?

Existing tools stop at drift *detection* — they tell you a number changed.
lakesense adds an **interpretation layer**: a two-tier pipeline that runs a fast LLM
assessment on every job, and fires an investigative agent (with DataHub lineage) only
when something is actually wrong.

Key properties:

- **Full column profiling** — null rates, int ranges, categorical distributions, boolean ratios, string lengths, schema drift
- **Sketch mergeability** — rolling window baselines are O(num_perm), not O(rows)
- **Zero-infra quickstart** — Parquet + DuckDB, no catalog or cluster required
- **Plugin architecture** — bring your own storage, alerting, and agent tools
- **Two-tier cost control** — fast LLM call always; expensive agent only on warn/alert

---

## Quickstart

```bash
pip install lakesense[duckdb]
```

```python
import asyncio
import pandas as pd
from lakesense.core import SketchFramework
from lakesense.storage.duckdb import DuckDBBackend
from lakesense.sketches.merge import BaselineConfig

framework = SketchFramework(storage=DuckDBBackend("./sketches"))

result = await framework.run({
    "dataset_id": "user_features",
    "job_id":     "train_job_42",
    "df":         pd.read_parquet("s3://my-bucket/features/latest.parquet"),
    "columns": {
        "text_columns":    ["description"],
        "id_columns":      ["user_id"],
        "numeric_columns": ["session_count", "revenue"],
    },
    "baseline_config": BaselineConfig(dataset_id="user_features", window_days=7),
})

print(result.severity)   # ok | warn | alert
print(result.summary)    # "Jaccard similarity dropped 34% vs 7-day baseline..."
```

Set `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` for instant LLM diagnosis.

**No-Network Operation**: `lakesense` can operate 100% locally without an LLM. If neither API key is provided, the Interpreter relies purely on the structural DataSketches math heuristic (evaluating Kolmogorov-Smirnov Distribution Shifts, Row Count Drops, Null Explosions, and Jaccard Schema decay).

Run the full quickstart example (no API key needed):

```bash
python examples/quickstart.py
```

---

## Architecture

```
Every run   →  Tier 1: sketch compute + baseline merge + LLM interpret  →  severity + summary
warn/alert  →  Tier 2: plugins (investigative agent, Slack, PagerDuty)  →  root cause + action
```

### Tier 1 — base interpretation (always runs)

1. Compute MinHash / HLL / KLL sketches from the dataset
2. Merge historical sketches into a baseline (rolling window, snapshot, or EWMA)
3. Compute drift signals (Jaccard delta, cardinality ratio, quantile shifts)
4. Call the LLM for a fast structured severity + summary

### Tier 2 — plugin chain (on warn/alert only)

Plugins run in registration order, each receiving the result enriched by prior plugins:

```python
framework = (
    SketchFramework(storage=DuckDBBackend("./sketches"))
    .register(InvestigativeAgentPlugin())   # DataHub lineage + root cause
    .register(SlackAlertPlugin(webhook=WEBHOOK))  # needs owners from agent
    .register(IcebergStorePlugin())         # always last
)
```

### Storage backends

| Backend | Use case | Extra deps |
|---|---|---|
| `ParquetBackend` | zero-infra, local dev | none |
| `DuckDBBackend` | local + SQL queries | `lakesense[duckdb]` |
| `IcebergBackend` | production lakehouse (v0.2) | `lakesense[iceberg]` |
| `DeltaBackend` | Databricks (v0.3) | — |

---

## Sketch types

| Sketch | Use case | Merge cost |
|---|---|---|
| MinHash | Text/set similarity, near-duplicate detection | O(num_perm) |
| HyperLogLog | Cardinality estimation (unique users, items) | O(registers) |
| KLL | Kolmogorov-Smirnov (KS) exact probability distribution shape shifts and quantile latency. | approx via sorted sample |

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

See [docs/plugins.md](docs/plugins.md) for the full plugin guide.

---

## Roadmap

- **v0.1** — core, sketches, Parquet + DuckDB storage, Tier 1 LLM interpret ✅
- **v0.2** — agent plugin, DataHub lineage, Slack plugin, IcebergBackend
- **v0.3** — DeltaBackend, Airflow operator, Spark native integration
- **v0.4** — OpenLineage support, JIRA plugin, column-level lineage

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). PRs welcome — especially new storage backends
and plugins. Please add tests and follow the existing module structure.

```bash
pip install -e ".[dev]"
pytest
ruff check .
mypy lakesense/
```

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
