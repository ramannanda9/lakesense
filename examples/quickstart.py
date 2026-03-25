"""
lakesense quickstart — no API key required.

Demonstrates the full pipeline using the heuristic fallback
(no LLM call) so you can see the framework working immediately.

Run:
    pip install lakesense[duckdb]
    python examples/quickstart.py

For LLM interpretation, set ANTHROPIC_API_KEY in your environment.
"""

import asyncio

import numpy as np
import pandas as pd

from lakesense.core import SketchFramework
from lakesense.sketches.merge import BaselineConfig, BaselineStrategy
from lakesense.storage.duckdb import DuckDBBackend


def make_dataset(n: int = 1000, drift: bool = False) -> pd.DataFrame:
    """Synthetic user feature dataset. drift=True simulates a schema/distribution change."""
    rng = np.random.default_rng(42 if not drift else 99)
    return pd.DataFrame(
        {
            "user_id": [f"user_{i}" for i in range(n)],
            "description": [
                "active user high engagement"
                if not drift
                else "churned low activity dropout"  # token distribution shift
                for _ in range(n)
            ],
            "session_count": rng.integers(1, 100 if not drift else 5, size=n).tolist(),
            "revenue": rng.exponential(50 if not drift else 5, size=n).tolist(),
        }
    )


async def main():
    # Always start fresh so the quickstart is reproducible
    import shutil

    data_dir = "./lakesense_quickstart_data"
    shutil.rmtree(data_dir, ignore_errors=True)

    storage = DuckDBBackend(data_dir)

    framework = SketchFramework(storage=storage)
    # (no Tier 2 plugins for quickstart — add SlackAlertPlugin here in production)

    print("=== lakesense quickstart ===\n")

    # --- Simulate 5 normal runs to build a baseline ---
    print("Running 5 baseline jobs...")
    for i in range(5):
        df = make_dataset(drift=False)
        result = await framework.run(
            {
                "dataset_id": "user_features",
                "job_id": f"train_job_{i}",
                "df": df,
                "columns": {
                    "text_columns": ["description"],
                    "id_columns": ["user_id"],
                    "numeric_columns": ["session_count", "revenue"],
                },
                "baseline_config": BaselineConfig(
                    dataset_id="user_features",
                    strategy=BaselineStrategy.ROLLING_WINDOW,
                    window_days=7,
                ),
            }
        )
        print(f"  run {i}: severity={result.severity.value:5s}  {result.summary}")

    # --- Simulate a drifted run ---
    print("\nRunning 1 drifted job (distribution shift)...")
    df_drift = make_dataset(drift=True)
    result = await framework.run(
        {
            "dataset_id": "user_features",
            "job_id": "train_job_drifted",
            "df": df_drift,
            "columns": {
                "text_columns": ["description"],
                "id_columns": ["user_id"],
                "numeric_columns": ["session_count", "revenue"],
            },
            "baseline_config": BaselineConfig(
                dataset_id="user_features",
                strategy=BaselineStrategy.ROLLING_WINDOW,
                window_days=7,
            ),
        }
    )
    print(f"  drifted: severity={result.severity.value:5s}  {result.summary}")
    print(f"  signals: {result.drift_signals.worst_signal()}")

    # --- Query the results with DuckDB ---
    print("\n=== All interpretation results ===")
    df_results = storage.query(
        "SELECT job_id, severity, summary, jaccard_delta FROM interpretations ORDER BY run_ts"
    ).df()
    print(df_results.to_string(index=False))

    print("\n=== Dataset summary ===")
    print(storage.summary().to_string(index=False))

    print("\ndone. data written to ./lakesense_quickstart_data/")


if __name__ == "__main__":
    asyncio.run(main())
