"""
lakesense — sketch-based ML data observability with LLM interpretation.

Quick start:
    from lakesense import SketchFramework
    from lakesense.storage.duckdb import DuckDBBackend

    framework = SketchFramework(storage=DuckDBBackend("./sketches"))
    result = await framework.run({
        "dataset_id": "my_features",
        "job_id":     "train_42",
        "df":         df,
        "columns": {"id_columns": ["user_id"], "numeric_columns": ["score"]},
    })
    print(result.severity, result.summary)
"""

from lakesense.core.framework import SketchFramework
from lakesense.core.plugin import SketchPlugin, StorageBackend
from lakesense.core.result import DatasetDriftSummary, DriftSignals, InterpretationResult, Severity
from lakesense.sketches.merge import BaselineConfig, BaselineStrategy

__version__ = "0.2.2"

__all__ = [
    "SketchFramework",
    "SketchPlugin",
    "StorageBackend",
    "InterpretationResult",
    "DatasetDriftSummary",
    "DriftSignals",
    "Severity",
    "BaselineConfig",
    "BaselineStrategy",
]
