"""
Storage plugins for lakesense.

StoragePlugin — always-on terminal plugin that persists InterpretationResults.
               Register last in every pipeline — it's the final write.

Usage:
    framework = (
        SketchFramework(storage=DuckDBBackend("./sketches"))
        .register(InvestigativeAgentPlugin())
        .register(SlackAlertPlugin(webhook=WEBHOOK))
        .register(StoragePlugin())   # always last
    )

If you don't register StoragePlugin, interpretations are computed but not
persisted. The framework won't auto-register it so the contract is explicit.
"""

from __future__ import annotations

import logging

from lakesense.core.plugin import SketchPlugin, StorageBackend
from lakesense.core.result import InterpretationResult

logger = logging.getLogger(__name__)


class StoragePlugin(SketchPlugin):
    """
    Terminal plugin — persists the final InterpretationResult to the
    configured StorageBackend after all other plugins have run.

    Always returns True from should_run() — every result is persisted.

    The storage backend is injected from the SketchFramework at runtime,
    so you don't need to pass it explicitly:

        framework = (
            SketchFramework(storage=DuckDBBackend("./sketches"))
            .register(StoragePlugin())
        )
    """

    def __init__(self, storage: StorageBackend | None = None) -> None:
        # If storage is None, it will be injected by SketchFramework.run()
        self._storage = storage

    def should_run(self, result: InterpretationResult) -> bool:
        return True

    async def run(self, result: InterpretationResult) -> InterpretationResult:
        if self._storage is None:
            logger.warning("StoragePlugin has no backend — result not persisted")
            return result
        await self._storage.write_interpretation(result)
        logger.debug(
            "persisted interpretation: dataset=%s severity=%s",
            result.dataset_id,
            result.severity.value,
        )
        return result

    def bind(self, storage: StorageBackend) -> StoragePlugin:
        """Bind a storage backend. Called by SketchFramework before execution."""
        self._storage = storage
        return self
