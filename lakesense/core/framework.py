"""
SketchFramework — the main orchestrator.

Tier 1: base interpretation runs on every job.
Tier 2: registered plugins run in order, each deciding via should_run().

Usage:
    framework = (
        SketchFramework(storage=ParquetBackend("./sketches"))
        .register(InvestigativeAgentPlugin())
        .register(SlackAlertPlugin(webhook=SLACK_WEBHOOK))
        .register(IcebergStorePlugin())
    )

    result = await framework.run(job)

    # or batch:
    results = await framework.run_batch(jobs)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from lakesense.core.plugin import SketchPlugin, StorageBackend
from lakesense.core.result import InterpretationResult

logger = logging.getLogger(__name__)


class SketchFramework:
    """
    Two-tier observability framework.

    Tier 1 (base_interpret) runs on every job — fast, cheap, always-on.
    Tier 2 plugins run conditionally based on each plugin's should_run().

    Plugin execution is sequential and ordered by registration.
    Each plugin receives the result as enriched by all prior plugins —
    this means plugin ordering matters (agent before slack, always last store).
    """

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage
        self._plugins: list[SketchPlugin] = []

    def register(self, plugin: SketchPlugin) -> SketchFramework:
        """Register a Tier 2 plugin. Returns self for fluent chaining."""
        self._plugins.append(plugin)
        logger.debug("registered plugin: %s", plugin.name)
        return self

    @property
    def plugins(self) -> list[SketchPlugin]:
        return list(self._plugins)

    async def run(self, job: dict[str, Any]) -> InterpretationResult:
        """
        Run the full pipeline for a single job.

        Args:
            job: dict with keys: dataset_id, job_id, and any sketch inputs.

        Returns:
            InterpretationResult enriched by all applicable plugins.
        """
        # Tier 1 — always runs
        result = await self._base_interpret(job)
        logger.info(
            "tier1 complete: dataset=%s severity=%s signals=%s",
            result.dataset_id,
            result.severity.value,
            result.dataset_drift_summary.worst_signal(),
        )

        # Ensure there is always a StoragePlugin in the chain.
        # If the user hasn't registered one, add a bound one automatically.
        plugins = self._plugins
        if not any(p.__class__.__name__ == "StoragePlugin" for p in plugins):
            from lakesense.plugins.store import StoragePlugin

            plugins = [*plugins, StoragePlugin(storage=self._storage)]

        # Bind any unbound plugins to our storage backend
        for plugin in plugins:
            if hasattr(plugin, "bind") and getattr(plugin, "_storage", None) is None:
                plugin.bind(self._storage)

        # Tier 2 — plugin chain
        for plugin in plugins:
            if plugin.should_run(result):
                logger.info("running plugin: %s", plugin.name)
                try:
                    result = await plugin.run(result)
                except Exception:
                    logger.exception("plugin %s failed — continuing", plugin.name)

        return result

    async def run_batch(
        self,
        jobs: list[dict[str, Any]],
        concurrency: int = 10,
    ) -> list[InterpretationResult]:
        """
        Run the pipeline for a batch of jobs concurrently.

        Args:
            jobs: list of job dicts.
            concurrency: max concurrent pipeline runs (default 10).
                         Tune down if hitting LLM rate limits.

        Returns:
            List of results in the same order as input jobs.
        """
        semaphore = asyncio.Semaphore(concurrency)

        async def _bounded(job: dict[str, Any]) -> InterpretationResult:
            async with semaphore:
                return await self.run(job)

        return list(await asyncio.gather(*[_bounded(j) for j in jobs]))

    async def _base_interpret(self, job: dict[str, Any]) -> InterpretationResult:
        """
        Tier 1: compute sketches, merge baseline, run LLM interpretation.
        Imported lazily to keep the framework decoupled from interpreter internals.
        """
        from lakesense.interpreter.base import base_interpret

        return await base_interpret(job, storage=self._storage)
