"""
InvestigativeAgentPlugin — ReAct LLM agent for root cause analysis.

Only runs when severity == ALERT. Uses the defined tools to query
historical sketches, downstream lineage, etc., and populates
`result.root_cause` and `result.affected_urns`.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable

from lakesense.core.plugin import SketchPlugin, StorageBackend
from lakesense.core.result import InterpretationResult, Severity
from lakesense.interpreter.providers import LLMProvider

logger = logging.getLogger(__name__)


class InvestigativeAgentPlugin(SketchPlugin):
    """
    Tier 2 Agent that triggers on ALERT.
    Uses an LLMProvider to investigate data anomalies and propose root causes.
    """

    def __init__(self, tools: list[Callable] | None = None, llm_provider: LLMProvider | None = None) -> None:
        self._custom_tools = tools or []
        self._llm_provider = llm_provider
        self._storage: StorageBackend | None = None

    def bind(self, storage: StorageBackend) -> None:
        """Bind the framework's storage backend so our native tools can access it."""
        self._storage = storage

    def should_run(self, result: InterpretationResult) -> bool:
        """Only run the expensive ReAct loop on ALERT."""
        return result.severity == Severity.ALERT

    async def run(self, result: InterpretationResult) -> InterpretationResult:
        # Resolve provider if not explicitly passed
        provider = self._llm_provider
        if not provider:
            if "ANTHROPIC_API_KEY" in os.environ:
                from lakesense.interpreter.providers.anthropic_provider import AnthropicProvider

                provider = AnthropicProvider()
            elif "OPENAI_API_KEY" in os.environ:
                from lakesense.interpreter.providers.openai_provider import OpenAIProvider

                provider = OpenAIProvider()

        if not provider:
            logger.warning(
                "InvestigativeAgentPlugin skipped: no llm_provider and no recognized API keys found in environment."
            )
            return result

        logger.info(
            "Agent beginning investigation for dataset %s using %s", result.dataset_id, provider.__class__.__name__
        )

        # Assemble native tools
        native_tools = [
            self.get_recent_interpretations,
        ]

        if hasattr(self._storage, "query"):
            native_tools.append(self.query_sketches_sql)

        all_funcs = native_tools + self._custom_tools

        # Build the initial prompt
        system_prompt = (
            "You are a Senior Data Engineer investigating a critical data observability alert.\n"
            "Your job is to use your available tools to trace lineage, examine historical sketches, "
            "and find the true root cause of the data anomaly.\n"
            "Once you find the root cause, output a concise explanation and identify any downstream affected datasets."
        )

        user_message = (
            f"ALERT on dataset: {result.dataset_id}\n"
            f"Job ID: {result.job_id}\n"
            f"Time: {result.run_ts.isoformat()}\n"
            f"Tier 1 Summary: {result.summary}\n"
            f"Worst Signal: {result.dataset_drift_summary.worst_signal()}\n\n"
            f"Please investigate using your tools."
        )

        try:
            final_answer, trace_messages = await provider.act_and_reason(
                user_message=user_message,
                system_prompt=system_prompt,
                tools=all_funcs,
                max_iterations=5,
            )

            result.root_cause = final_answer
            result.agent_trace = trace_messages

        except Exception as e:
            logger.error("InvestigativeAgentPlugin failed during ReAct loop: %s", e)
            result.root_cause = f"Agent investigation failed: {e}"

        return result

    # --- Native Tools

    async def get_recent_interpretations(self, dataset_id: str) -> str:
        """Fetch the most recent interpretation anomalies for a given dataset."""
        if not self._storage:
            return "No storage backend available."
        history = await self._storage.read_interpretation_history(dataset_id=dataset_id, limit=5)
        if not history:
            return f"No history found for {dataset_id}."
        return json.dumps(
            [
                {"run_ts": r.run_ts.isoformat(), "severity": r.severity.value, "summary": r.summary}
                for r in history
                if r.severity != Severity.OK
            ]
        )

    def query_sketches_sql(self, query: str) -> str:
        """Run an arbitrary SQL query against historical sketch metric tables.

        Example: 'SELECT dataset_id, run_ts, null_delta FROM interpretations'.
        """
        if not hasattr(self._storage, "query"):
            return "Error: The configured storage backend does not support SQL queries."
        try:
            # DuckDB implementation returns a relation that has .df()
            df = self._storage.query(query).df()
            return df.head(10).to_string()  # return string representation of top 10 rows
        except Exception as e:
            return f"SQL Error: {e}"
