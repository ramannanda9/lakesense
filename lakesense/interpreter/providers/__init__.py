from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable


class LLMProvider(ABC):
    """
    Abstract interface for LLM calls during interpretation.
    Allows passing customized model providers (Anthropic, OpenAI, local)
    or overriding parameters (temperature, max_tokens, retries).
    """

    @abstractmethod
    async def analyze(self, prompt: str, system_prompt: str) -> str:
        """
        Execute the prompt against the LLM and return the raw string text response.
        The system prompt contains the instructions for output format (JSON).
        """
        ...

    @abstractmethod
    async def act_and_reason(
        self,
        user_message: str,
        system_prompt: str,
        tools: list[Callable],
        max_iterations: int = 5,
    ) -> tuple[str, list[dict]]:
        """
        Execute a ReAct (Reasoning and Acting) loop using the provided tools.
        Returns a tuple of (final_root_cause_explanation, agent_trace_messages).
        """
        ...
