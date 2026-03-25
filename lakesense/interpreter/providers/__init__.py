from __future__ import annotations

from abc import ABC, abstractmethod


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
