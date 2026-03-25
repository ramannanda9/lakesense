from __future__ import annotations

import os

from . import LLMProvider


class AnthropicProvider(LLMProvider):
    """
    Anthropic implementation of the LLM interpretation engine using AsyncAnthropic.
    Requires: pip install lakesense[anthropic]
    """

    def __init__(self, api_key: str | None = None, model: str = "claude-sonnet-4-20250514"):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is required to initialize AnthropicProvider")
        
        try:
            import anthropic
        except ImportError as e:
            raise ImportError("anthropic is required. Run: pip install lakesense[anthropic]") from e

        self.model = model
        self.client = anthropic.AsyncAnthropic(api_key=self.api_key)

    async def analyze(self, prompt: str, system_prompt: str) -> str:
        response = await self.client.messages.create(
            model=self.model,
            max_tokens=400,
            system=system_prompt,
            messages=[{"role": "user", "content": prompt}],
        )
        return str(response.content[0].text)
