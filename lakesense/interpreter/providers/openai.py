from __future__ import annotations
import os
from . import LLMProvider


class OpenAIProvider(LLMProvider):
    """
    OpenAI implementation of the LLM interpretation engine using AsyncOpenAI.
    Requires: pip install lakesense[openai]
    """

    def __init__(self, api_key: str | None = None, model: str = "gpt-4o"):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is required to initialize OpenAIProvider")
        
        try:
            import openai
        except ImportError as e:
            raise ImportError("openai is required. Run: pip install lakesense[openai]") from e

        self.model = model
        self.client = openai.AsyncOpenAI(api_key=self.api_key)

    async def analyze(self, prompt: str, system_prompt: str) -> str:
        response = await self.client.chat.completions.create(
            model=self.model,
            max_tokens=400,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
        )
        # Note: Depending on typing, choices[0].message.content can theoretically be None
        content = response.choices[0].message.content
        return str(content) if content else "{}"
