from __future__ import annotations

import inspect
import logging
import os
from collections.abc import Callable
from typing import Any

from . import LLMProvider

logger = logging.getLogger(__name__)


def _function_to_anthropic_tool(func: Callable) -> dict[str, Any]:
    """Convert a Python function to an Anthropic tool schema using its signature and docstring."""
    sig = inspect.signature(func)
    doc = inspect.getdoc(func) or "No description provided."

    properties = {}
    required = []

    for name, param in sig.parameters.items():
        if name == "self":
            continue

        param_type = "string"  # simplified fallback
        if param.annotation is int:
            param_type = "integer"
        elif param.annotation is float:
            param_type = "number"
        elif param.annotation is bool:
            param_type = "boolean"

        properties[name] = {
            "type": param_type,
            "description": f"Parameter: {name}",
        }

        if param.default == inspect.Parameter.empty:
            required.append(name)

    return {
        "name": func.__name__,
        "description": doc.split("\n")[0] if "\n" in doc else doc,
        "input_schema": {
            "type": "object",
            "properties": properties,
            "required": required,
        },
    }


class AnthropicProvider(LLMProvider):
    """
    Anthropic implementation of the LLM interpretation engine using AsyncAnthropic.
    Requires: pip install lakesense[anthropic]
    """

    def __init__(self, api_key: str | None = None, model: str = "claude-sonnet-4-6", max_tokens: int = 4096):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is required to initialize AnthropicProvider")

        try:
            import anthropic
        except ImportError as e:
            raise ImportError("anthropic is required. Run: pip install lakesense[anthropic]") from e

        self.model = model
        self.max_tokens = max_tokens
        self.client = anthropic.AsyncAnthropic(api_key=self.api_key)

    async def analyze(self, prompt: str, system_prompt: str) -> str:
        response = await self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system_prompt,
            messages=[{"role": "user", "content": prompt}],
        )
        return str(response.content[0].text)

    async def act_and_reason(
        self,
        user_message: str,
        system_prompt: str,
        tools: list[Callable],
        max_iterations: int = 5,
    ) -> tuple[str, list[dict]]:
        tool_schemas = [_function_to_anthropic_tool(f) for f in tools]
        messages: list[dict[str, Any]] = [{"role": "user", "content": user_message}]

        for _ in range(max_iterations):
            response = await self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=system_prompt,
                tools=tool_schemas,
                messages=messages,
            )

            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                break

            if response.stop_reason == "max_tokens":
                logger.warning("Anthropic hit max_tokens during agent loop — consider increasing max_tokens.")
                break

            if response.stop_reason == "tool_use":
                # Collect ALL tool results into a single user message
                # (Anthropic requires all tool_results for a turn in one message)
                tool_results: list[dict[str, Any]] = []

                for block in response.content:
                    if block.type == "tool_use":
                        tool_name = block.name
                        tool_args = block.input
                        logger.info("Anthropic Provider calling tool: %s", tool_name)

                        func = next((f for f in tools if f.__name__ == tool_name), None)
                        if func:
                            try:
                                if inspect.iscoroutinefunction(func):
                                    tool_result = await func(**tool_args)
                                else:
                                    tool_result = func(**tool_args)
                                tool_str_result = (
                                    str(tool_result) if tool_result else "Tool succeeded with empty output."
                                )
                            except Exception as e:
                                tool_str_result = f"Error executing tool: {e}"
                        else:
                            tool_str_result = f"Error: Tool {tool_name} not found."

                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": tool_str_result,
                            }
                        )

                if tool_results:
                    messages.append({"role": "user", "content": tool_results})
            else:
                # Unknown stop_reason, bail out
                logger.warning("Unexpected stop_reason: %s", response.stop_reason)
                break

        # Extract final text from the last assistant message
        final_text = ""
        for msg in reversed(messages):
            if msg.get("role") == "assistant":
                content = msg.get("content")
                if isinstance(content, list):
                    for b in content:
                        if getattr(b, "type", "") == "text":
                            final_text = getattr(b, "text", "")
                            break
                elif isinstance(content, str):
                    final_text = content

                if final_text:
                    break

        agent_trace = [{"role": m["role"], "content_type": type(m["content"]).__name__} for m in messages]

        return final_text, agent_trace
