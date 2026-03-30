from __future__ import annotations

import inspect
import json
import logging
import os
from collections.abc import Callable
from typing import Any

from . import LLMProvider

logger = logging.getLogger(__name__)


def _function_to_openai_tool(func: Callable) -> dict[str, Any]:
    """Convert a Python function to an OpenAI function calling schema."""
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
        "type": "function",
        "function": {
            "name": func.__name__,
            "description": doc.split("\n")[0] if "\n" in doc else doc,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
            "strict": False,
        },
    }


class OpenAIProvider(LLMProvider):
    """
    OpenAI implementation of the LLM interpretation engine using AsyncOpenAI.
    Requires: pip install lakesense[openai]
    """

    def __init__(self, api_key: str | None = None, model: str = "gpt-4o", max_tokens: int = 4096):
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is required to initialize OpenAIProvider")

        try:
            import openai
        except ImportError as e:
            raise ImportError("openai is required. Run: pip install lakesense[openai]") from e

        self.model = model
        self.max_tokens = max_tokens
        self.client = openai.AsyncOpenAI(api_key=self.api_key)

    async def analyze(self, prompt: str, system_prompt: str) -> str:
        response = await self.client.chat.completions.create(
            model=self.model,
            max_tokens=self.max_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content
        return str(content) if content else "{}"

    async def act_and_reason(
        self,
        user_message: str,
        system_prompt: str,
        tools: list[Callable],
        max_iterations: int = 5,
    ) -> tuple[str, list[dict]]:
        tool_schemas = [_function_to_openai_tool(f) for f in tools]

        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ]

        for _ in range(max_iterations):
            response = await self.client.chat.completions.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=messages,
                tools=tool_schemas,
            )

            choice = response.choices[0]
            message = choice.message

            # Dump the Pydantic model to a dict, excluding None values so OpenAI accepts it back
            msg_dict = message.model_dump(exclude_none=True)
            messages.append(msg_dict)

            if choice.finish_reason == "length":
                logger.warning("OpenAI hit max_tokens during agent loop — consider increasing max_tokens.")
                break

            if message.tool_calls:
                # Handle all parallel tool calls before the next iteration
                for tool_call in message.tool_calls:
                    tool_name = tool_call.function.name
                    logger.info("OpenAI Provider calling tool: %s", tool_name)

                    try:
                        tool_args = json.loads(tool_call.function.arguments)
                    except json.JSONDecodeError:
                        tool_args = {}

                    func = next((f for f in tools if f.__name__ == tool_name), None)
                    if func:
                        try:
                            if inspect.iscoroutinefunction(func):
                                tool_result = await func(**tool_args)
                            else:
                                tool_result = func(**tool_args)
                            tool_str_result = str(tool_result) if tool_result else "Tool succeeded with empty output."
                        except Exception as e:
                            tool_str_result = f"Error executing tool: {e}"
                    else:
                        tool_str_result = f"Error: Tool {tool_name} not found."

                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": tool_str_result,
                        }
                    )
            else:
                # finish_reason == "stop" — model is done
                break

        # Extract final text
        final_text = ""
        for msg in reversed(messages):
            if msg.get("role") == "assistant" and not msg.get("tool_calls"):
                final_text = msg.get("content") or ""
                break

        agent_trace = [
            {"role": m.get("role", "unknown"), "content_preview": str(m.get("content", ""))[:100]} for m in messages
        ]

        return str(final_text), agent_trace
