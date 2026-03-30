"""Unit tests for the Tier 2 InvestigativeAgentPlugin and LLM Providers."""

from unittest.mock import patch

import pytest

from lakesense.core.result import InterpretationResult, Severity
from lakesense.interpreter.providers.anthropic_provider import _function_to_anthropic_tool
from lakesense.interpreter.providers.openai_provider import _function_to_openai_tool
from lakesense.plugins.agent import InvestigativeAgentPlugin


def test_function_to_anthropic_tool():
    """Test reflection of python functions into Anthropic JSON schema."""

    def sample_tool(region: str, max_results: int) -> str:
        """Search regional data."""
        pass

    schema = _function_to_anthropic_tool(sample_tool)

    assert schema["name"] == "sample_tool"
    assert "Search regional data" in schema["description"]
    assert "region" in schema["input_schema"]["properties"]
    assert schema["input_schema"]["properties"]["max_results"]["type"] == "integer"
    assert "region" in schema["input_schema"]["required"]
    assert "max_results" in schema["input_schema"]["required"]


def test_function_to_openai_tool():
    """Test reflection of python functions into OpenAI JSON schema."""

    def sample_tool(region: str, max_results: int) -> str:
        """Search regional data."""
        pass

    schema = _function_to_openai_tool(sample_tool)

    assert schema["type"] == "function"
    assert schema["function"]["name"] == "sample_tool"
    assert "Search regional data" in schema["function"]["description"]
    assert "region" in schema["function"]["parameters"]["properties"]
    assert schema["function"]["parameters"]["properties"]["max_results"]["type"] == "integer"
    assert "region" in schema["function"]["parameters"]["required"]
    assert "max_results" in schema["function"]["parameters"]["required"]


def test_agent_should_run():
    """Agent must only run on ALERT."""
    agent = InvestigativeAgentPlugin()

    # Empty result
    res_ok = InterpretationResult(dataset_id="x", job_id="1", severity=Severity.OK)
    res_warn = InterpretationResult(dataset_id="x", job_id="1", severity=Severity.WARN)
    res_alert = InterpretationResult(dataset_id="x", job_id="1", severity=Severity.ALERT)

    assert not agent.should_run(res_ok)
    assert not agent.should_run(res_warn)
    assert agent.should_run(res_alert)


@pytest.mark.asyncio
async def test_agent_skips_without_auth():
    """Agent returns unmodified result if no API key is set."""
    # Ensure no API key via patch
    with patch.dict("os.environ", clear=True):
        agent = InvestigativeAgentPlugin()
        res = InterpretationResult(dataset_id="x", job_id="1", severity=Severity.ALERT)
        out = await agent.run(res)

        # It shouldn't crash, and root_cause should remain None
        assert out.root_cause is None
