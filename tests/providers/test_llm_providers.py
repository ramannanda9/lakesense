import pytest


def test_anthropic_instantiation(monkeypatch):
    pytest.importorskip("anthropic")
    from lakesense.interpreter.providers.anthropic_provider import AnthropicProvider

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-anthropic")
    provider = AnthropicProvider()
    assert provider.api_key == "test-key-anthropic"


def test_openai_instantiation(monkeypatch):
    pytest.importorskip("openai")
    from lakesense.interpreter.providers.openai_provider import OpenAIProvider

    monkeypatch.setenv("OPENAI_API_KEY", "test-key-openai")
    provider = OpenAIProvider()
    assert provider.api_key == "test-key-openai"


def test_missing_anthropic_key(monkeypatch):
    pytest.importorskip("anthropic")
    from lakesense.interpreter.providers.anthropic_provider import AnthropicProvider

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY is required"):
        AnthropicProvider(api_key=None)


def test_missing_openai_key(monkeypatch):
    pytest.importorskip("openai")
    from lakesense.interpreter.providers.openai_provider import OpenAIProvider

    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(ValueError, match="OPENAI_API_KEY is required"):
        OpenAIProvider(api_key=None)
