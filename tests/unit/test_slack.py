"""Unit tests for SlackAlertPlugin."""

from unittest.mock import AsyncMock, patch

import pytest

from lakesense.core.result import DatasetDriftSummary, InterpretationResult, Severity
from lakesense.plugins.slack import SlackAlertPlugin


class TestSlackShouldRun:
    def _result(self, severity=Severity.OK, root_cause=None):
        return InterpretationResult(
            dataset_id="ds1",
            job_id="j1",
            severity=severity,
            root_cause=root_cause,
        )

    def test_skips_ok(self):
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test")
        assert not plugin.should_run(self._result(Severity.OK))

    def test_skips_warn_without_agent(self):
        """Default only_after_agent=True means we wait for root_cause."""
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test")
        assert not plugin.should_run(self._result(Severity.WARN))

    def test_runs_warn_with_agent_enriched(self):
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test")
        assert plugin.should_run(self._result(Severity.WARN, root_cause="upstream broke"))

    def test_runs_alert_with_agent_enriched(self):
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test")
        assert plugin.should_run(self._result(Severity.ALERT, root_cause="schema drop"))

    def test_runs_without_agent_when_disabled(self):
        """When only_after_agent=False, should fire even without root_cause."""
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test", only_after_agent=False)
        assert plugin.should_run(self._result(Severity.ALERT))

    def test_custom_alert_on(self):
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test", alert_on={"alert"}, only_after_agent=False)
        assert not plugin.should_run(self._result(Severity.WARN))
        assert plugin.should_run(self._result(Severity.ALERT))


class TestSlackPayload:
    def test_payload_has_blocks(self):
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test")
        result = InterpretationResult(
            dataset_id="user_features",
            job_id="train_42",
            severity=Severity.ALERT,
            summary="Jaccard dropped 40%",
            root_cause="upstream schema migration",
            dataset_drift_summary=DatasetDriftSummary(jaccard_delta=-0.4),
            owners=["U001", "U002"],
            affected_urns=["urn:li:dataset:parent_table"],
        )
        payload = plugin._build_payload(result)

        assert "attachments" in payload
        blocks = payload["attachments"][0]["blocks"]

        # Header, job/severity fields, summary, root cause, drift signals, affected, owners, divider
        block_types = [b["type"] for b in blocks]
        assert "header" in block_types
        assert "divider" in block_types

        # Color should be red for ALERT
        assert payload["attachments"][0]["color"] == "#cc0000"

    def test_header_truncated(self):
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/test")
        result = InterpretationResult(
            dataset_id="a" * 200,
            job_id="j",
            severity=Severity.ALERT,
            summary="test",
        )
        payload = plugin._build_payload(result)
        header_text = payload["attachments"][0]["blocks"][0]["text"]["text"]
        assert len(header_text) <= 150


class TestSlackErrorResilience:
    @pytest.mark.asyncio
    async def test_webhook_failure_does_not_crash(self):
        """A failed webhook should log error and return result, not raise."""
        pytest.importorskip("httpx")
        plugin = SlackAlertPlugin(webhook="https://hooks.slack.com/invalid-url")

        result = InterpretationResult(
            dataset_id="ds1",
            job_id="j1",
            severity=Severity.ALERT,
            summary="test",
            root_cause="test",
        )

        # Mock httpx to raise on POST
        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.side_effect = Exception("Connection refused")
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client_cls.return_value = mock_client

            out = await plugin.run(result)

        # Should return result with error metadata, not crash
        assert out.metadata.get("slack_error") is not None
        assert "slack_sent" not in out.metadata
