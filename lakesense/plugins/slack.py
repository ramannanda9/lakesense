"""
SlackAlertPlugin — posts data quality alerts to Slack.

Requires: pip install lakesense[slack]

Usage:
    from lakesense.plugins.slack import SlackAlertPlugin

    framework = (
        SketchFramework(storage=backend)
        .register(SlackAlertPlugin(
            webhook="https://hooks.slack.com/services/...",
            alert_on={"alert", "warn"},
            channel="#ml-data-quality",   # optional display name
        ))
        .register(StoragePlugin())
    )

The plugin uses owners from InterpretationResult (populated by InvestigativeAgentPlugin)
to @mention dataset owners in the alert. If owners are absent, posts without mentions.
"""

from __future__ import annotations

import logging
from typing import Any

from lakesense.core.plugin import SketchPlugin
from lakesense.core.result import InterpretationResult, Severity

logger = logging.getLogger(__name__)

_SEVERITY_EMOJI = {
    Severity.OK: "✅",
    Severity.WARN: "⚠️",
    Severity.ALERT: "🚨",
}

_SEVERITY_COLOR = {
    Severity.OK: "#2eb886",  # green
    Severity.WARN: "#daa038",  # amber
    Severity.ALERT: "#cc0000",  # red
}


class SlackAlertPlugin(SketchPlugin):
    """
    Posts a formatted Slack alert when severity matches alert_on.

    Args:
        webhook:    Slack incoming webhook URL
        alert_on:   set of severity values that trigger an alert
                    default: {"alert", "warn"}
        channel:    optional display name shown in alert header
        only_after_agent: if True (default), only alert when root_cause is set.
                          Prevents noisy alerts before the agent has investigated.
    """

    def __init__(
        self,
        webhook: str,
        alert_on: set[str] | None = None,
        channel: str = "#ml-data-quality",
        only_after_agent: bool = True,
    ) -> None:
        self._webhook = webhook
        self._alert_on = alert_on or {"alert", "warn"}
        self._channel = channel
        self._only_after_agent = only_after_agent

    def should_run(self, result: InterpretationResult) -> bool:
        if result.severity.value not in self._alert_on:
            return False
        return not self._only_after_agent or result.is_agent_enriched()

    async def run(self, result: InterpretationResult) -> InterpretationResult:
        try:
            import httpx
        except ImportError as e:
            raise ImportError("SlackAlertPlugin requires httpx. Install with: pip install lakesense[slack]") from e

        payload = self._build_payload(result)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(self._webhook, json=payload, timeout=10.0)
                resp.raise_for_status()
        except Exception as e:
            logger.error("Slack webhook failed (pipeline continues): %s", e)
            result.metadata["slack_error"] = str(e)
            return result

        result.metadata["slack_sent"] = True
        result.metadata["slack_channel"] = self._channel
        logger.info(
            "slack alert sent: dataset=%s severity=%s",
            result.dataset_id,
            result.severity.value,
        )
        return result

    def _build_payload(self, result: InterpretationResult) -> dict[str, Any]:
        emoji = _SEVERITY_EMOJI[result.severity]
        color = _SEVERITY_COLOR[result.severity]

        # @mention owners if available (populated by InvestigativeAgentPlugin)
        mentions = " ".join(f"<@{o}>" for o in result.owners) if result.owners else ""

        blocks: list[dict[str, Any]] = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": (f"{emoji} Data Quality {result.severity.value.upper()}: {result.dataset_id}")[:150],
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job:*\n`{result.job_id}`"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{result.severity.value.upper()}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Summary:*\n{result.summary}"},
            },
        ]

        if result.root_cause:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Root cause:*\n{result.root_cause}"},
                }
            )

        if result.drift_signals.worst_signal() != "no signals":
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Drift signals:*\n`{result.drift_signals.worst_signal()}`",
                    },
                }
            )

        if result.affected_urns:
            affected = "\n".join(f"• `{u}`" for u in result.affected_urns[:5])
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Affected datasets:*\n{affected}"},
                }
            )

        if mentions:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Owners:* {mentions}"},
                }
            )

        blocks.append({"type": "divider"})

        return {
            "attachments": [{"color": color, "blocks": blocks}],
            "username": "lakesense",
            "icon_emoji": ":bar_chart:",
        }
