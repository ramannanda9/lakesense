"""
Slack utility tools for the lakesense LLM Agent.

Requires: pip install lakesense[slack]
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


class SlackIncidentSearchTool:
    """
    Exposes Slack message search to the LLM agent to find
    recent incidents correlated with data drift.
    """

    def __init__(self, token: str) -> None:
        self.token = token

    async def search_slack_incidents(self, keywords: str) -> str:
        """
        Search recent messages in Slack for a given keyword
        (like a table name or service outage). Returns latest matching messages.
        """
        try:
            import httpx  # type: ignore
        except ImportError:
            return "Error: httpx is not installed. Run pip install lakesense[slack]."

        logger.info("Agent called search_slack_incidents with keywords: %s", keywords)

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.get(
                    "https://slack.com/api/search.messages",
                    headers={"Authorization": f"Bearer {self.token}"},
                    params={"query": keywords, "sort": "timestamp", "sort_dir": "desc", "count": 5},
                )

            res_json = res.json()
            if not res_json.get("ok"):
                return f"Slack API Error: {res_json.get('error', 'unknown error')}"

            messages = res_json.get("messages", {}).get("matches", [])
            if not messages:
                return f"No messages found matching '{keywords}'."

            parsed_messages = []
            for m in messages:
                parsed_messages.append(
                    {
                        "ts": m.get("ts"),
                        "channel": m.get("channel", {}).get("name"),
                        "user": m.get("username", m.get("user")),
                        "text": m.get("text", "")[:500],  # truncate very long messages
                    }
                )

            return json.dumps({"search_results": parsed_messages})

        except Exception as e:
            logger.error("Slack Search API failed: %s", e)
            return f"Error connecting to Slack: {str(e)}"
