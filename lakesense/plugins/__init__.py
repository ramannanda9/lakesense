"""
lakesense.plugins — built-in Tier 2 plugins.

    StoragePlugin   — persists InterpretationResult (always register last)
    SlackAlertPlugin — posts alerts to Slack (requires lakesense[slack])

Custom plugins implement SketchPlugin from lakesense.core.
"""

from lakesense.plugins.slack import SlackAlertPlugin
from lakesense.plugins.store import StoragePlugin

__all__ = ["StoragePlugin", "SlackAlertPlugin"]
