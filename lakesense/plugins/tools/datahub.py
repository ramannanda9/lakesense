"""
DataHub utility tools for the lakesense LLM Agent.

Requires: pip install lakesense[datahub]
"""

from __future__ import annotations

import json
import logging

logger = logging.getLogger(__name__)


def _get_datahub_graph(endpoint: str, token: str | None = None):
    """Initialize a DataHubGraph connection lazily."""
    try:
        from datahub.emitter.mcp_builder import DatahubClientConfig
        from datahub.ingestion.graph.client import DataHubGraph
    except ImportError as e:
        raise ImportError("acryl-datahub is not installed. Run pip install lakesense[datahub]") from e

    config = DatahubClientConfig(server=endpoint, token=token)
    return DataHubGraph(config)


class DataHubLineageTool:
    """
    Exposes lineage checking to the LLM agent.

    Usage:
        tool = DataHubLineageTool(endpoint="http://my-datahub.local")
        agent = InvestigativeAgentPlugin(tools=[tool.get_upstream_lineage, tool.get_downstream_lineage])
    """

    def __init__(self, endpoint: str, token: str | None = None) -> None:
        self.endpoint = endpoint
        self.token = token

    def get_upstream_lineage(self, dataset_urn: str) -> str:
        """
        Check DataHub for the upstream lineages/parent schemas of a given dataset by URN.
        Returns a JSON payload with upstream dataset names.
        """
        try:
            graph = _get_datahub_graph(self.endpoint, self.token)
        except ImportError as e:
            return f"Error: {e}"

        logger.info("Agent called get_upstream_lineage for %s", dataset_urn)

        try:
            lineage = graph.get_lineage(entity_urn=dataset_urn, direction="UPSTREAM")
            if not lineage:
                return json.dumps({"upstream_dependencies": []})

            urns = [edge.sourceUrn for edge in lineage.upstreams]
            return json.dumps({"upstream_dependencies": urns})
        except Exception as e:
            logger.error("DataHub Graph API failed: %s", e)
            return f"Error fetching lineage: {str(e)}"

    def get_downstream_lineage(self, dataset_urn: str) -> str:
        """
        Check DataHub for the downstream consumers (blast radius) of a given dataset by URN.
        Returns a JSON payload of downstream URNs and their types (dashboard, model, table).
        """
        try:
            graph = _get_datahub_graph(self.endpoint, self.token)
        except ImportError as e:
            return f"Error: {e}"

        logger.info("Agent called get_downstream_lineage for %s", dataset_urn)

        try:
            lineage = graph.get_lineage(entity_urn=dataset_urn, direction="DOWNSTREAM")
            if not lineage:
                return json.dumps({"downstream_consumers": []})

            urns = [edge.destinationUrn for edge in lineage.downstreams]
            return json.dumps({"downstream_consumers": urns})
        except Exception as e:
            logger.error("DataHub Graph API failed: %s", e)
            return f"Error fetching lineage: {str(e)}"


class DataHubSearchTool:
    """
    Exposes dataset search resolution to the LLM agent.
    """

    def __init__(self, endpoint: str, token: str | None = None) -> None:
        self.endpoint = endpoint
        self.token = token

    def search_datahub_dataset(self, dataset_name: str) -> str:
        """
        Resolve a friendly dataset name (like 'user_features') to its official DataHub URN.
        Always run this before pulling lineage if you only have a short ID.
        """
        try:
            graph = _get_datahub_graph(self.endpoint, self.token)
        except ImportError as e:
            return f"Error: {e}"

        try:
            # Performs a basic entity search across Dataset entity types
            res = graph.execute_graphql(
                query="""
                query search($input: SearchInput!) {
                  search(input: $input) {
                    searchResults {
                      entity {
                        urn
                      }
                    }
                  }
                }
                """,
                variables={"input": {"type": "DATASET", "query": dataset_name, "start": 0, "count": 5}},
            )

            hits = []
            if "search" in res and "searchResults" in res["search"]:
                for result in res["search"]["searchResults"]:
                    hits.append(result["entity"]["urn"])

            if not hits:
                return f"No results found for {dataset_name}"

            return json.dumps({"search_results": hits})

        except Exception as e:
            logger.error("DataHub GraphQL Search failed: %s", e)
            return f"Error searching: {str(e)}"
