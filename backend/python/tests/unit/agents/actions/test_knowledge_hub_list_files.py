"""`knowledgehub.list_files` driven through the real `KnowledgeHubService`
and the real scope resolver; only the graph database is stubbed, with an
autospec of `IGraphDBProvider` so a call with the wrong arguments fails.
"""

from __future__ import annotations

import inspect
import json
import logging
from typing import Any
from unittest.mock import MagicMock, create_autospec

import pytest

from app.agents.actions.knowledge_hub.knowledge_hub import (
    MAX_QUERY_LENGTH,
    KnowledgeHub,
)
from app.services.graph_db.arango.arango_http_provider import ArangoHTTPProvider

ORG_ID = "org-1"
USER_ID = "user-ext-1"
USER_KEY = "user-key-1"
AGENT_APPS = ["app-jira", "app-drive"]
AGENT_KBS = ["kb-hr"]


def _node(node_id: str, name: str, node_type: str = "record") -> dict[str, Any]:
    return {"id": node_id, "name": name, "nodeType": node_type, "origin": "CONNECTOR"}


@pytest.fixture
def graph() -> MagicMock:
    g = create_autospec(ArangoHTTPProvider, instance=True)
    g.get_user_by_user_id.return_value = {"_key": USER_KEY}
    g.get_knowledge_hub_search.return_value = {"nodes": [], "total": 0}
    g.get_knowledge_hub_root_nodes.return_value = {"nodes": [], "total": 0}
    g.get_knowledge_hub_children.return_value = {"nodes": [], "total": 0}
    g.get_knowledge_hub_node_info.return_value = None
    g.get_knowledge_hub_parent_node.return_value = None
    g.get_user_app_ids.return_value = []
    g.get_user_permission_app_ids.return_value = []
    g.get_document.return_value = None
    g.get_knowledge_hub_filter_options.return_value = {"apps": []}
    return g


def _state(graph: MagicMock, **overrides: object) -> dict[str, Any]:
    state: dict[str, Any] = {
        "graph_provider": graph,
        "org_id": ORG_ID,
        "user_id": USER_ID,
        "apps": list(AGENT_APPS),
        "kb": list(AGENT_KBS),
        "logger": logging.getLogger("test.knowledge_hub"),
    }
    state.update(overrides)
    return state


def _search_kwargs(graph: MagicMock) -> dict[str, Any]:
    graph.get_knowledge_hub_search.assert_awaited_once()
    return graph.get_knowledge_hub_search.await_args.kwargs


class TestSearchByName:
    async def test_query_reaches_the_graph_search(self, graph: MagicMock) -> None:
        graph.get_knowledge_hub_search.return_value = {
            "nodes": [_node("rec-1", "Q3 budget.xlsx")],
            "total": 1,
        }
        ok, payload = await KnowledgeHub(_state(graph)).list_files(query="budget")

        assert ok is True
        kwargs = _search_kwargs(graph)
        assert kwargs["search_query"] == "budget"
        graph.get_knowledge_hub_root_nodes.assert_not_awaited()
        assert [i["name"] for i in json.loads(payload)["items"]] == ["Q3 budget.xlsx"]
