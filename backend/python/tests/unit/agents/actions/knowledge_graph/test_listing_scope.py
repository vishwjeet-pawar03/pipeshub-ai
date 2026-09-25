"""`knowledgegraph__list_files` through the real `KnowledgeHubService` and
scope resolver; only the graph database is stubbed, with an autospec of the
production `ArangoHTTPProvider`. The stubbed search applies the connector
filter before paging, as the provider's query does.
"""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock, create_autospec

import pytest

from app.agents.actions.knowledge_graph.ops.listing import execute_list_files
from app.services.graph_db.arango.arango_http_provider import ArangoHTTPProvider

USER_KEY = "user-key-1"


def _node(node_id: str, name: str, node_type: str = "record") -> dict[str, Any]:
    return {"id": node_id, "name": name, "nodeType": node_type, "origin": "CONNECTOR"}


# (connector id, node) pairs the user can see: three apps and two KBs.
_USER_VISIBLE = [
    ("app-jira", _node("jira-1", "budget ticket")),
    ("app-drive", _node("drive-1", "budget sheet")),
    ("app-slack", _node("slack-1", "budget thread")),
    ("kb-hr", _node("hr-1", "budget policy")),
    ("kb-hr", _node("hr-2", "budget faq")),
    ("kb-finance", _node("fin-1", "budget plan")),
]
_USER_APPS = ["app-jira", "app-drive", "app-slack", "kb-hr", "kb-finance"]


def _provider_search(
    *, skip: int, limit: int, search_query: str | None = None,
    connector_ids: list[str] | None = None, **_: object,
) -> dict[str, Any]:
    matches = [
        node for cid, node in _USER_VISIBLE
        if (not connector_ids or cid in connector_ids)
        and (not search_query or search_query in node["name"])
    ]
    return {"nodes": matches[skip:skip + limit], "total": len(matches)}


def _provider_root_nodes(*, user_app_ids: list[str], skip: int, limit: int, **_: object) -> dict[str, Any]:
    nodes = [_node(app_id, app_id, "app") for app_id in user_app_ids]
    return {"nodes": nodes[skip:skip + limit], "total": len(nodes)}


@pytest.fixture
def graph() -> MagicMock:
    g = create_autospec(ArangoHTTPProvider, instance=True)
    g.get_user_by_user_id.return_value = {"_key": USER_KEY}
    g.get_knowledge_hub_search.side_effect = _provider_search
    g.get_knowledge_hub_root_nodes.side_effect = _provider_root_nodes
    g.get_user_app_ids.return_value = list(_USER_APPS)
    g.get_user_permission_app_ids.return_value = []
    g.get_knowledge_hub_filter_options.return_value = {"apps": []}
    return g


def _state(graph: MagicMock, *, apps: list[str], kb: list[str]) -> dict[str, Any]:
    return {
        "graph_provider": graph,
        "org_id": "org-1",
        "user_id": "user-ext-1",
        "apps": apps,
        "kb": kb,
        "logger": logging.getLogger("test.kg_listing"),
    }


def _ids(text: str) -> set[str]:
    return {part.split("=", 1)[1].split()[0] for part in text.split("|") if "_id=" in part}


class TestSearchByName:
    async def test_a_name_search_searches(self, graph: MagicMock) -> None:
        ok, text = await execute_list_files(_state(graph, apps=["app-jira"], kb=[]), query="budget")

        assert ok is True
        graph.get_knowledge_hub_search.assert_awaited_once()
        assert graph.get_knowledge_hub_search.await_args.kwargs["search_query"] == "budget"
        graph.get_knowledge_hub_root_nodes.assert_not_awaited()
        assert _ids(text) == {"jira-1"}

    async def test_without_a_query_it_lists(self, graph: MagicMock) -> None:
        await execute_list_files(_state(graph, apps=["app-jira"], kb=[]))

        graph.get_knowledge_hub_search.assert_not_awaited()
        graph.get_knowledge_hub_root_nodes.assert_awaited_once()


class TestStaysInsideTheAgentsSources:
    async def test_kb_only_agent_search_sees_only_its_kbs(self, graph: MagicMock) -> None:
        _, text = await execute_list_files(_state(graph, apps=[], kb=["kb-hr"]), query="budget")
        assert _ids(text) == {"hr-1", "hr-2"}

    async def test_kb_only_agent_listing_shows_only_its_kbs(self, graph: MagicMock) -> None:
        _, text = await execute_list_files(_state(graph, apps=[], kb=["kb-hr"]))
        assert _ids(text) == {"kb-hr"}

    async def test_mixed_agent_search_sees_only_its_sources(self, graph: MagicMock) -> None:
        _, text = await execute_list_files(_state(graph, apps=["app-jira"], kb=["kb-hr"]), query="budget")
        assert _ids(text) == {"jira-1", "hr-1", "hr-2"}

    async def test_mixed_agent_listing_shows_only_its_sources(self, graph: MagicMock) -> None:
        _, text = await execute_list_files(_state(graph, apps=["app-jira"], kb=["kb-hr"]))
        assert _ids(text) == {"app-jira", "kb-hr"}

    async def test_narrowing_to_one_kb_never_widens(self, graph: MagicMock) -> None:
        state = _state(graph, apps=["app-jira"], kb=["kb-hr"])
        _, text = await execute_list_files(state, query="budget", source_ids=["kb-hr", "kb-finance"])
        assert _ids(text) == {"hr-1", "hr-2"}

    async def test_scoped_results_page_with_correct_totals(self, graph: MagicMock) -> None:
        state = _state(graph, apps=["app-jira"], kb=["kb-hr"])
        pages = [await execute_list_files(state, query="budget", page=p, limit=2) for p in (1, 2)]

        assert [len(_ids(text)) for _, text in pages] == [2, 1]
        assert set().union(*(_ids(text) for _, text in pages)) == {"jira-1", "hr-1", "hr-2"}
        kwargs = [c.kwargs for c in graph.get_knowledge_hub_search.await_args_list]
        assert [(k["skip"], k["limit"]) for k in kwargs] == [(0, 2), (2, 2)]
