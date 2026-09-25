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

    async def test_search_is_scoped_to_the_user_and_agent_sources(self, graph: MagicMock) -> None:
        await KnowledgeHub(_state(graph)).list_files(query="budget")

        graph.get_user_by_user_id.assert_awaited_once_with(user_id=USER_ID)
        kwargs = _search_kwargs(graph)
        assert kwargs["user_key"] == USER_KEY
        assert kwargs["org_id"] == ORG_ID
        assert kwargs["connector_ids"] == AGENT_APPS
        assert kwargs["record_group_ids"] == AGENT_KBS

    async def test_caller_cannot_choose_the_user_or_org(self) -> None:
        params = inspect.signature(KnowledgeHub.list_files).parameters
        assert not {"user_id", "org_id", "user_key"} & set(params)

    async def test_requested_connectors_are_narrowed_never_widened(self, graph: MagicMock) -> None:
        await KnowledgeHub(_state(graph)).list_files(
            query="budget", connector_ids=["app-drive", "app-someone-elses"],
        )
        assert _search_kwargs(graph)["connector_ids"] == ["app-drive"]

    async def test_connectors_outside_the_agent_fall_back_to_its_own(self, graph: MagicMock) -> None:
        await KnowledgeHub(_state(graph)).list_files(
            query="budget", connector_ids=["app-someone-elses"],
        )
        assert _search_kwargs(graph)["connector_ids"] == AGENT_APPS

    async def test_requested_kbs_are_narrowed_never_widened(self, graph: MagicMock) -> None:
        state = _state(graph, kb=["kb-hr", "kb-eng"])
        await KnowledgeHub(state).list_files(query="policy", record_group_ids=["kb-eng", "kb-finance"])
        assert _search_kwargs(graph)["record_group_ids"] == ["kb-eng"]

    async def test_kbs_outside_the_agent_fall_back_to_its_own(self, graph: MagicMock) -> None:
        await KnowledgeHub(_state(graph)).list_files(query="policy", record_group_ids=["kb-finance"])
        assert _search_kwargs(graph)["record_group_ids"] == AGENT_KBS

    async def test_single_strings_are_accepted_for_list_filters(self, graph: MagicMock) -> None:
        await KnowledgeHub(_state(graph)).list_files(
            query="budget", connector_ids="app-jira", node_types="record", record_types="FILE",
        )
        kwargs = _search_kwargs(graph)
        assert kwargs["connector_ids"] == ["app-jira"]
        assert kwargs["node_types"] == ["record"]
        assert kwargs["record_types"] == ["FILE"]

    async def test_unknown_node_types_and_sort_values_are_dropped(self, graph: MagicMock) -> None:
        await KnowledgeHub(_state(graph)).list_files(
            query="budget", node_types=["spreadsheet"], sort_by="DROP TABLE", sort_order="sideways",
        )
        kwargs = _search_kwargs(graph)
        assert kwargs["node_types"] is None
        assert (kwargs["sort_field"], kwargs["sort_dir"]) == ("updatedAt", "DESC")

    async def test_long_queries_are_cut_to_the_maximum(self, graph: MagicMock) -> None:
        await KnowledgeHub(_state(graph)).list_files(query="x" * (MAX_QUERY_LENGTH + 200))
        assert _search_kwargs(graph)["search_query"] == "x" * MAX_QUERY_LENGTH


class TestPagination:
    async def test_page_and_limit_become_an_offset(self, graph: MagicMock) -> None:
        graph.get_knowledge_hub_search.return_value = {
            "nodes": [_node(f"rec-{i}", f"doc {i}") for i in range(10)],
            "total": 45,
        }
        ok, payload = await KnowledgeHub(_state(graph)).list_files(query="doc", page=3, limit=10)

        assert ok is True
        kwargs = _search_kwargs(graph)
        assert (kwargs["skip"], kwargs["limit"]) == (20, 10)
        pagination = json.loads(payload)["pagination"]
        assert pagination == {
            "page": 3, "limit": 10, "totalItems": 45, "totalPages": 5,
            "hasNext": True, "hasPrev": True,
        }

    @pytest.mark.parametrize(
        ("page", "limit", "expected_skip", "expected_limit"),
        [(0, 20, 0, 20), (-4, 0, 0, 1), (2, 500, 50, 50)],
    )
    async def test_out_of_range_values_are_clamped(
        self, graph: MagicMock, page: int, limit: int, expected_skip: int, expected_limit: int,
    ) -> None:
        await KnowledgeHub(_state(graph)).list_files(query="doc", page=page, limit=limit)
        kwargs = _search_kwargs(graph)
        assert (kwargs["skip"], kwargs["limit"]) == (expected_skip, expected_limit)

    async def test_last_page_reports_no_next_page(self, graph: MagicMock) -> None:
        graph.get_knowledge_hub_search.return_value = {"nodes": [_node("rec-9", "doc 9")], "total": 21}
        _, payload = await KnowledgeHub(_state(graph)).list_files(query="doc", page=3, limit=10)
        pagination = json.loads(payload)["pagination"]
        assert (pagination["hasNext"], pagination["hasPrev"]) == (False, True)


class TestBrowsing:
    async def test_short_query_browses_only_the_agents_sources(self, graph: MagicMock) -> None:
        graph.get_user_app_ids.return_value = [*AGENT_APPS, "app-not-on-this-agent"]
        await KnowledgeHub(_state(graph)).list_files(query="a")

        if graph.get_knowledge_hub_search.await_count:
            kwargs = _search_kwargs(graph)
            assert kwargs["search_query"] is None
            assert kwargs["connector_ids"] == AGENT_APPS
        else:
            kwargs = graph.get_knowledge_hub_root_nodes.await_args.kwargs
            assert kwargs["user_app_ids"] == AGENT_APPS

    async def test_parent_without_type_is_refused(self, graph: MagicMock) -> None:
        ok, payload = await KnowledgeHub(_state(graph)).list_files(query="x", parent_id="folder-1")

        assert ok is False
        assert "parent_type is required" in json.loads(payload)["message"]
        graph.get_user_by_user_id.assert_not_awaited()

    async def test_browsing_a_folder_lists_its_children_for_this_user(self, graph: MagicMock) -> None:
        graph.get_knowledge_hub_node_info.return_value = {"id": "folder-1", "name": "Plans", "nodeType": "folder"}
        graph.get_knowledge_hub_children.return_value = {"nodes": [_node("rec-2", "plan.md")], "total": 1}

        ok, payload = await KnowledgeHub(_state(graph)).list_files(parent_id="folder-1", parent_type="folder")

        assert ok is True
        kwargs = graph.get_knowledge_hub_children.await_args.kwargs
        assert (kwargs["parent_id"], kwargs["user_key"], kwargs["org_id"]) == ("folder-1", USER_KEY, ORG_ID)
        assert kwargs["record_group_ids"] == AGENT_KBS
        assert json.loads(payload)["currentNode"]["name"] == "Plans"

    async def test_a_folder_that_is_gone_or_hidden_reads_as_not_found(self, graph: MagicMock) -> None:
        ok, payload = await KnowledgeHub(_state(graph)).list_files(parent_id="folder-x", parent_type="folder")

        assert ok is False
        assert "no longer have access" in json.loads(payload)["message"]
        graph.get_knowledge_hub_children.assert_not_awaited()

    async def test_switched_off_demo_data_is_not_browsable(self, graph: MagicMock) -> None:
        state = _state(graph, excluded_app_ids=frozenset({"app-demo"}))
        ok, payload = await KnowledgeHub(state).list_files(parent_id="app-demo", parent_type="app")

        assert ok is False
        assert "no longer have access" in json.loads(payload)["message"]
        graph.get_knowledge_hub_children.assert_not_awaited()

    async def test_only_fetchable_ids_are_remembered(self, graph: MagicMock) -> None:
        graph.get_knowledge_hub_search.return_value = {
            "nodes": [
                _node("rec-1", "a.pdf", "record"),
                _node("fold-1", "Docs", "folder"),
                _node("app-jira", "Jira", "app"),
                _node("rg-1", "Space", "recordGroup"),
            ],
            "total": 4,
        }
        state = _state(graph)
        await KnowledgeHub(state).list_files(query="docs")
        assert state["known_record_ids"] == {"rec-1", "fold-1"}


class TestFailures:
    async def test_without_state(self) -> None:
        ok, payload = await KnowledgeHub(None).list_files(query="budget")
        assert ok is False
        assert json.loads(payload)["status"] == "error"

    async def test_without_graph_provider(self, graph: MagicMock) -> None:
        ok, payload = await KnowledgeHub(_state(graph, graph_provider=None)).list_files(query="budget")
        assert ok is False
        assert json.loads(payload)["message"] == "Graph provider not available"

    async def test_agent_without_sources_searches_nothing(self, graph: MagicMock) -> None:
        state = _state(graph, apps=[], kb=[], has_knowledge=False)
        ok, payload = await KnowledgeHub(state).list_files(query="budget")

        assert ok is False
        assert json.loads(payload)["message"] == "No knowledge sources configured for this agent"
        graph.get_knowledge_hub_search.assert_not_awaited()

    async def test_unknown_user_gets_an_error_not_results(self, graph: MagicMock) -> None:
        graph.get_user_by_user_id.return_value = None
        ok, payload = await KnowledgeHub(_state(graph)).list_files(query="budget")

        assert ok is False
        assert json.loads(payload)["message"] == "User not found"
        graph.get_knowledge_hub_search.assert_not_awaited()

    async def test_database_failure_reads_as_plain_language(self, graph: MagicMock, caplog: pytest.LogCaptureFixture) -> None:
        graph.get_knowledge_hub_search.side_effect = ConnectionError("arangodb://root:hunter2@graph:8529 refused")
        ok, payload = await KnowledgeHub(_state(graph)).list_files(query="budget")

        assert ok is False
        message = json.loads(payload)["message"]
        assert message.startswith("We couldn't")
        assert "hunter2" not in payload
        assert "Traceback" not in payload

    async def test_unexpected_error_is_reported_without_a_traceback(self, graph: MagicMock) -> None:
        state = _state(graph, excluded_app_ids=42)
        ok, payload = await KnowledgeHub(state).list_files(query="budget")

        assert ok is False
        body = json.loads(payload)
        assert body["status"] == "error"
        assert body["message"].startswith("Knowledge hub error:")
        assert "Traceback" not in payload
