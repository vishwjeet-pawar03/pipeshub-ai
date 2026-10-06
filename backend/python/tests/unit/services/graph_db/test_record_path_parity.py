"""Arango and Neo4j must build the same ancestor path for a record or group.

Both providers return candidate chains and share ``select_canonical_chain_names``.
The fixtures are the anomalies that used to make the backends disagree:
duplicate edges, several canonical parents, equal-length ties, cycles.
"""

import inspect
import re
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames
from app.services.graph_db.arango.arango_http_provider import ArangoHTTPProvider
from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider
from app.services.graph_db.neo4j.neo4j_provider import Neo4jProvider


def _arango(rows=None, error=None):
    provider = object.__new__(ArangoHTTPProvider)
    provider.http_client = MagicMock(execute_aql=AsyncMock(return_value=rows, side_effect=error))
    provider.logger = MagicMock()
    return provider


def _arango_graph(rows, name_keys):
    """The graph a set of candidate chains describes, as Arango's two queries
    return it: every node once, then the distinct child -> parent links."""
    nodes, links = {}, []
    for row in rows:
        ids = row["ids"]
        for k, node_id in enumerate(ids):
            nodes.setdefault(node_id, {"id": node_id, **{key: row[src][k] for key, src in name_keys}})
            if k and {"child": ids[k - 1], "parent": node_id} not in links:
                links.append({"child": ids[k - 1], "parent": node_id})
    return [list(nodes.values()), links]


def _arango_from_rows(rows, name_keys):
    provider = object.__new__(ArangoHTTPProvider)
    provider.http_client = MagicMock(execute_aql=AsyncMock(side_effect=_arango_graph(rows, name_keys)))
    provider.logger = MagicMock()
    return provider


def _neo4j(rows=None, error=None):
    provider = object.__new__(Neo4jProvider)
    provider.client = MagicMock(execute_query=AsyncMock(return_value=rows, side_effect=error))
    provider.logger = MagicMock()
    return provider


RECORD_CASES = {
    "single_chain_with_prefix_rows": (
        [
            {"ids": ["f"], "names": ["f.txt"]},
            {"ids": ["f", "d"], "names": ["f.txt", "Docs"]},
            {"ids": ["f", "d", "r"], "names": ["f.txt", "Docs", "Root"]},
        ],
        ["Root", "Docs", "f.txt"],
    ),
    "duplicate_parent_edges": (
        [
            {"ids": ["f", "d"], "names": ["f.txt", "Docs"]},
            {"ids": ["f", "d"], "names": ["f.txt", "Docs"]},
        ],
        ["Docs", "f.txt"],
    ),
    "two_parents_longest_wins": (
        [
            {"ids": ["f", "a"], "names": ["f", "A"]},
            {"ids": ["f", "b", "br"], "names": ["f", "B", "BRoot"]},
        ],
        ["BRoot", "B", "f"],
    ),
    "two_parents_tie_smallest_ids": (
        [
            {"ids": ["f", "p2"], "names": ["f", "Two"]},
            {"ids": ["f", "p1"], "names": ["f", "One"]},
        ],
        ["One", "f"],
    ),
    # f -> a -> b -> a: both backends stop before revisiting a, so only these rows come back.
    "cycle_rows_without_repeated_vertex": (
        [
            {"ids": ["f"], "names": ["f"]},
            {"ids": ["f", "a"], "names": ["f", "A"]},
            {"ids": ["f", "a", "b"], "names": ["f", "A", "B"]},
        ],
        ["B", "A", "f"],
    ),
    "empty_names_dropped": ([{"ids": ["f", "d"], "names": ["f", ""]}], ["f"]),
    "unnamed_vertex_still_counts_for_length": (
        [
            {"ids": ["f", "a", "ar"], "names": ["f", None, "ARoot"]},
            {"ids": ["f", "b"], "names": ["f", "B"]},
        ],
        ["ARoot", "f"],
    ),
    "record_missing": ([], []),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True], ids=["as_returned", "reversed"])
@pytest.mark.parametrize("rows,expected", RECORD_CASES.values(), ids=RECORD_CASES.keys())
async def test_record_path_segments_parity(rows, expected, reverse):
    rows = list(reversed(rows)) if reverse else rows
    arango = await _arango_from_rows(rows, [("name", "names")]).get_record_path_segments("f")
    neo4j = await _neo4j(rows).get_record_path_segments("f")
    assert arango == neo4j == expected


@pytest.mark.asyncio
async def test_arango_record_query_visits_each_ancestor_once_then_reads_its_links():
    # A path traversal enumerated every path: 2^depth with parallel parent edges.
    provider = object.__new__(ArangoHTTPProvider)
    provider.http_client = MagicMock(execute_aql=AsyncMock(side_effect=[
        [{"id": "f", "name": "f"}, {"id": "d", "name": "Docs"}],
        [{"child": "f", "parent": "d"}],
    ]))
    provider.logger = MagicMock()
    assert await provider.get_record_path_segments("f", transaction="tx-1") == ["Docs", "f"]
    first, second = provider.http_client.execute_aql.await_args_list
    query = first.args[0]
    assert "GRAPH" not in query
    assert f"INBOUND start_record {CollectionNames.NODE_RELATIONS.value}" in query
    assert 'uniqueVertices: "global"' in query and 'order: "bfs"' in query
    assert query.index("PRUNE") < query.index("OPTIONS")
    assert "externalParentId != null" in query
    assert first.kwargs["bind_vars"] == {
        "record_id": "f", "records_collection": "records",
        "relation_types": ["PARENT_CHILD", "ATTACHMENT"],
    }
    assert "RETURN DISTINCT" in second.args[0]
    assert second.kwargs["bind_vars"]["ids"] == ["records/f", "records/d"]
    assert first.kwargs["txn_id"] == second.kwargs["txn_id"] == "tx-1"


@pytest.mark.asyncio
async def test_neo4j_record_query_prunes_inside_the_quantified_pattern():
    provider = _neo4j([])
    await provider.get_record_path_segments("f", transaction="tx-1")
    call = provider.client.execute_query.await_args
    query = call.args[0]
    assert "){0,100}" in query
    assert "NODE_RELATION*" not in query
    assert "RECORD_LINK" not in query
    assert "ORDER BY size(path_nodes) DESC, [n IN path_nodes | n.id] ASC" in query
    assert call.kwargs["parameters"] == {
        "record_id": "f",
        "relation_types": ["PARENT_CHILD", "ATTACHMENT"],
        "max_candidates": 64,
    }
    assert call.kwargs["txn_id"] == "tx-1"


@pytest.mark.asyncio
async def test_neo4j_record_query_excludes_paths_that_repeat_a_vertex():
    # Arango gets this from uniqueVertices: "path"; Cypher's default match mode
    # only keeps relationships distinct, so the query must drop repeats itself.
    provider = _neo4j([])
    await provider.get_record_path_segments("f")
    query = " ".join(provider.client.execute_query.await_args.args[0].split())
    assert "NOT path_nodes[i] IN path_nodes[i + 1..]" in query
    assert query.index("NOT path_nodes[i] IN") < query.index("ORDER BY")


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_arango, _neo4j], ids=["arango", "neo4j"])
async def test_record_path_error_default_returns_empty(make):
    provider = make(error=RuntimeError("down"))
    assert await provider.get_record_path_segments("f") == []
    provider.logger.error.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_arango, _neo4j], ids=["arango", "neo4j"])
async def test_record_path_error_raises_when_asked(make):
    with pytest.raises(RuntimeError):
        await make(error=RuntimeError("down")).get_record_path_segments("f", raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_arango, _neo4j], ids=["arango", "neo4j"])
async def test_record_missing_with_raise_on_error_returns_empty(make):
    assert await make([]).get_record_path_segments("f", raise_on_error=True) == []


GROUP_CASES = {
    "root_only": ([{"ids": ["g"], "groupNames": ["G"], "names": [None]}], ["G"]),
    "nested": (
        [
            {"ids": ["g"], "groupNames": ["G"], "names": [None]},
            {"ids": ["g", "p"], "groupNames": ["G", "P"], "names": [None, None]},
        ],
        ["P", "G"],
    ),
    "two_parents_tie_smallest_ids": (
        [
            {"ids": ["g", "pb"], "groupNames": ["G", "B"], "names": [None, None]},
            {"ids": ["g", "pa"], "groupNames": ["G", "A"], "names": [None, None]},
        ],
        ["A", "G"],
    ),
    "empty_group_name_falls_back_to_name": (
        [{"ids": ["g", "p"], "groupNames": ["G", ""], "names": [None, "Parent"]}],
        ["Parent", "G"],
    ),
    # g -> a -> b -> a: both backends stop before revisiting a, so only these rows come back.
    "cycle_rows_without_repeated_vertex": (
        [
            {"ids": ["g"], "groupNames": ["G"], "names": [None]},
            {"ids": ["g", "a"], "groupNames": ["G", "A"], "names": [None, None]},
            {"ids": ["g", "a", "b"], "groupNames": ["G", "A", "B"], "names": [None, None, None]},
        ],
        ["B", "A", "G"],
    ),
    "group_missing": ([], []),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse", [False, True], ids=["as_returned", "reversed"])
@pytest.mark.parametrize("rows,expected", GROUP_CASES.values(), ids=GROUP_CASES.keys())
async def test_record_group_path_parity(rows, expected, reverse):
    rows = list(reversed(rows)) if reverse else rows
    arango = await _arango_from_rows(
        rows, [("groupName", "groupNames"), ("name", "names")],
    ).get_record_group_path("g")
    neo4j = await _neo4j(rows).get_record_group_path("g")
    assert arango == neo4j == expected


@pytest.mark.asyncio
async def test_arango_group_query_visits_each_group_once_then_reads_its_links():
    provider = object.__new__(ArangoHTTPProvider)
    provider.http_client = MagicMock(execute_aql=AsyncMock(side_effect=[
        [{"id": "g", "groupName": "G", "name": None}, {"id": "p", "groupName": "P", "name": None}],
        [{"child": "g", "parent": "p"}],
    ]))
    provider.logger = MagicMock()
    assert await provider.get_record_group_path("g", transaction="tx-1") == ["P", "G"]
    first, second = provider.http_client.execute_aql.await_args_list
    query = first.args[0]
    assert "OUTBOUND start_rg belongsTo" in query
    assert 'uniqueVertices: "global"' in query
    assert query.index("PRUNE") < query.index("OPTIONS")
    assert first.kwargs["bind_vars"] == {"record_group_id": "g", "rg_collection": "recordGroups"}
    assert second.kwargs["bind_vars"]["ids"] == ["recordGroups/g", "recordGroups/p"]
    assert first.kwargs["txn_id"] == second.kwargs["txn_id"] == "tx-1"


@pytest.mark.asyncio
async def test_neo4j_group_query_only_crosses_record_groups():
    provider = _neo4j([])
    await provider.get_record_group_path("g", transaction="tx-1")
    call = provider.client.execute_query.await_args
    query = call.args[0]
    assert "((child:RecordGroup)-[edge:BELONGS_TO]->(parent:RecordGroup)" in query
    assert "}){0,50}" in query
    # One of several parallel BELONGS_TO edges is followed (2^depth otherwise).
    assert "elementId(twin) < elementId(edge)" in query
    assert "BELONGS_TO*" not in query
    assert "ORDER BY size(path_nodes) DESC, [n IN path_nodes | n.id] ASC" in query
    assert call.kwargs["parameters"] == {
        "record_group_id": "g",
        "max_candidates": 64,
    }
    assert call.kwargs["txn_id"] == "tx-1"


@pytest.mark.asyncio
async def test_neo4j_group_query_excludes_paths_that_repeat_a_vertex():
    provider = _neo4j([])
    await provider.get_record_group_path("g")
    query = " ".join(provider.client.execute_query.await_args.args[0].split())
    assert "NOT path_nodes[i] IN path_nodes[i + 1..]" in query
    assert query.index("NOT path_nodes[i] IN") < query.index("ORDER BY")


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_arango, _neo4j], ids=["arango", "neo4j"])
async def test_group_path_error_default_returns_empty(make):
    provider = make(error=RuntimeError("down"))
    assert await provider.get_record_group_path("g") == []
    provider.logger.error.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_arango, _neo4j], ids=["arango", "neo4j"])
async def test_group_path_error_raises_when_asked(make):
    with pytest.raises(RuntimeError):
        await make(error=RuntimeError("down")).get_record_group_path("g", raise_on_error=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("make", [_arango, _neo4j], ids=["arango", "neo4j"])
async def test_group_missing_with_raise_on_error_returns_empty(make):
    assert await make([]).get_record_group_path("g", raise_on_error=True) == []


def _squash(text: str) -> str:
    return " ".join(text.split())


async def _arango_query(method: str, arg: str) -> str:
    provider = _arango([])
    await getattr(provider, method)(arg)
    return provider.http_client.execute_aql.await_args.args[0]


@pytest.mark.asyncio
async def test_arango_record_query_filters_exactly_what_it_prunes():
    # PRUNE still emits the vertex it stops at; the FILTER must repeat the same
    # step condition, or an edit to one silently changes which chains are walked.
    query = await _arango_query("get_record_path_segments", "f")
    pruned = re.search(r"PRUNE e != null AND NOT \((.*?)\)\s*OPTIONS", query, re.S)
    kept = re.search(r"FILTER e == null OR \((.*?)\)\s*RETURN", query, re.S)
    assert pruned and kept
    assert _squash(pruned.group(1)) == _squash(kept.group(1))
    assert "e.relationshipType IN @relation_types" in pruned.group(1)


@pytest.mark.asyncio
async def test_arango_group_query_filters_exactly_what_it_prunes():
    query = await _arango_query("get_record_group_path", "g")
    pruned = re.search(r"PRUNE NOT (.*?)\s*OPTIONS", query, re.S)
    kept = re.search(r"FILTER (.*?)\s*RETURN", query.split("OPTIONS", 1)[1], re.S)
    assert pruned and kept
    assert _squash(pruned.group(1)) == _squash(kept.group(1))


def _graph_providers() -> list[type]:
    found, pending = [], list(IGraphDBProvider.__subclasses__())
    while pending:
        cls = pending.pop()
        found.append(cls)
        pending.extend(cls.__subclasses__())
    # Only backends from the application: test doubles other test modules build
    # linger in __subclasses__() until they are garbage-collected.
    return [cls for cls in found if not inspect.isabstract(cls) and cls.__module__.startswith("app.")]


@pytest.mark.parametrize("method", ["get_record_path_segments", "get_record_group_path"])
def test_every_provider_accepts_raise_on_error(method):
    # Storage paths and pattern-match scoping always pass it; a backend without
    # it would fail every lookup with a TypeError.
    providers = _graph_providers()
    assert {ArangoHTTPProvider, Neo4jProvider} <= set(providers)
    for cls in providers:
        params = inspect.signature(getattr(cls, method)).parameters
        assert "raise_on_error" in params, f"{cls.__name__}.{method}"
        assert params["raise_on_error"].kind is inspect.Parameter.KEYWORD_ONLY
        assert params["raise_on_error"].default is False


def test_chains_over_parallel_edges_are_enumerated_once():
    from app.services.graph_db.common.utils import ancestor_chain_rows

    # 30 levels, each link given twice (parallel edges): one chain, not 2^30.
    links = [{"child": f"n{i}", "parent": f"n{i - 1}"} for i in range(30, 0, -1)] * 2
    rows = ancestor_chain_rows("n30", links, max_depth=100)
    assert rows == [[f"n{i}" for i in range(30, -1, -1)]]


def test_chain_enumeration_honours_the_depth_bound_and_cycles():
    from app.services.graph_db.common.utils import ancestor_chain_rows

    chain = [{"child": f"n{i}", "parent": f"n{i - 1}"} for i in range(10, 0, -1)]
    assert ancestor_chain_rows("n10", chain, max_depth=3) == [["n10", "n9", "n8", "n7"]]
    cycle = [{"child": "a", "parent": "b"}, {"child": "b", "parent": "a"}]
    assert ancestor_chain_rows("a", cycle, max_depth=50) == [["a", "b"]]
