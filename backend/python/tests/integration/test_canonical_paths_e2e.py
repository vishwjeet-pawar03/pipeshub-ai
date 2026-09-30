"""Canonical storage paths and group scoping against a real Neo4j and a real ArangoDB.

Blob storage paths are built from ``get_record_path_segments`` and
``get_record_group_path``, so both backends must return the same single chain
for the same graph. Every case below runs the same assertions on both. The
graph shapes are the ones that made the backends disagree before: a second
non-canonical parent, a non-canonical link on the way up, duplicate edges,
equal-length ties and cycles.

The group-scoping cases check that pattern match narrows grep to every record
group of a connector the user reaches, including groups that inherit from a
grant on another connector.

Needs Docker services, and skips cleanly when they are not reachable:

  docker compose -f deployment/docker-compose/docker-compose.integration.graph-db.yml \
    up -d --wait neo4j-graph-it arango-graph-it
  cd backend/python && pytest tests/integration/test_canonical_paths_e2e.py -m integration

Environment: NEO4J_IT_URI, NEO4J_IT_PASSWORD, ARANGO_IT_URL, ARANGO_IT_PASSWORD.
"""

import asyncio
import contextlib
import logging
import os
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.config.constants.arangodb import CollectionNames, PermissionModel, ProgressStatus
from app.services.graph_db.arango.arango_http_provider import ArangoHTTPProvider
from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider
from app.services.graph_db.neo4j.neo4j_provider import Neo4jProvider
from app.utils.pattern_match import _accessible_groups_for_connector, _fetch_container_group_rows
from app.utils.time_conversion import get_epoch_timestamp_in_ms

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]

NEO4J_URI = os.environ.get("NEO4J_IT_URI", "bolt://localhost:17687")
NEO4J_PASSWORD = os.environ.get("NEO4J_IT_PASSWORD", "ensure-it-pass")
ARANGO_URL = os.environ.get("ARANGO_IT_URL", "http://localhost:18529")
ARANGO_PASSWORD = os.environ.get("ARANGO_IT_PASSWORD", "ensure-it-pass")
ARANGO_DB = "canonical_paths_it"

RECORDS = CollectionNames.RECORDS.value
GROUPS = CollectionNames.RECORD_GROUPS.value
APPS = CollectionNames.APPS.value
USERS = CollectionNames.USERS.value
EDGE_COLLECTIONS = (
    CollectionNames.RECORD_RELATIONS.value,
    CollectionNames.BELONGS_TO.value,
    CollectionNames.PERMISSION.value,
    CollectionNames.INHERIT_PERMISSIONS.value,
    CollectionNames.USER_APP_RELATION.value,
)

logger = logging.getLogger("canonical-paths-it")


@dataclass
class _Env:
    graph: IGraphDBProvider
    suffix: str
    org_id: str
    connector_id: str
    created: list[tuple[str, str]] = field(default_factory=list)

    def key(self, name: str) -> str:
        # Shared prefix, so tie-breaks on id order follow the names the tests pick.
        return f"{self.suffix}-{name}"

    async def _node(self, collection: str, doc: dict) -> str:
        await self.graph.batch_upsert_nodes([doc], collection=collection)
        self.created.append((collection, doc["id"]))
        return doc["id"]

    async def record(
        self,
        name: str,
        *,
        parent: str | None = None,
        parent_by_id: bool = False,
        external_of: str | None = None,
    ) -> str:
        """A record named *name*; *parent* names the record its externalParentId points at.

        *external_of* reuses another record's externalRecordId: the graph anomaly
        that gives a child two canonical parents.
        """
        now = get_epoch_timestamp_in_ms()
        doc = {
            "id": self.key(name),
            "orgId": self.org_id,
            "recordName": name,
            "externalRecordId": f"ext-{self.key(external_of or name)}",
            "recordType": "TICKET",
            "origin": "CONNECTOR",
            "connectorName": "JIRA",
            "connectorId": self.connector_id,
            "version": 0,
            "virtualRecordId": f"vr-{self.key(name)}",
            "indexingStatus": ProgressStatus.COMPLETED.value,
            "createdAtTimestamp": now,
            "updatedAtTimestamp": now,
        }
        if parent is not None:
            doc["externalParentId"] = self.key(parent) if parent_by_id else f"ext-{self.key(parent)}"
        return await self._node(RECORDS, doc)

    async def group(
        self, name: str, *, connector_id: str | None = None, permission_model: str | None = None,
    ) -> str:
        now = get_epoch_timestamp_in_ms()
        doc = {
            "id": self.key(name),
            "orgId": self.org_id,
            "groupName": name,
            "externalGroupId": f"ext-{self.key(name)}",
            "groupType": "PROJECT",
            "connectorName": "JIRA",
            "connectorId": connector_id or self.connector_id,
            "createdAtTimestamp": now,
            "updatedAtTimestamp": now,
        }
        if permission_model is not None:
            doc["permissionModel"] = permission_model
        return await self._node(GROUPS, doc)

    async def app(self, name: str, *, app_type: str = "Jira") -> str:
        now = get_epoch_timestamp_in_ms()
        return await self._node(APPS, {
            "id": self.key(name),
            "name": name,
            "type": app_type,
            "appGroup": "Atlassian",
            "scope": "team",
            "isActive": True,
            "orgId": self.org_id,
            # Un-backfilled apps make the container query fall back.
            "vectorMembershipBackfilled": True,
            "createdAtTimestamp": now,
            "updatedAtTimestamp": now,
        })

    async def user(self, name: str) -> str:
        now = get_epoch_timestamp_in_ms()
        user_id = self.key(name)
        return await self._node(USERS, {
            "id": user_id,
            "userId": user_id,
            "orgId": self.org_id,
            "email": f"{user_id}@example.com",
            "isActive": True,
            "createdAtTimestamp": now,
            "updatedAtTimestamp": now,
        })

    async def edge(
        self, collection: str, from_coll: str, from_id: str, to_coll: str, to_id: str,
        transaction: str | None = None, **props: object,
    ) -> None:
        now = get_epoch_timestamp_in_ms()
        await self.graph.batch_create_edges(
            [{
                "from_id": from_id, "from_collection": from_coll,
                "to_id": to_id, "to_collection": to_coll,
                "createdAtTimestamp": now, "updatedAtTimestamp": now,
                **props,
            }],
            collection=collection,
            transaction=transaction,
        )

    async def child_of(self, parent: str, child: str, relation: str = "PARENT_CHILD", **kw: object) -> None:
        await self.edge(
            CollectionNames.RECORD_RELATIONS.value, RECORDS, parent, RECORDS, child,
            relationshipType=relation, **kw,
        )

    async def duplicate_child_of(self, parent: str, child: str) -> None:
        """A second identical parent edge. batch_create_edges dedupes, so write it raw."""
        now = get_epoch_timestamp_in_ms()
        if isinstance(self.graph, Neo4jProvider):
            await self.graph.client.execute_query(
                "MATCH (p:Record {id: $p}), (c:Record {id: $c}) "
                "CREATE (p)-[:RECORD_RELATION {relationshipType: 'PARENT_CHILD', createdAtTimestamp: $t}]->(c)",
                parameters={"p": parent, "c": child, "t": now},
            )
            return
        await self.graph.http_client.execute_aql(
            f"INSERT {{_from: CONCAT('{RECORDS}/', @p), _to: CONCAT('{RECORDS}/', @c), "
            f"relationshipType: 'PARENT_CHILD', createdAtTimestamp: @t}} "
            f"INTO {CollectionNames.RECORD_RELATIONS.value}",
            {"p": parent, "c": child, "t": now},
        )

    async def belongs_to(self, child_group: str, parent: str, parent_coll: str = GROUPS) -> None:
        await self.edge(CollectionNames.BELONGS_TO.value, GROUPS, child_group, parent_coll, parent)


async def _connect_neo4j(monkeypatch: pytest.MonkeyPatch) -> IGraphDBProvider:
    monkeypatch.setenv("NEO4J_URI", NEO4J_URI)
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", NEO4J_PASSWORD)
    monkeypatch.setenv("NEO4J_DATABASE", "neo4j")
    provider = Neo4jProvider(logger, MagicMock())
    if not await asyncio.wait_for(provider.connect(), timeout=60):
        raise ConnectionError("Neo4jProvider.connect returned False")
    return provider


async def _connect_arango() -> IGraphDBProvider:
    config_service = MagicMock()
    config_service.get_config = AsyncMock(
        return_value={"url": ARANGO_URL, "username": "root", "password": ARANGO_PASSWORD, "db": ARANGO_DB}
    )
    provider = ArangoHTTPProvider(logger, config_service)
    if not await asyncio.wait_for(provider.connect(), timeout=60):
        raise ConnectionError("ArangoHTTPProvider.connect returned False")
    # Strict schemas: a seeded document the production writer could not store is rejected.
    await provider.ensure_schema()
    return provider


async def _remove_test_data(env: _Env) -> None:
    ids = sorted({node_id for _collection, node_id in env.created})
    if not ids:
        return
    if isinstance(env.graph, Neo4jProvider):
        await env.graph.client.execute_query(
            "MATCH (n) WHERE n.id IN $ids DETACH DELETE n", parameters={"ids": ids},
        )
        return
    handles = sorted({f"{collection}/{node_id}" for collection, node_id in env.created})
    for edges in EDGE_COLLECTIONS:
        await env.graph.http_client.execute_aql(
            f"FOR e IN {edges} FILTER e._from IN @h OR e._to IN @h REMOVE e IN {edges}",
            {"h": handles},
        )
    for collection in {collection for collection, _node_id in env.created}:
        await env.graph.http_client.execute_aql(
            f"FOR d IN {collection} FILTER d._key IN @ids REMOVE d IN {collection}",
            {"ids": ids},
        )


# A backend that failed to connect once is skipped at once for the rest of the
# run, instead of every test waiting out the connect timeout again.
_unavailable: dict[str, str] = {}


@pytest.fixture(params=["neo4j", "arango"])
async def env(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Env]:
    if request.param in _unavailable:
        pytest.skip(_unavailable[request.param])
    async with contextlib.AsyncExitStack() as cleanup:
        try:
            graph = await (_connect_neo4j(monkeypatch) if request.param == "neo4j" else _connect_arango())
        except Exception as exc:
            _unavailable[request.param] = f"{request.param} not available: {exc}"
            pytest.skip(_unavailable[request.param])
        disconnect = getattr(graph, "disconnect", None)
        if disconnect is not None:
            cleanup.push_async_callback(disconnect)

        suffix = uuid.uuid4().hex[:10]
        environment = _Env(
            graph=graph, suffix=suffix, org_id=f"org-cp-{suffix}", connector_id=f"conn-cp-{suffix}",
        )
        cleanup.push_async_callback(_remove_test_data, environment)
        yield environment


# ---------------------------------------------------------------------------
# get_record_path_segments
# ---------------------------------------------------------------------------


async def test_record_chain_is_returned_root_first(env: _Env) -> None:
    root = await env.record("Root")
    docs = await env.record("Docs", parent="Root")
    leaf = await env.record("f.txt", parent="Docs")
    await env.child_of(root, docs)
    await env.child_of(docs, leaf)

    assert await env.graph.get_record_path_segments(leaf) == ["Root", "Docs", "f.txt"]
    assert await env.graph.get_record_path_segments(root) == ["Root"]


async def test_second_parent_that_is_not_canonical_is_not_followed(env: _Env) -> None:
    """A Drive file in two folders: only the folder its externalParentId names counts."""
    r1, r2 = await env.record("R1"), await env.record("R2")
    d1 = await env.record("D1", parent="R1")
    d2 = await env.record("D2", parent="R2")
    leaf = await env.record("f.txt", parent="D1")
    await env.child_of(r1, d1)
    await env.child_of(r2, d2)
    await env.child_of(d1, leaf)
    await env.child_of(d2, leaf)

    assert await env.graph.get_record_path_segments(leaf) == ["R1", "D1", "f.txt"]


async def test_walk_stops_at_a_non_canonical_relation(env: _Env) -> None:
    """Nothing reached through a link edge may enter the path, even if its own parent edge is canonical."""
    folder = await env.record("Folder")
    leaf = await env.record("f.txt", parent="Folder")
    project = await env.record("Project")
    issue = await env.record("Issue", parent="Project")
    await env.child_of(folder, leaf)
    await env.child_of(project, issue)
    await env.child_of(issue, leaf, relation="LINKED_TO")

    assert await env.graph.get_record_path_segments(leaf) == ["Folder", "f.txt"]


async def test_attachment_edges_and_parent_matched_by_id(env: _Env) -> None:
    mail = await env.record("Mail")
    attachment = await env.record("invoice.pdf", parent="Mail")
    page = await env.record("Page")
    child = await env.record("Child", parent="Page", parent_by_id=True)
    await env.child_of(mail, attachment, relation="ATTACHMENT")
    await env.child_of(page, child)

    assert await env.graph.get_record_path_segments(attachment) == ["Mail", "invoice.pdf"]
    assert await env.graph.get_record_path_segments(child) == ["Page", "Child"]


async def test_longest_of_two_canonical_chains_wins(env: _Env) -> None:
    top = await env.record("Top")
    short_parent = await env.record("A")
    long_parent = await env.record("B", external_of="A", parent="Top")
    leaf = await env.record("f.txt", parent="A")
    await env.child_of(short_parent, leaf)
    await env.child_of(long_parent, leaf)
    await env.child_of(top, long_parent)

    assert await env.graph.get_record_path_segments(leaf) == ["Top", "B", "f.txt"]


async def test_equal_length_chains_pick_the_smallest_ids_on_both_backends(env: _Env) -> None:
    p1 = await env.record("p1")
    p2 = await env.record("p2", external_of="p1")
    leaf = await env.record("f.txt", parent="p1")
    await env.child_of(p2, leaf)
    await env.child_of(p1, leaf)

    assert await env.graph.get_record_path_segments(leaf) == ["p1", "f.txt"]


async def test_duplicate_parent_edges_count_once(env: _Env) -> None:
    docs = await env.record("Docs")
    leaf = await env.record("f.txt", parent="Docs")
    await env.child_of(docs, leaf)
    await env.duplicate_child_of(docs, leaf)

    assert await env.graph.get_record_path_segments(leaf) == ["Docs", "f.txt"]


async def test_cycle_terminates_without_repeating_a_record(env: _Env) -> None:
    a = await env.record("A", parent="B")
    b = await env.record("B", parent="A")
    leaf = await env.record("f.txt", parent="A")
    await env.child_of(b, a)
    await env.child_of(a, b)
    await env.child_of(a, leaf)

    assert await env.graph.get_record_path_segments(leaf) == ["B", "A", "f.txt"]


async def test_missing_record_has_no_path(env: _Env) -> None:
    missing = env.key("missing")
    assert await env.graph.get_record_path_segments(missing) == []
    assert await env.graph.get_record_path_segments(missing, raise_on_error=True) == []


async def test_record_path_sees_uncommitted_edges_inside_a_transaction(env: _Env) -> None:
    """Old paths are snapshotted inside the sync transaction, before commit.

    Nothing is asserted after the rollback: by default the Neo4j client runs
    each statement of a "transaction" as its own auto-commit, so the edge stays.
    """
    docs = await env.record("Docs")
    leaf = await env.record("f.txt", parent="Docs")
    txn = await env.graph.begin_transaction(
        read=[RECORDS, CollectionNames.RECORD_RELATIONS.value],
        write=[RECORDS, CollectionNames.RECORD_RELATIONS.value],
    )
    try:
        await env.child_of(docs, leaf, transaction=txn)
        assert await env.graph.get_record_path_segments(leaf, transaction=txn) == ["Docs", "f.txt"]
    finally:
        await env.graph.rollback_transaction(txn)


async def test_record_path_query_failure_raises_only_when_asked(env: _Env) -> None:
    leaf = await env.record("f.txt")
    assert await env.graph.get_record_path_segments(leaf, transaction="no-such-transaction") == []
    with pytest.raises(Exception):
        await env.graph.get_record_path_segments(
            leaf, transaction="no-such-transaction", raise_on_error=True,
        )


# ---------------------------------------------------------------------------
# get_record_group_path
# ---------------------------------------------------------------------------


async def test_group_chain_is_returned_root_first(env: _Env) -> None:
    root = await env.group("Root")
    parent = await env.group("Parent")
    leaf = await env.group("Leaf")
    await env.belongs_to(leaf, parent)
    await env.belongs_to(parent, root)

    assert await env.graph.get_record_group_path(leaf) == ["Root", "Parent", "Leaf"]
    assert await env.graph.get_record_group_path(root) == ["Root"]


async def test_group_with_two_parents_takes_the_longest_chain(env: _Env) -> None:
    top = await env.group("Top")
    long_parent = await env.group("P1")
    short_parent = await env.group("P2")
    leaf = await env.group("Leaf")
    await env.belongs_to(leaf, long_parent)
    await env.belongs_to(leaf, short_parent)
    await env.belongs_to(long_parent, top)

    assert await env.graph.get_record_group_path(leaf) == ["Top", "P1", "Leaf"]


async def test_group_parents_of_equal_depth_pick_the_smallest_ids(env: _Env) -> None:
    pa = await env.group("pa")
    pb = await env.group("pb")
    leaf = await env.group("Leaf")
    await env.belongs_to(leaf, pb)
    await env.belongs_to(leaf, pa)

    assert await env.graph.get_record_group_path(leaf) == ["pa", "Leaf"]


async def test_group_chain_stops_at_a_non_group_parent(env: _Env) -> None:
    app = await env.app("App")
    parent = await env.group("Parent")
    leaf = await env.group("Leaf")
    await env.belongs_to(leaf, parent)
    await env.belongs_to(parent, app, parent_coll=APPS)

    assert await env.graph.get_record_group_path(leaf) == ["Parent", "Leaf"]


async def test_group_cycle_terminates(env: _Env) -> None:
    a = await env.group("A")
    b = await env.group("B")
    await env.belongs_to(a, b)
    await env.belongs_to(b, a)

    assert await env.graph.get_record_group_path(a) == ["B", "A"]


async def test_missing_group_has_no_path(env: _Env) -> None:
    missing = env.key("missing")
    assert await env.graph.get_record_group_path(missing) == []
    assert await env.graph.get_record_group_path(missing, raise_on_error=True) == []


async def test_group_path_query_failure_raises_only_when_asked(env: _Env) -> None:
    leaf = await env.group("Leaf")
    assert await env.graph.get_record_group_path(leaf, transaction="no-such-transaction") == []
    with pytest.raises(Exception):
        await env.graph.get_record_group_path(
            leaf, transaction="no-such-transaction", raise_on_error=True,
        )


# ---------------------------------------------------------------------------
# Pattern-match group scoping from real containers
# ---------------------------------------------------------------------------


async def _grant(env: _Env, user: str, group: str) -> None:
    await env.edge(
        CollectionNames.PERMISSION.value, USERS, user, GROUPS, group, type="USER", role="READER",
    )


async def _inherits(env: _Env, child: str, parent: str) -> None:
    await env.edge(CollectionNames.INHERIT_PERMISSIONS.value, GROUPS, child, GROUPS, parent)


async def _uses_app(env: _Env, user: str, app: str) -> None:
    now = get_epoch_timestamp_in_ms()
    await env.edge(
        CollectionNames.USER_APP_RELATION.value, USERS, user, APPS, app,
        syncState="COMPLETED", lastSyncUpdate=now,
    )


async def _groups_for(env: _Env, containers: object, connector_id: str) -> list[str]:
    rows = await _fetch_container_group_rows(env.graph, containers)
    groups = _accessible_groups_for_connector(
        containers, rows, org_id=env.org_id, connector_id=connector_id,
    )
    return [g["id"] for g in groups]


async def test_scoping_reaches_every_group_the_user_can_read_including_cross_connector_inheritance(
    env: _Env,
) -> None:
    trusted = PermissionModel.RECORD_GROUP_LEVEL.value
    user = await env.user("user")
    jira = await env.app("jira")
    other = await env.app("other")
    await _uses_app(env, user, jira)
    await _uses_app(env, user, other)

    granted = await env.group("granted", connector_id=jira, permission_model=trusted)
    inherited = await env.group("inherited", connector_id=jira, permission_model=trusted)
    cross = await env.group("cross", connector_id=jira, permission_model=trusted)
    hidden = await env.group("hidden", connector_id=jira, permission_model=trusted)
    other_granted = await env.group("other-granted", connector_id=other, permission_model=trusted)
    await _grant(env, user, granted)
    await _grant(env, user, other_granted)
    await _inherits(env, inherited, granted)
    # A Jira group inheriting from a grant on another connector.
    await _inherits(env, cross, other_granted)

    containers = await env.graph.get_accessible_containers(user_id=user, org_id=env.org_id)

    assert containers.fallback_reason is None
    assert await _groups_for(env, containers, jira) == sorted([granted, inherited, cross])
    assert hidden not in await _groups_for(env, containers, jira)
    assert await _groups_for(env, containers, other) == [other_granted]


async def test_scoping_is_off_for_a_connector_the_user_cannot_reach(env: _Env) -> None:
    user = await env.user("user")
    jira = await env.app("jira")
    granted = await env.group("granted", connector_id=jira, permission_model=PermissionModel.RECORD_GROUP_LEVEL.value)
    await _grant(env, user, granted)

    containers = await env.graph.get_accessible_containers(user_id=user, org_id=env.org_id)

    assert await _groups_for(env, containers, jira) == []


async def test_scoping_is_off_for_root_scoped_connectors(env: _Env) -> None:
    user = await env.user("user")
    slack = await env.app("slack", app_type="Slack")
    await _uses_app(env, user, slack)
    channel = await env.group("channel", connector_id=slack, permission_model=PermissionModel.RECORD_GROUP_LEVEL.value)
    await _grant(env, user, channel)

    containers = await env.graph.get_accessible_containers(user_id=user, org_id=env.org_id)

    assert channel in containers.root_group_ids
    assert await _groups_for(env, containers, slack) == []
