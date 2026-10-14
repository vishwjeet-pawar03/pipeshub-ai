"""Deleting artifacts against a real Neo4j and a real ArangoDB.

The unit tests run on an in-memory fake, so this checks what the fake can only
claim: that the providers' node deletes really take every permission, type and
lineage edge with them in both directions, that the gallery stops returning the
artifact to its owner and to a reader, and that nothing else is touched.

Runs on Neo4j and ArangoDB (backend-matrix). Environment: NEO4J_IT_URI,
NEO4J_IT_PASSWORD, ARANGO_IT_URL, ARANGO_IT_PASSWORD. Storage is the unit
tests' fake: the Node purge route has its own tests.
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

import pytest

from app.config.constants.arangodb import CollectionNames
from app.models.entities import ArtifactType, ArtifactVisibility
from app.services.artifact_registry.access import ArtifactNotFoundError
from app.services.artifact_registry.models import Actor
from app.services.artifact_registry.registry import ArtifactRegistryService
from tests.integration.real_graph import (
    backend_unavailable,
    connect_arango,
    connect_neo4j,
)
from tests.unit.services.artifact_registry.fakes import FakeBlobStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

pytestmark = [pytest.mark.integration, pytest.mark.timeout(300)]

ARANGO_DB = "artifact_delete_it"
RECORDS = CollectionNames.RECORDS.value
ARTIFACTS = CollectionNames.ARTIFACTS.value
USERS = CollectionNames.USERS.value
EDGE_COLLECTIONS = (
    CollectionNames.PERMISSION.value,
    CollectionNames.IS_OF_TYPE.value,
    CollectionNames.RECORD_RELATIONS.value,
)

logger = logging.getLogger("artifact-delete-it")


class _World:
    def __init__(self, graph: IGraphDBProvider, run: str) -> None:
        self.graph = graph
        self.org = f"org-artdel-{run}"
        self.conversation = f"conv-{run}"
        self.owner = Actor(org_id=self.org, user_id=f"owner-{run}")
        self.reader = Actor(org_id=self.org, user_id=f"reader-{run}")
        self.owner_key = f"ukey-owner-{run}"
        self.reader_key = f"ukey-reader-{run}"
        self.registry = ArtifactRegistryService(graph, FakeBlobStore())
        self.artifacts: list[str] = []

    async def create(self, name: str, **kwargs) -> str:
        metadata = await self.registry.register(
            actor=self.owner, name=name, artifact_type=kwargs.pop("artifact_type", ArtifactType.IMAGE),
            mime_type="image/png", content=name.encode(),
            conversation_id=kwargs.pop("conversation_id", self.conversation), **kwargs,
        )
        self.artifacts.append(metadata.artifact_id)
        return metadata.artifact_id

    async def share(self, artifact_id: str) -> None:
        assert await self.graph.batch_create_edges([{
            "from_id": self.reader_key, "from_collection": USERS, "to_id": artifact_id,
            "to_collection": RECORDS, "type": "USER", "role": "READER",
        }], CollectionNames.PERMISSION.value)

    async def edges_touching(self, artifact_id: str) -> list[tuple[str, str, str]]:
        """(collection, from, to) of every edge at either node. The isOfType
        edge joins the two, so it is seen from both ends, and Neo4j returns it
        as `_from`/`_to` from one end and `from_id`/`from_collection` from the
        other: keying by endpoints counts it once."""
        def end(edge: dict, side: str) -> str:
            return edge.get(f"_{side}") or f"{edge.get(f'{side}_collection')}/{edge.get(f'{side}_id')}"

        found: set[tuple[str, str, str]] = set()
        for node in (f"{RECORDS}/{artifact_id}", f"{ARTIFACTS}/{artifact_id}"):
            for collection in EDGE_COLLECTIONS:
                for edge in [
                    *await self.graph.get_edges_from_node(node, collection),
                    *await self.graph.get_edges_to_node(node, collection),
                ]:
                    found.add((collection, end(edge, "from"), end(edge, "to")))
        return sorted(found)

    async def gone(self, artifact_id: str) -> bool:
        return (
            await self.graph.get_document(artifact_id, RECORDS) is None
            and await self.graph.get_document(artifact_id, ARTIFACTS) is None
            and await self.edges_touching(artifact_id) == []
        )

    async def gallery_ids(self, user_key: str) -> set[str]:
        rows, _ = await self.graph.list_accessible_artifacts(
            user_id=user_key, org_id=self.org, skip=0, limit=100, search=None, artifact_types=None,
            conversation_id=None, date_from=None, date_to=None, sort_by="createdAtTimestamp", sort_order="desc",
        )
        return {row["id"] for row in rows}


@pytest.fixture(params=["neo4j", "arango"])
async def world(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_World]:
    async with contextlib.AsyncExitStack() as cleanup:
        try:
            graph = await (
                connect_neo4j(logger, monkeypatch) if request.param == "neo4j"
                else connect_arango(logger, ARANGO_DB)
            )
        except Exception as exc:
            backend_unavailable(request.param, exc)
        disconnect = getattr(graph, "disconnect", None)
        if disconnect is not None:
            cleanup.push_async_callback(disconnect)

        w = _World(graph, uuid.uuid4().hex[:10])

        async def _remove() -> None:
            with contextlib.suppress(Exception):
                await graph.delete_nodes_and_edges(w.artifacts, RECORDS)
                await graph.delete_nodes(w.artifacts, ARTIFACTS)
                await graph.delete_nodes_and_edges([w.owner_key, w.reader_key], USERS)

        cleanup.push_async_callback(_remove)
        assert await graph.batch_upsert_nodes([
            {"_key": key, "userId": actor.user_id, "orgId": w.org, "email": f"{key}@example.com", "isActive": True}
            for key, actor in ((w.owner_key, w.owner), (w.reader_key, w.reader))
        ], USERS)
        yield w


@pytest.mark.asyncio
async def test_delete_takes_every_edge_and_leaves_the_rest(world: _World) -> None:
    code = await world.create("make_chart.py", artifact_type=ArtifactType.CODE)
    output = await world.create("chart.png")
    kept = await world.create("kept.png")
    await world.registry.add_version(actor=world.owner, artifact_id=output, content=b"v2")
    await world.registry.record_derivation(
        output_artifact_id=output, code_artifact_id=code, code_version=1, output_version=2,
    )
    await world.share(output)
    assert output in await world.gallery_ids(world.reader_key)

    await world.registry.delete(actor=world.owner, artifact_id=output)

    assert await world.gone(output)
    assert output not in await world.gallery_ids(world.owner_key)
    assert output not in await world.gallery_ids(world.reader_key)
    assert await world.registry.lineage.get_outputs_for_code(code) == []
    for survivor in (code, kept):
        assert await world.graph.get_document(survivor, RECORDS) is not None
        assert await world.graph.get_document(survivor, ARTIFACTS) is not None
        edges = await world.edges_touching(survivor)
        assert [e[0] for e in edges] == [CollectionNames.IS_OF_TYPE.value, CollectionNames.PERMISSION.value], edges
    assert {code, kept} <= await world.gallery_ids(world.owner_key)

    with pytest.raises(ArtifactNotFoundError):
        await world.registry.delete(actor=world.owner, artifact_id=output)


@pytest.mark.asyncio
async def test_deleting_the_code_keeps_the_output(world: _World) -> None:
    code = await world.create("make_chart.py", artifact_type=ArtifactType.CODE)
    output = await world.create("chart.png")
    await world.registry.record_derivation(
        output_artifact_id=output, code_artifact_id=code, code_version=1, output_version=1,
    )

    await world.registry.delete(actor=world.owner, artifact_id=code)

    assert await world.gone(code)
    assert await world.graph.get_document(output, RECORDS) is not None
    assert await world.registry.lineage.get_lineage_for_output(output) is None


@pytest.mark.asyncio
async def test_conversation_delete_leaves_no_artifact_of_that_chat(world: _World) -> None:
    in_chat = [
        await world.create("a.png"),
        await world.create("b.py", visibility=ArtifactVisibility.STAGING),
        await world.create("c.pdf", is_temporary=True),
        await world.create("d.json", artifact_type=ArtifactType.TOOL_RESULT),
    ]
    elsewhere = await world.create("e.png", conversation_id=f"{world.conversation}-other")

    deleted = await world.registry.delete_for_conversation(actor=world.owner, conversation_id=world.conversation)

    assert deleted == len(in_chat)
    for artifact_id in in_chat:
        assert await world.gone(artifact_id)
    assert await world.graph.get_documents_paginated(
        ARTIFACTS, skip=0, limit=10, filters={"orgId": world.org, "conversationId": world.conversation},
    ) == []
    assert await world.graph.get_document(elsewhere, RECORDS) is not None
    assert await world.registry.delete_for_conversation(
        actor=world.owner, conversation_id=world.conversation,
    ) == 0
