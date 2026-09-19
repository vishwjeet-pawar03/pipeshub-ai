"""Indexing survives its dependencies going away mid-run.

Each test uploads a batch of documents to a fresh knowledge base and, while
they are being indexed, takes one dependency away: the message broker (Redis)
is restarted, the vector database (Qdrant) is restarted, or the indexing
process is killed outright. Then every document must still reach COMPLETED,
exactly once:

  * the knowledge base lists each upload once, and nothing else;
  * each document has vectors, and the same number as its siblings. The
    documents share one shape, so a document indexed twice over shows up as
    a count the others do not have.

A document left QUEUED, IN_PROGRESS or FAILED after the recovery window is a
real failure: the product lost work when a dependency blinked.

    RESILIENCE_FILES                 documents per test (default 8)
    RESILIENCE_RECOVERY_TIMEOUT_SEC  how long indexing may take to recover (default 900)
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from helper.clients.kb_client import KBClient
from helper.compose_control import ComposeStack

logger = logging.getLogger("resilience")

FILES = int(os.getenv("RESILIENCE_FILES", "8"))
RECOVERY_TIMEOUT = int(os.getenv("RESILIENCE_RECOVERY_TIMEOUT_SEC", "900"))
POLL = 5
UNFINISHED = {"NOT_STARTED", "QUEUED", "IN_PROGRESS"}

# Matches the indexing service's own command line, not this shell's: the
# bracket keeps the pattern from matching the text of the pattern.
KILL_INDEXING = (
    "for p in /proc/[0-9]*; do "
    "if tr '\\0' ' ' < $p/cmdline 2>/dev/null | grep -q '^python[0-9.]* -m [a]pp[.]indexing_main'; "
    "then kill -9 ${p#/proc/} && echo ${p#/proc/}; fi; done"
)

pytestmark = [pytest.mark.resilience, pytest.mark.asyncio(loop_scope="session")]


def _restart(service: str) -> Callable[[ComposeStack], None]:
    def inject(compose: ComposeStack) -> None:
        compose.restart(service)
        compose.wait_ready(service)

    return inject


def _kill_indexing(compose: ComposeStack) -> None:
    killed = compose.exec("pipeshub-ai", ["sh", "-c", KILL_INDEXING]).stdout.split()
    assert killed, "no indexing process was running in the pipeshub-ai container to kill"
    logger.info("Killed indexing process(es) %s; the container's monitor restarts it", killed)


@dataclass(frozen=True)
class Outage:
    name: str
    inject: Callable[[ComposeStack], None]


OUTAGES = [
    Outage("broker restart", _restart("redis")),
    Outage("vector database restart", _restart("qdrant")),
    Outage("indexing process killed", _kill_indexing),
]


def _document(token: str) -> bytes:
    # One shape for every document, so each chunks the same way; only the token differs.
    sections = "\n\n".join(
        f"## Section {n}\n\nOperating note {n} for batch {token}. " + "Routine maintenance detail. " * 12
        for n in range(1, 6)
    )
    return f"# Runbook {token}\n\n{sections}\n".encode()


def _record_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("record") or payload.get("data", {}).get("record") or payload


def _statuses(kb_client: KBClient, record_ids: list[str]) -> dict[str, str]:
    return {
        record_id: _record_fields(kb_client.get_record(record_id)).get("indexingStatus", "UNKNOWN")
        for record_id in record_ids
    }


async def _wait_until_finished(kb_client: KBClient, record_ids: list[str]) -> dict[str, str]:
    deadline = asyncio.get_event_loop().time() + RECOVERY_TIMEOUT
    statuses = _statuses(kb_client, record_ids)
    while asyncio.get_event_loop().time() < deadline:
        if not UNFINISHED & set(statuses.values()):
            return statuses
        await asyncio.sleep(POLL)
        statuses = _statuses(kb_client, record_ids)
    return statuses


def _listed_names(kb_client: KBClient, kb_id: str) -> list[str]:
    payload = kb_client.list_records(kb_id, limit=100)
    records = payload.get("records") or payload.get("data", {}).get("records") or []
    return [r["recordName"] for r in records]


@pytest.mark.parametrize("outage", OUTAGES, ids=[o.name for o in OUTAGES])
async def test_indexing_recovers_from_outage_without_losing_or_duplicating(
    outage: Outage,
    compose: ComposeStack,
    kb_client: KBClient,
    vector_store,
) -> None:
    kb_id = kb_client.create_kb(f"resilience-{uuid.uuid4().hex[:8]}")["id"]
    try:
        names: list[str] = []
        record_ids: list[str] = []
        for _ in range(FILES):
            token = uuid.uuid4().hex[:12]
            name = f"runbook-{token}.md"
            upload = kb_client.upload_file(kb_id, name, _document(token), mimetype="text/markdown")
            assert upload["summary"]["failed"] == 0, f"upload of {name} failed before any fault: {upload}"
            names.append(name)
            record_ids.append(upload["records"][0]["recordId"])

        at_fault = _statuses(kb_client, record_ids)
        if not UNFINISHED & set(at_fault.values()):
            pytest.skip(
                f"all {FILES} documents were indexed before the {outage.name} could land; "
                "raise RESILIENCE_FILES so the fault meets work in flight"
            )
        logger.info("Injecting %s with indexing at %s", outage.name, sorted(at_fault.values()))
        outage.inject(compose)

        final = await _wait_until_finished(kb_client, record_ids)
        stuck = {rid: status for rid, status in final.items() if status != "COMPLETED"}
        assert not stuck, (
            f"after a {outage.name}, {len(stuck)} of {FILES} documents did not reach COMPLETED "
            f"within {RECOVERY_TIMEOUT}s: {stuck}"
        )

        listed = _listed_names(kb_client, kb_id)
        assert sorted(listed) == sorted(names), (
            f"after a {outage.name}, the knowledge base lists {sorted(listed)}; "
            f"expected each of the {FILES} uploads exactly once"
        )

        counts = {}
        for record_id in record_ids:
            virtual_id = _record_fields(kb_client.get_record(record_id)).get("virtualRecordId")
            assert virtual_id, f"record {record_id} is COMPLETED but has no virtual record id"
            counts[record_id] = await vector_store.count_for_virtual_record(str(virtual_id))
        assert all(counts.values()), f"after a {outage.name}, some COMPLETED documents have no vectors: {counts}"
        assert len(set(counts.values())) == 1, (
            f"after a {outage.name}, documents of one shape hold different vector counts {counts}; "
            "a larger count is a document indexed twice over"
        )
    finally:
        try:
            kb_client.delete_kb(kb_id)
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the result
            logger.warning("Could not delete knowledge base %s: %s", kb_id, exc)
