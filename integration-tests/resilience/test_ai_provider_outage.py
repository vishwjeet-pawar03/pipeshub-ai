"""Indexing tells people when the AI provider is down, and recovers when it is back.

Indexing calls the configured AI provider for every document. This test cuts
the app container off from the provider (its host names point at an address
that refuses connections), restarts the indexing process so no connection
opened earlier survives, and uploads documents to a fresh knowledge base:

  * the product must show a problem on the affected documents — a failed
    status or a reason — rather than leave them looking fine or silently stuck;
  * once the provider is reachable again, every document must end COMPLETED,
    by the product's own retries or by the re-index a person would click.

The reasons shown during the outage are kept for the plain-language check at
the end of this module.

    RESILIENCE_AI_OUTAGE_WINDOW_SEC  how long the product may take to show the problem (default 600)
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

import pytest

from helper.clients.kb_client import KBClient
from helper.compose_control import ComposeStack
from helper.fault_switches import KILL_INDEXING, ai_provider_hosts, block_hosts_script, restore_hosts_script
from helper.indexing_progress import POLL, document, record_fields, wait_until_finished
from helper.plain_message import plain_language_problems

logger = logging.getLogger("resilience")

pytestmark = [pytest.mark.resilience, pytest.mark.asyncio(loop_scope="session")]

FILES = int(os.getenv("RESILIENCE_FILES", "8"))
OUTAGE_WINDOW = int(os.getenv("RESILIENCE_AI_OUTAGE_WINDOW_SEC", "600"))

# Reasons shown while the provider was unreachable, for the plain-language check.
OUTAGE_REASONS: dict[str, str] = {}


def _state(kb_client: KBClient, record_id: str) -> dict[str, str]:
    fields = record_fields(kb_client.get_record(record_id))
    return {
        "indexing": fields.get("indexingStatus") or "UNKNOWN",
        "extraction": fields.get("extractionStatus") or "",
        "reason": (fields.get("reason") or "").strip(),
    }


def _shows_a_problem(state: dict[str, str]) -> bool:
    return state["indexing"] == "FAILED" or state["extraction"] == "FAILED" or bool(state["reason"])


def _cut_off(compose: ComposeStack) -> None:
    compose.exec("pipeshub-ai", ["sh", "-c", block_hosts_script(ai_provider_hosts())])
    # A process that already holds a provider connection could keep using it.
    compose.exec("pipeshub-ai", ["sh", "-c", KILL_INDEXING])


def _reconnect(compose: ComposeStack) -> None:
    compose.exec("pipeshub-ai", ["sh", "-c", restore_hosts_script()])


async def test_indexing_shows_and_recovers_from_an_ai_provider_outage(
    compose: ComposeStack,
    kb_client: KBClient,
    pipeshub_client,
) -> None:
    kb_id = kb_client.create_kb(f"resilience-ai-{uuid.uuid4().hex[:8]}")["id"]
    record_ids: list[str] = []
    try:
        _cut_off(compose)
        try:
            for _ in range(FILES):
                token = uuid.uuid4().hex[:12]
                upload = kb_client.upload_file(
                    kb_id, f"runbook-{token}.md", document(token), mimetype="text/markdown"
                )
                assert upload["summary"]["failed"] == 0, f"upload failed before indexing began: {upload}"
                record_ids.append(upload["records"][0]["recordId"])

            deadline = asyncio.get_event_loop().time() + OUTAGE_WINDOW
            states = {rid: _state(kb_client, rid) for rid in record_ids}
            while asyncio.get_event_loop().time() < deadline:
                if all(_shows_a_problem(s) or s["indexing"] == "COMPLETED" for s in states.values()):
                    break
                await asyncio.sleep(POLL)
                states = {rid: _state(kb_client, rid) for rid in record_ids}

            clean = [rid for rid, s in states.items() if s["indexing"] == "COMPLETED" and not _shows_a_problem(s)]
            affected = {rid: s for rid, s in states.items() if _shows_a_problem(s)}
            assert not clean, (
                f"{len(clean)} document(s) finished cleanly with the AI provider unreachable, so the cut-off "
                f"did not take effect (blocked hosts: {ai_provider_hosts()}): {clean}"
            )
            assert affected, (
                f"with the AI provider unreachable for {OUTAGE_WINDOW}s, none of {len(record_ids)} documents "
                f"showed a problem; they stayed {sorted({s['indexing'] for s in states.values()})}"
            )
            OUTAGE_REASONS.update({rid: s["reason"] for rid, s in affected.items()})
            logger.info("Reasons shown during the AI outage: %s", OUTAGE_REASONS)
        finally:
            _reconnect(compose)

        for rid, state in affected.items():
            if state["indexing"] == "FAILED" or state["extraction"] == "FAILED":
                pipeshub_client.reindex_record(rid)
        final = await wait_until_finished(kb_client, record_ids)
        stuck = {rid: status for rid, status in final.items() if status != "COMPLETED"}
        assert not stuck, (
            f"after the AI provider came back and failed documents were re-indexed, "
            f"{len(stuck)} of {len(record_ids)} did not reach COMPLETED: {stuck}"
        )
    finally:
        try:
            kb_client.delete_kb(kb_id)
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the result
            logger.warning("Could not delete knowledge base %s: %s", kb_id, exc)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "A document hit by an AI provider outage shows the provider library's own error text as its "
        "reason: the indexing handler stores str(exception), e.g. \"Connection error.\" or "
        "\"Transient failure, retry scheduled: Connection error.\", with no mention of the AI model "
        "and no next step."
    ),
)
async def test_ai_outage_reasons_read_plainly() -> None:
    if not OUTAGE_REASONS:
        pytest.skip("the AI outage test did not run or captured no reasons")
    problems = {
        reason: plain_language_problems(reason, about=("AI", "model", "provider"))
        for reason in set(OUTAGE_REASONS.values())
    }
    unclear = {reason: why for reason, why in problems.items() if why}
    assert not unclear, f"reasons shown to users during an AI outage do not read plainly: {unclear}"
