"""Upload-and-wait pieces shared by the resilience tests.

    RESILIENCE_RECOVERY_TIMEOUT_SEC  how long indexing may take to recover (default 900)
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from helper.clients.kb_client import KBClient

RECOVERY_TIMEOUT = int(os.getenv("RESILIENCE_RECOVERY_TIMEOUT_SEC", "900"))
POLL = 5
UNFINISHED = {"NOT_STARTED", "QUEUED", "IN_PROGRESS"}


def document(token: str) -> bytes:
    # One shape for every document, so each chunks the same way; only the token
    # differs. Long enough that indexing a batch takes a while.
    sections = "\n\n".join(
        f"## Section {n}\n\nOperating note {n} for batch {token}. " + "Routine maintenance detail. " * 12
        for n in range(1, 31)
    )
    return f"# Runbook {token}\n\n{sections}\n".encode()


def record_fields(payload: dict[str, Any]) -> dict[str, Any]:
    return payload.get("record") or payload.get("data", {}).get("record") or payload


def statuses(kb_client: KBClient, record_ids: list[str]) -> dict[str, str]:
    return {
        record_id: record_fields(kb_client.get_record(record_id)).get("indexingStatus", "UNKNOWN")
        for record_id in record_ids
    }


async def wait_until_finished(kb_client: KBClient, record_ids: list[str]) -> dict[str, str]:
    deadline = asyncio.get_event_loop().time() + RECOVERY_TIMEOUT
    current = statuses(kb_client, record_ids)
    while asyncio.get_event_loop().time() < deadline:
        if not UNFINISHED & set(current.values()):
            return current
        await asyncio.sleep(POLL)
        current = statuses(kb_client, record_ids)
    return current
