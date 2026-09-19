"""Uploads made while MongoDB restarts either land whole or fail cleanly.

MongoDB holds each upload's storage document, so it is on the path of every
upload. This test keeps uploading to a fresh knowledge base while MongoDB
restarts, then checks each upload's outcome against what the product holds:

  * an upload reported as saved is listed once and reaches COMPLETED;
  * an upload reported as failed left nothing behind in the knowledge base,
    and uploading it again succeeds;
  * no upload hangs past the client's timeout.

Uploads that happen to succeed throughout still count: what matters is that
none was half-created. The test fails if no upload ran while MongoDB was down.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid

import pytest
import requests

from helper.clients.kb_client import KBClient
from helper.compose_control import ComposeStack
from helper.indexing_progress import document, wait_until_finished
from pipeshub_client import PipeshubClientError

logger = logging.getLogger("resilience")

pytestmark = [pytest.mark.resilience, pytest.mark.asyncio(loop_scope="session")]

AFTER_RESTART_UPLOADS = 2


def _upload(kb_client: KBClient, kb_id: str, name: str) -> tuple[str, str | None, str]:
    """("saved", record id, "") or ("failed", None, why). A hang raises."""
    token = name.split("-", 1)[1].split(".", 1)[0]
    try:
        result = kb_client.upload_file(kb_id, name, document(token), mimetype="text/markdown")
    except (PipeshubClientError, requests.HTTPError) as exc:
        return "failed", None, str(exc)
    if result["summary"]["failed"] or not result["records"]:
        return "failed", None, str(result.get("failed"))
    return "saved", result["records"][0]["recordId"], ""


async def test_uploads_during_a_database_restart_land_whole_or_fail_cleanly(
    compose: ComposeStack,
    kb_client: KBClient,
) -> None:
    kb_id = kb_client.create_kb(f"resilience-mongo-{uuid.uuid4().hex[:8]}")["id"]
    restart_error: list[BaseException] = []

    def restart() -> None:
        try:
            compose.restart("mongodb")
            compose.wait_ready("mongodb")
        except BaseException as exc:  # noqa: BLE001 - reported on the main thread
            restart_error.append(exc)

    try:
        outcomes: dict[str, tuple[str, str | None, str]] = {}
        restarting = threading.Thread(target=restart, name="mongodb-restart")
        restarting.start()
        during = 0
        while restarting.is_alive():
            name = f"upload-{uuid.uuid4().hex[:12]}.md"
            try:
                outcomes[name] = _upload(kb_client, kb_id, name)
            except requests.Timeout as exc:
                pytest.fail(f"an upload during the MongoDB restart hung until the client gave up: {exc}")
            during += 1
            time.sleep(0.5)
        restarting.join()
        assert not restart_error, f"MongoDB did not come back: {restart_error[0]}"
        assert during, "MongoDB restarted before a single upload was made; nothing was tested"
        for _ in range(AFTER_RESTART_UPLOADS):
            name = f"upload-{uuid.uuid4().hex[:12]}.md"
            outcomes[name] = _upload(kb_client, kb_id, name)

        failed = {n: why for n, (state, _, why) in outcomes.items() if state == "failed"}
        saved = {n: rid for n, (state, rid, _) in outcomes.items() if state == "saved"}
        logger.info("%d upload(s) during the restart; %d failed: %s", during, len(failed), failed)
        late = list(outcomes)[-AFTER_RESTART_UPLOADS:]
        assert not set(late) & set(failed), (
            f"uploads made after MongoDB was back still failed: { {n: failed[n] for n in late if n in failed} }"
        )

        final = await wait_until_finished(kb_client, list(saved.values()))
        stuck = {n: final[rid] for n, rid in saved.items() if final[rid] != "COMPLETED"}
        assert not stuck, f"uploads reported as saved never finished indexing: {stuck}"

        listed = [item["name"] for item in kb_client.list_records(kb_id).get("items") or []]
        assert sorted(listed) == sorted(saved), (
            f"the knowledge base lists {sorted(listed)}; expected exactly the {len(saved)} saved uploads "
            f"and none of the {len(failed)} that failed ({sorted(failed)})"
        )

        retried = {}
        for name in failed:
            state, record_id, why = _upload(kb_client, kb_id, name)
            assert state == "saved", f"uploading {name} again after MongoDB was back failed: {why}"
            retried[name] = record_id
        final = await wait_until_finished(kb_client, list(retried.values()))
        stuck = {n: final[rid] for n, rid in retried.items() if final[rid] != "COMPLETED"}
        assert not stuck, f"uploads retried after the restart never finished indexing: {stuck}"
    finally:
        try:
            kb_client.delete_kb(kb_id)
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the result
            logger.warning("Could not delete knowledge base %s: %s", kb_id, exc)
