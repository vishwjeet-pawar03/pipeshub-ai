"""How often stale-record recovery may hand the same record back for indexing.

Recovery republishes a record left IN_PROGRESS past its deadline. Its events
carried no retry tracking id, so the consumer counted their failures under a
new id each time and every recovery started a fresh retry budget: a record
that hung its worker on every attempt was indexed again each recovery
interval, model calls included, for good.

Every event recovery publishes for one version of a record now shares a
tracking id, so their failures add up in the consumer's counter, and recovery
counts its own hand-backs in the delivery counter, which also covers attempts
that died before a failure could be counted. Past either limit the record is
marked failed instead. A success clears both counters.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import ProgressStatus
from app.services.messaging.config import messaging_env
from app.utils.user_errors import ENRICHMENT_FAILED, RETRIES_EXHAUSTED

if TYPE_CHECKING:
    from logging import Logger

    from app.services.distributed.interface import IRetryTracker


def recovery_tracking_id(record_id: str, version: object) -> str:
    return f"stale-recovery:{record_id}:{version}"


async def recovery_exhausted(
    tracker: IRetryTracker | None, tracking_id: str, logger: Logger
) -> bool:
    """True when the record has failed or been handed back too often. Counts this hand-back."""
    if tracker is None:
        return False
    try:
        if await tracker.get_count(tracking_id) >= messaging_env.max_delivery_attempts:
            return True
        return await tracker.record_delivery(tracking_id) >= messaging_env.redis_max_deliveries
    except Exception as exc:
        # The consumer's own retry budget still applies to the event.
        logger.warning("Could not read recovery budget %s (%r); recovering anyway", tracking_id, exc)
        return False


def exhausted_fields(record: dict[str, Any], *, resume_enrichment: bool) -> dict[str, Any]:
    """The status to write on a record recovery gives up on."""
    if resume_enrichment:
        # Indexed and searchable; only its enrichment is abandoned.
        return {
            "extractionStatus": ProgressStatus.FAILED.value,
            "processingStartedAt": None,
            "reason": ENRICHMENT_FAILED,
        }
    extraction_done = record.get("extractionStatus") == ProgressStatus.COMPLETED.value
    return {
        "parsingStatus": ProgressStatus.FAILED.value,
        "indexingStatus": ProgressStatus.FAILED.value,
        "extractionStatus": (
            ProgressStatus.COMPLETED.value if extraction_done else ProgressStatus.FAILED.value
        ),
        "processingStartedAt": None,
        "reason": RETRIES_EXHAUSTED,
    }
