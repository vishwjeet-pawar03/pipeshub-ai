"""The budget stale-record recovery spends before giving up on a record."""
from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.config.constants.arangodb import ProgressStatus
from app.modules.indexing.recovery_budget import (
    exhausted_fields,
    recovery_exhausted,
    recovery_tracking_id,
)
from app.utils.user_errors import ENRICHMENT_FAILED, RETRIES_EXHAUSTED

if TYPE_CHECKING:
    from collections.abc import Iterator


@pytest.fixture(autouse=True)
def limits() -> Iterator[None]:
    with patch("app.modules.indexing.recovery_budget.messaging_env") as env:
        env.max_delivery_attempts = 3
        env.redis_max_deliveries = 10
        yield


def _tracker(failures: int = 0, deliveries: int = 1) -> AsyncMock:
    tracker = AsyncMock()
    tracker.get_count.return_value = failures
    tracker.record_delivery.return_value = deliveries
    return tracker


class TestRecoveryExhausted:
    def test_one_tracking_id_per_record_version(self) -> None:
        assert recovery_tracking_id("r1", 2) == "stale-recovery:r1:2"
        assert recovery_tracking_id("r1", 2) != recovery_tracking_id("r1", 3)

    async def test_without_a_tracker_there_is_no_limit(self) -> None:
        assert await recovery_exhausted(None, "t", MagicMock()) is False

    async def test_failures_at_the_retry_limit_exhaust_it_without_counting_a_delivery(self) -> None:
        tracker = _tracker(failures=3)

        assert await recovery_exhausted(tracker, "t", MagicMock()) is True
        tracker.record_delivery.assert_not_awaited()

    @pytest.mark.parametrize(("deliveries", "exhausted"), [(9, False), (10, True)])
    async def test_hand_backs_are_bounded_by_the_delivery_backstop(
        self, deliveries: int, exhausted: bool
    ) -> None:
        tracker = _tracker(failures=0, deliveries=deliveries)

        assert await recovery_exhausted(tracker, "t", MagicMock()) is exhausted
        tracker.record_delivery.assert_awaited_once_with("t")

    async def test_an_unreadable_budget_lets_the_record_through(self) -> None:
        """The event still carries the consumer's own retry budget."""
        tracker = _tracker()
        tracker.get_count.side_effect = TimeoutError("Timeout connecting to server")
        logger = MagicMock()

        assert await recovery_exhausted(tracker, "t", logger) is False
        logger.warning.assert_called_once()


class TestExhaustedFields:
    def test_a_record_that_never_indexed_is_marked_failed(self) -> None:
        fields = exhausted_fields(
            {"extractionStatus": ProgressStatus.NOT_STARTED.value}, resume_enrichment=False
        )

        assert fields == {
            "parsingStatus": ProgressStatus.FAILED.value,
            "indexingStatus": ProgressStatus.FAILED.value,
            "extractionStatus": ProgressStatus.FAILED.value,
            "processingStartedAt": None,
            "reason": RETRIES_EXHAUSTED,
        }

    def test_a_completed_extraction_is_kept(self) -> None:
        fields = exhausted_fields(
            {"extractionStatus": ProgressStatus.COMPLETED.value}, resume_enrichment=False
        )

        assert fields["extractionStatus"] == ProgressStatus.COMPLETED.value

    def test_an_indexed_record_stays_searchable_and_only_its_enrichment_fails(self) -> None:
        fields = exhausted_fields(
            {"indexingStatus": ProgressStatus.COMPLETED.value}, resume_enrichment=True
        )

        assert fields == {
            "extractionStatus": ProgressStatus.FAILED.value,
            "processingStartedAt": None,
            "reason": ENRICHMENT_FAILED,
        }
