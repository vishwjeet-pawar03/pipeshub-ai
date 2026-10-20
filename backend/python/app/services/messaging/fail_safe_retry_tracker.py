"""A retry tracker whose budget holds while its store does not answer.

A consumer that cannot count a failure leaves the message pending, and its
own-pending recovery offers that entry again within a second, through a claim
that deliberately counts no delivery. So while the store stayed unreachable,
a record that kept failing was run again and again, every model call
included, with nothing to stop it. This tracker also counts in the process
and answers from those counts whenever the store raises.

The process's counts are a floor, never the answer on their own while the
store works: the store's survive restarts and are shared by every replica.
"""
from __future__ import annotations

import time
from collections import OrderedDict
from typing import TYPE_CHECKING

from app.services.distributed.interface import IRetryTracker

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from logging import Logger

DEFAULT_MAX_ENTRIES = 10_000
# One warning per operation per interval while the store is failing.
_WARN_INTERVAL_SECONDS = 30.0


class _Tally:
    """Counts per id, each forgotten *ttl_seconds* after its last change; the oldest go first past *max_entries*."""

    def __init__(self, ttl_seconds: float, max_entries: int, clock: Callable[[], float]) -> None:
        self._ttl = ttl_seconds
        self._max_entries = max(1, max_entries)
        self._clock = clock
        self._counts: OrderedDict[str, tuple[int, float]] = OrderedDict()

    def increment(self, key: str) -> int:
        count = self.get(key) + 1
        self._counts[key] = (count, self._clock() + self._ttl)
        self._counts.move_to_end(key)
        while len(self._counts) > self._max_entries:
            self._counts.popitem(last=False)
        return count

    def get(self, key: str) -> int:
        entry = self._counts.get(key)
        if entry is None:
            return 0
        count, expires_at = entry
        if self._clock() >= expires_at:
            del self._counts[key]
            return 0
        return count

    def discard(self, key: str) -> None:
        self._counts.pop(key, None)


class FailSafeRetryTracker(IRetryTracker):
    def __init__(
        self,
        inner: IRetryTracker,
        logger: Logger,
        *,
        ttl_seconds: float,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._inner = inner
        self._logger = logger
        self._clock = clock
        self._failures = _Tally(ttl_seconds, max_entries, clock)
        self._deliveries = _Tally(ttl_seconds, max_entries, clock)
        self._last_warning: dict[str, float] = {}

    async def initialize(self) -> None:
        await self._inner.initialize()

    async def cleanup(self) -> None:
        await self._inner.cleanup()

    async def increment_and_check(self, message_id: str, max_attempts: int) -> tuple[int, bool]:
        local = self._failures.increment(message_id)

        async def stored() -> int:
            count, _ = await self._inner.increment_and_check(message_id, max_attempts)
            return count

        count = await self._at_least(local, stored, "count a failure")
        return count, count >= max_attempts

    async def get_count(self, message_id: str) -> int:
        return await self._at_least(
            self._failures.get(message_id),
            lambda: self._inner.get_count(message_id),
            "read a failure count",
        )

    async def record_delivery(self, message_id: str) -> int:
        return await self._at_least(
            self._deliveries.increment(message_id),
            lambda: self._inner.record_delivery(message_id),
            "count a delivery",
        )

    async def clear(self, message_id: str) -> None:
        self._forget(message_id)
        await self._inner.clear(message_id)

    async def clear_batch(self, message_ids: list[str]) -> int:
        for message_id in message_ids:
            self._forget(message_id)
        return await self._inner.clear_batch(message_ids)

    async def has_pending_retries(self, message_ids: list[str]) -> bool:
        try:
            return await self._inner.has_pending_retries(message_ids)
        except Exception as exc:
            self._warn("check pending retries", exc)
            return any(self._failures.get(message_id) for message_id in message_ids)

    async def _at_least(self, local: int, stored: Callable[[], Awaitable[int]], what: str) -> int:
        try:
            return max(int(await stored()), local)
        except Exception as exc:
            self._warn(what, exc)
            return local

    def _forget(self, message_id: str) -> None:
        self._failures.discard(message_id)
        self._deliveries.discard(message_id)

    def _warn(self, what: str, exc: Exception) -> None:
        now = self._clock()
        if now - self._last_warning.get(what, float("-inf")) < _WARN_INTERVAL_SECONDS:
            return
        self._last_warning[what] = now
        self._logger.warning(
            "Retry store could not %s (%r); using this process's count until it answers",
            what, exc,
        )
