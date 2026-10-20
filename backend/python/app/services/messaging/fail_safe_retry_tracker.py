"""A retry tracker whose budget holds while its store does not answer.

A consumer that cannot count a failure leaves the message pending, and its
own-pending recovery offers that entry again within a second, through a claim
that deliberately counts no delivery. So while the store stayed unreachable,
a record that kept failing was run again and again, every model call
included, with nothing to stop it.

This tracker keeps, per id, the increments the store did not take and the
last count the store reported. While the store fails it answers with the two
together; once a write gets through it hands the store the increments it
missed. While the store answers, the store's count is the count: it survives
restarts, is shared by every replica, and a clear by any of them wins.
"""
from __future__ import annotations

import threading
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
    """Values per id, each forgotten *ttl_seconds* after its last change; the oldest go first past *max_entries*.

    Locked: the consumers reach the tracker from their main loop and their
    worker loop, which run on different threads.
    """

    def __init__(self, ttl_seconds: float, max_entries: int, clock: Callable[[], float]) -> None:
        self._ttl = ttl_seconds
        self._max_entries = max(1, max_entries)
        self._clock = clock
        self._values: OrderedDict[str, tuple[int, float]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> int:
        with self._lock:
            return self._get(key)

    def add(self, key: str, delta: int) -> int:
        with self._lock:
            return self._set(key, self._get(key) + delta)

    def set(self, key: str, value: int) -> None:
        with self._lock:
            self._set(key, value)

    def discard(self, key: str) -> None:
        with self._lock:
            self._values.pop(key, None)

    def _get(self, key: str) -> int:
        entry = self._values.get(key)
        if entry is None:
            return 0
        value, expires_at = entry
        if self._clock() >= expires_at:
            del self._values[key]
            return 0
        return value

    def _set(self, key: str, value: int) -> int:
        if value <= 0:
            self._values.pop(key, None)
            return 0
        self._values[key] = (value, self._clock() + self._ttl)
        self._values.move_to_end(key)
        while len(self._values) > self._max_entries:
            self._values.popitem(last=False)
        return value


class _Counter:
    """One kind of count (failures, deliveries, hand-backs) as this process knows it."""

    def __init__(self, ttl_seconds: float, max_entries: int, clock: Callable[[], float]) -> None:
        self.stored = _Tally(ttl_seconds, max_entries, clock)
        self.unpersisted = _Tally(ttl_seconds, max_entries, clock)

    def known(self, key: str) -> int:
        return self.stored.get(key) + self.unpersisted.get(key)

    def forget(self, key: str) -> None:
        self.stored.discard(key)
        self.unpersisted.discard(key)


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
        self._failures = _Counter(ttl_seconds, max_entries, clock)
        self._deliveries = _Counter(ttl_seconds, max_entries, clock)
        self._handbacks = _Counter(ttl_seconds, max_entries, clock)
        self._last_warning: dict[str, float] = {}

    async def initialize(self) -> None:
        await self._inner.initialize()

    async def cleanup(self) -> None:
        await self._inner.cleanup()

    async def increment_and_check(self, message_id: str, max_attempts: int) -> tuple[int, bool]:
        async def increment() -> int:
            count, _ = await self._inner.increment_and_check(message_id, max_attempts)
            return count

        count = await self._increment(self._failures, message_id, increment, "count a failure")
        return count, count >= max_attempts

    async def get_count(self, message_id: str) -> int:
        return await self._read(
            self._failures, message_id, lambda: self._inner.get_count(message_id), "read a failure count"
        )

    async def record_delivery(self, message_id: str) -> int:
        return await self._increment(
            self._deliveries, message_id, lambda: self._inner.record_delivery(message_id), "count a delivery"
        )

    async def record_handback(self, message_id: str) -> int:
        return await self._increment(
            self._handbacks, message_id, lambda: self._inner.record_handback(message_id), "count a hand-back"
        )

    async def get_handbacks(self, message_id: str) -> int:
        return await self._read(
            self._handbacks, message_id, lambda: self._inner.get_handbacks(message_id), "read a hand-back count"
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
            return any(self._failures.known(message_id) for message_id in message_ids)

    async def _increment(
        self, counter: _Counter, key: str, increment: Callable[[], Awaitable[int]], what: str
    ) -> int:
        try:
            stored = int(await increment())
        except Exception as exc:
            self._warn(what, exc)
            counter.unpersisted.add(key, 1)
            return counter.known(key)
        # Hand over what the store missed, one taken increment at a time, so an
        # increment that fails concurrently is kept rather than written off.
        while counter.unpersisted.get(key) > 0:
            try:
                stored = int(await increment())
            except Exception as exc:
                self._warn(what, exc)
                break
            counter.unpersisted.add(key, -1)
        counter.stored.set(key, stored)
        return counter.known(key)

    async def _read(
        self, counter: _Counter, key: str, read: Callable[[], Awaitable[int]], what: str
    ) -> int:
        try:
            stored = int(await read())
        except Exception as exc:
            self._warn(what, exc)
            return counter.known(key)
        counter.stored.set(key, stored)
        return counter.known(key)

    def _forget(self, message_id: str) -> None:
        for counter in (self._failures, self._deliveries, self._handbacks):
            counter.forget(message_id)

    def _warn(self, what: str, exc: Exception) -> None:
        now = self._clock()
        if now - self._last_warning.get(what, float("-inf")) < _WARN_INTERVAL_SECONDS:
            return
        self._last_warning[what] = now
        self._logger.warning(
            "Retry store could not %s (%r); using this process's count until it answers",
            what, exc,
        )
