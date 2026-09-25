"""Start-up and shutdown of IndexingKafkaConsumer's worker loop when the worker
thread is slow to reach run_forever().

The worker creates its loop, signals readiness, then calls run_forever(). If
readiness is signalled before the loop runs, initialize() can see a loop that
is not running yet and fail; the stop that follows must still end the worker.
These tests hold the worker in that gap on purpose instead of hoping the
scheduler lands there.
"""

import asyncio
import logging
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from unittest.mock import AsyncMock, patch

import pytest

from app.services.messaging.kafka.config.kafka_config import KafkaConsumerConfig
from app.services.messaging.kafka.consumer import indexing_consumer as consumer_module
from app.services.messaging.kafka.consumer.indexing_consumer import (
    IndexingKafkaConsumer,
)

# A healthy start or stop takes milliseconds; this only bounds a hang so the
# test fails with a message instead of tripping pytest's global timeout.
_HANG_TIMEOUT = 10.0


@pytest.fixture
def consumer() -> IndexingKafkaConsumer:
    config = KafkaConsumerConfig(
        topics=["idx-topic"],
        client_id="idx-consumer",
        group_id="idx-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        bootstrap_servers=["broker:9092"],
        ssl=False,
        sasl=None,
    )
    return IndexingKafkaConsumer(logging.getLogger("test_startup_race"), config)


class _WorkerGate:
    """Parks the worker thread just before run_forever() until the consumer
    begins shutting its executor down, by which point stop has already
    decided whether to ask the loop to stop."""

    def __init__(self, should_park: Callable[[], bool]) -> None:
        self.should_park = should_park
        self.parked = threading.Event()
        self.release = threading.Event()
        self.loop: asyncio.AbstractEventLoop | None = None

    @contextmanager
    def installed(self) -> Iterator["_WorkerGate"]:
        gate = self
        real_new_event_loop = asyncio.new_event_loop

        class GatedLoop(asyncio.SelectorEventLoop):
            def run_forever(self) -> None:
                if gate.should_park():
                    gate.parked.set()
                    gate.release.wait(_HANG_TIMEOUT)
                super().run_forever()

        def new_event_loop() -> asyncio.AbstractEventLoop:
            if threading.current_thread().name.startswith("indexing-worker"):
                gate.loop = GatedLoop()
                return gate.loop
            return real_new_event_loop()

        class ReleasingExecutor(ThreadPoolExecutor):
            def shutdown(self, wait: bool = True, **kwargs: object) -> None:
                gate.release.set()
                super().shutdown(wait=wait, **kwargs)

        with (
            patch.object(asyncio, "new_event_loop", new_event_loop),
            patch.object(consumer_module, "ThreadPoolExecutor", ReleasingExecutor),
        ):
            yield self

    def force_stop(self) -> None:
        """Unwind a hung worker so a failing test does not leak its thread."""
        self.release.set()
        loop = self.loop
        if loop is not None and not loop.is_closed():
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)


def _run_in_thread(target: Callable[[], None], gate: _WorkerGate) -> bool:
    """Run target in a thread; return True if it hung (it is unwound after)."""
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(_HANG_TIMEOUT)
    hung = thread.is_alive()
    if hung:
        gate.force_stop()
        thread.join(_HANG_TIMEOUT)
    return hung


def test_ready_is_signalled_only_once_the_worker_loop_is_running(consumer) -> None:
    running_when_set: list[bool] = []

    class RecordingEvent(threading.Event):
        def set(self) -> None:
            loop = consumer.worker_loop
            running_when_set.append(loop is not None and loop.is_running())
            super().set()

    consumer.worker_loop_ready = RecordingEvent()
    consumer._IndexingKafkaConsumer__start_worker_thread()
    try:
        assert consumer.worker_loop_ready.wait(timeout=_HANG_TIMEOUT)
        assert running_when_set == [True]
    finally:
        consumer._IndexingKafkaConsumer__stop_worker_thread()


def test_initialize_succeeds_when_worker_is_slow_to_start_its_loop(consumer) -> None:
    # Parks only if readiness was signalled before run_forever(), which is
    # exactly the window initialize() used to fall into.
    gate = _WorkerGate(should_park=consumer.worker_loop_ready.is_set)
    outcome: dict[str, BaseException] = {}

    def run_initialize() -> None:
        try:
            asyncio.run(consumer.initialize())
        except BaseException as exc:
            outcome["error"] = exc

    with (
        gate.installed(),
        patch.object(consumer_module, "AIOKafkaConsumer") as kafka_cls,
    ):
        kafka_cls.return_value.start = AsyncMock()
        kafka_cls.return_value.stop = AsyncMock()
        hung = _run_in_thread(run_initialize, gate)

    try:
        assert not hung, "initialize() hung shutting down a worker loop nobody asked to stop"
        assert "error" not in outcome, f"initialize() failed: {outcome.get('error')!r}"
        assert consumer.worker_loop is not None
        assert consumer.worker_loop.is_running()
    finally:
        asyncio.run(consumer.stop())


def test_stop_ends_a_worker_whose_loop_is_not_running_yet(consumer) -> None:
    gate = _WorkerGate(should_park=lambda: True)
    with gate.installed():
        consumer._IndexingKafkaConsumer__start_worker_thread()
        assert gate.parked.wait(_HANG_TIMEOUT)
        loop = gate.loop
        assert loop is not None and not loop.is_running()

        hung = _run_in_thread(consumer._IndexingKafkaConsumer__stop_worker_thread, gate)

    assert not hung, "stop hung waiting on a worker loop it never asked to stop"
    assert loop.is_closed()
    assert consumer.worker_executor is None
    assert consumer.worker_loop is None
