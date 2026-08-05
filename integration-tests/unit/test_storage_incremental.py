"""Unit tests for storage incremental-sync IT helpers (no live services)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from helper.storage_incremental import (
    assert_incremental_new_files,
    record_names_from_keys,
    restart_sync,
    settle_record_baseline,
    sync_until_names_visible,
    unique_incremental_csv_files,
)

pytestmark = pytest.mark.unit


def test_unique_incremental_csv_files_are_distinct_and_suffixed() -> None:
    files = unique_incremental_csv_files(suffix="deadbeef")
    assert sorted(files) == [
        "incremental-test/new-file-alpha-deadbeef.csv",
        "incremental-test/new-file-beta-deadbeef.csv",
    ]
    assert all(body.startswith(b"id,name,value\n") for body in files.values())

    a = unique_incremental_csv_files()
    b = unique_incremental_csv_files()
    assert set(a) != set(b)


def test_record_names_from_keys_accepts_dict_or_list() -> None:
    files = unique_incremental_csv_files(suffix="abc")
    assert record_names_from_keys(files) == [
        "new-file-alpha-abc.csv",
        "new-file-beta-abc.csv",
    ]
    assert record_names_from_keys(list(files)) == record_names_from_keys(files)


def test_restart_sync_toggles_off_then_on_with_waits() -> None:
    client = MagicMock()
    restart_sync(client, "conn-1")
    assert client.toggle_sync.call_args_list == [
        call("conn-1", enable=False),
        call("conn-1", enable=True),
    ]
    assert client.wait.call_args_list == [call(5), call(8)]


@pytest.mark.asyncio
async def test_settle_record_baseline_waits_idle_then_stable_count() -> None:
    client = MagicMock()
    graph = MagicMock()
    with (
        patch(
            "helper.storage_incremental.wait_for_sync_completion",
            new_callable=AsyncMock,
        ) as wait_sync,
        patch(
            "helper.storage_incremental.async_wait_for_stable_record_count",
            new_callable=AsyncMock,
            return_value=42,
        ) as wait_stable,
    ):
        count = await settle_record_baseline(client, graph, "conn-1", timeout=120)

    assert count == 42
    wait_sync.assert_awaited_once_with(
        client, graph, "conn-1", timeout=120, sync_start_timeout=0
    )
    wait_stable.assert_awaited_once_with(graph, "conn-1")


@pytest.mark.asyncio
async def test_sync_until_names_visible_retries_until_names_appear() -> None:
    client = MagicMock()
    graph = MagicMock()
    graph.record_paths_or_names_contain = AsyncMock(side_effect=[False, True, True])
    graph.count_records = AsyncMock(return_value=10)
    restarts: list[str] = []

    def _restart(_client: object, connector_id: str) -> None:
        restarts.append(connector_id)

    with (
        patch(
            "helper.storage_incremental.wait_for_sync_completion",
            new_callable=AsyncMock,
        ) as wait_sync,
        patch(
            "helper.storage_incremental.wait_until_graph_condition",
            new_callable=AsyncMock,
        ) as wait_names,
    ):
        after = await sync_until_names_visible(
            client,
            graph,
            "conn-1",
            ["a.csv", "b.csv"],
            max_attempts=3,
            restart_fn=_restart,
        )

    assert after == 10
    assert restarts == ["conn-1", "conn-1"]
    assert wait_sync.await_count == 2
    wait_names.assert_awaited_once()


@pytest.mark.asyncio
async def test_assert_incremental_new_files_requires_delta_and_names() -> None:
    graph = MagicMock()
    graph.assert_record_paths_or_names_contain = AsyncMock()

    await assert_incremental_new_files(
        graph,
        "conn-1",
        before_count=5,
        after_count=7,
        new_names=["a.csv", "b.csv"],
    )
    graph.assert_record_paths_or_names_contain.assert_awaited_once_with(
        "conn-1", ["a.csv", "b.csv"]
    )

    with pytest.raises(AssertionError, match="at least 2 new records"):
        await assert_incremental_new_files(
            graph,
            "conn-1",
            before_count=5,
            after_count=6,
            new_names=["a.csv", "b.csv"],
        )
