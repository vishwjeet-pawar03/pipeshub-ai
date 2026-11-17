"""BookStack sync against an in-memory BookStack and an in-memory record store.

A page deleted in BookStack, or left out by a narrowed sync filter, must leave
every store; a listing or a lookup BookStack could not answer must remove
nothing. The fakes answer the way the real services do: BookStack reports an
error as a JSON body with no page list, the store hands back a plain ``Record``
from ``get_record_by_external_id``, and sync points hold only strings.

A role deleted in BookStack must leave the store too. When that delete fails,
the failure must propagate so the run is not recorded as a success.

Since commit f466f5c7c (PR #5012) the connector no longer exposes one
``_sync_records``/``_sync_user_roles`` incremental entry point: a full listing
and removal now lives in ``_sync_records_full``, a single confirmed page
delete in ``_handle_page_delete_event``, and a role delete in
``_handle_role_delete_event``. These tests call those current methods
directly rather than through the retired combined entry points.

covers: backend/python/app/connectors/sources/bookstack/connector.py
"""

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from app.connectors.core.registry.filters import FilterCollection
from app.connectors.sources.bookstack.connector import BookStackConnector
from app.models.entities import FileRecord, Record
from app.services.graph_db.common.record_visibility import (
    RecordVisibility,
    matches_visibility,
)
from app.sources.client.bookstack.bookstack import BookStackResponse

CONNECTOR_ID = "bs-1"
KEPT_BOOK = 10
OTHER_BOOK = 20


class FakeBookStack:
    """Pages, their audit log and the failures BookStack can answer with."""

    def __init__(self) -> None:
        self.pages: dict[int, dict[str, Any]] = {}
        self.audit: list[dict[str, Any]] = []
        self.listing_error_at_offset: int | None = None
        self.lookup_error_for: set[int] = set()
        self.missed_by_listing: set[int] = set()
        self.delete_log_errors = 0
        # From this offset on, a listing answers with no rows but still reports the full total.
        self.listing_ends_early_at: int | None = None
        self.audit_ends_early_at: int | None = None
        self.lookups = 0

    def add_page(self, page_id: int, book_id: int = KEPT_BOOK, revision: int = 1) -> None:
        self.pages[page_id] = {
            "id": page_id, "name": f"page-{page_id}", "book_id": book_id, "chapter_id": None,
            "slug": f"page-{page_id}", "book_slug": f"book-{book_id}", "revision_count": revision,
            "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-02T00:00:00Z",
        }

    def delete_page(self, page_id: int) -> None:
        page = self.pages.pop(page_id)
        self.audit.append({"type": "page_delete", "detail": f"({page_id}) {page['name']}",
                           "loggable_type": "page", "loggable_id": page_id})

    def purge_page(self, page_id: int) -> None:
        """Delete with the recycle bin keeping nothing: BookStack strips the id from the event."""
        page = self.pages.pop(page_id)
        self.audit.append({"type": "page_delete", "detail": page["name"],
                           "loggable_type": "page", "loggable_id": None})

    @staticmethod
    def _body(body: dict[str, Any]) -> BookStackResponse:
        return BookStackResponse(success=True, data={"content": json.dumps(body), "content_type": "application/json"})

    async def list_pages(self, count: int | None = None, offset: int | None = None,
                         sort: str | None = None, filter: dict[str, str] | None = None) -> BookStackResponse:  # noqa: A002
        if filter and "id" in filter:
            self.lookups += 1
            page_id = int(filter["id"])
            if page_id in self.lookup_error_for:
                return self._body({"error": {"code": 429, "message": "Too many requests"}})
            found = [self.pages[page_id]] if page_id in self.pages else []
            return self._body({"data": found, "total": len(found)})
        offset = offset or 0
        if self.listing_error_at_offset is not None and offset >= self.listing_error_at_offset:
            return self._body({"error": {"code": 500, "message": "Server Error"}})
        ordered = [self.pages[k] for k in sorted(self.pages) if k not in self.missed_by_listing]
        rows = [] if self.listing_ends_early_at is not None and offset >= self.listing_ends_early_at else ordered[offset:offset + (count or 100)]
        return self._body({"data": rows, "total": len(ordered)})

    async def list_audit_log(self, count: int | None = None, offset: int | None = None,
                             sort: str | None = None, filter: dict[str, str] | None = None) -> BookStackResponse:  # noqa: A002
        wanted = (filter or {}).get("type")
        if wanted == "page_delete" and self.delete_log_errors:
            self.delete_log_errors -= 1
            return BookStackResponse(success=True, data={"error": {"code": 429, "message": "Too many requests"}})
        events = [e for e in self.audit if e["type"] == wanted]
        offset = offset or 0
        page = events[offset:offset + (count or 100)]
        if self.audit_ends_early_at is not None and offset >= self.audit_ends_early_at:
            page = []
        return BookStackResponse(success=True, data={"data": page, "total": len(events)})

    async def get_content_permissions(self, content_type: str, content_id: int) -> BookStackResponse:
        return BookStackResponse(success=True, data={"owner": None, "role_permissions": [],
                                                     "fallback_permissions": {"inheriting": True}})


class FakeRecordStore:
    """The slice of ``DataSourceEntitiesProcessor`` the page sync uses, kept in memory."""

    def __init__(self) -> None:
        self.org_id = "org-1"
        self.records: dict[str, Any] = {}
        self.deleted: list[str] = []
        self.fail_scan = False
        self.fail_delete = False

    async def get_record_by_external_id(self, connector_id: str, external_record_id: str) -> Record | None:
        found = self.records.get(external_record_id)
        if found is None:
            return None
        return Record.model_validate(found.model_dump(include=set(Record.model_fields)))

    async def get_records_by_status(self, connector_id: str, status_filters: list[str] | None,
                                    limit: int | None = None, offset: int = 0,
                                    after_key: str | None = None,
                                    visibility: RecordVisibility = RecordVisibility.LIVE,
                                    **_: object) -> list[Record]:
        """Live records only unless asked, as both graph stores answer."""
        if self.fail_scan:
            raise RuntimeError("graph unavailable")
        ordered = sorted((r for r in self.records.values() if matches_visibility(r, visibility)), key=lambda r: r.id)
        if after_key is not None:
            ordered = [r for r in ordered if r.id > after_key]
        return ordered[:limit] if limit else ordered

    async def on_new_records(self, batch: list[tuple[Any, list[Any]]]) -> None:
        for record, _ in batch:
            self.records[record.external_record_id] = record.model_copy(deep=True)

    async def on_record_metadata_update(self, record: FileRecord) -> None:
        self.records[record.external_record_id] = record.model_copy(deep=True)

    async def on_record_content_update(self, record: FileRecord) -> None:
        self.records[record.external_record_id] = record.model_copy(deep=True)

    async def on_updated_record_permissions(self, record: FileRecord, permissions: list[Any]) -> None:
        return None

    async def on_record_deleted(self, record_id: str) -> None:
        if self.fail_delete:
            raise RuntimeError("graph unavailable")
        match = next((k for k, r in self.records.items() if r.id == record_id), None)
        if match is not None:
            del self.records[match]
            self.deleted.append(match)


class FakeSyncPoint:
    def __init__(self) -> None:
        self.points: dict[str, dict[str, str]] = {}

    async def read_sync_point(self, key: str) -> dict[str, str]:
        return dict(self.points.get(key, {}))

    async def update_sync_point(self, key: str, data: dict[str, str]) -> None:
        # Timestamps are strings; the role sync point also carries the rebuildOwed flag.
        assert all(isinstance(v, (str, bool)) for v in data.values())
        # Both graph stores merge an update into the stored sync point.
        self.points.setdefault(key, {}).update(data)


@contextmanager
def _connector(source: FakeBookStack, store: FakeRecordStore) -> Iterator[BookStackConnector]:
    with patch("app.connectors.sources.bookstack.connector.BookStackApp"), \
         patch("app.connectors.sources.bookstack.connector.SyncPoint", side_effect=lambda **_: FakeSyncPoint()):
        connector = BookStackConnector(
            logger=logging.getLogger("bookstack-behaviour"),
            data_entities_processor=store,
            data_store_provider=AsyncMock(),
            config_service=AsyncMock(),
            connector_id=CONNECTOR_ID,
            scope="team",
            created_by="admin",
        )
    connector.data_source = source
    connector.bookstack_base_url = "https://bookstack.example.com/"
    connector.list_roles_with_details = AsyncMock(return_value={})
    connector.get_all_users = AsyncMock(return_value=[])
    connector.batch_size = 2
    yield connector


def _without_book(book_id: int) -> FilterCollection:
    return FilterCollection.from_dict({"book_ids": {"operator": "not_in", "type": "list", "value": [str(book_id)]}})


@pytest.fixture
def world() -> Iterator[tuple[FakeBookStack, FakeRecordStore, Any]]:
    source, store = FakeBookStack(), FakeRecordStore()
    for page_id in (1, 2, 3):
        source.add_page(page_id)
    source.add_page(4, book_id=OTHER_BOOK)
    with _connector(source, store) as connector:
        yield source, store, connector


# ---------------------------------------------------------------------------
# A single confirmed page delete (``_handle_page_delete_event``): the unit the
# audit-event pipeline (``_apply_events``) calls for one ``page_delete`` event.
# ---------------------------------------------------------------------------

async def test_a_page_deleted_in_bookstack_leaves_on_the_next_incremental_sync(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    assert set(store.records) == {"page/1", "page/2", "page/3", "page/4"}

    source.delete_page(2)
    assert await connector._handle_page_delete_event(2) is True

    assert store.deleted == ["page/2"]
    assert set(store.records) == {"page/1", "page/3", "page/4"}


async def test_a_page_restored_before_the_sync_keeps_its_record(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    page = dict(source.pages[2])
    source.delete_page(2)
    source.pages[2] = page

    assert await connector._handle_page_delete_event(2) is True

    assert store.deleted == []
    assert "page/2" in store.records


async def test_a_purged_page_whose_event_lost_its_id_is_still_removed(world) -> None:
    """A purge event names no id; the reconciliation sweep (a full sync) still finds it gone."""
    source, store, connector = world
    await connector._sync_records_full()
    source.purge_page(2)

    await connector._sync_records_full()

    assert store.deleted == ["page/2"]
    assert set(store.records) == {"page/1", "page/3", "page/4"}


async def test_a_delete_bookstack_cannot_confirm_is_kept_and_retried(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.delete_page(2)
    source.lookup_error_for.add(2)

    assert await connector._handle_page_delete_event(2) is False
    assert store.deleted == []
    assert "page/2" in store.records

    source.lookup_error_for.clear()
    assert await connector._handle_page_delete_event(2) is True
    assert store.deleted == ["page/2"]


async def test_a_delete_that_fails_to_save_is_retried(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.delete_page(2)
    store.fail_delete = True

    assert await connector._handle_page_delete_event(2) is False
    assert "page/2" in store.records

    store.fail_delete = False
    assert await connector._handle_page_delete_event(2) is True
    assert store.deleted == ["page/2"]


# ---------------------------------------------------------------------------
# The reconciliation sweep of a full listing (``_sync_records_full``): removes
# a page the listing no longer returns, after a complete listing only.
# ---------------------------------------------------------------------------

async def test_a_page_list_that_ends_before_its_total_removes_nothing_and_looks_nothing_up(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.purge_page(2)
    with patch("app.connectors.sources.bookstack.connector._LISTING_PAGE_SIZE", 2):
        source.listing_ends_early_at = 2
        await connector._sync_records_full()
        assert store.deleted == []
        assert source.lookups == 0, "an incomplete list must not fall back to one lookup per page"
        assert not connector._health.ok

        source.listing_ends_early_at = None
        connector._health = type(connector._health)()
        await connector._sync_records_full()

    assert store.deleted == ["page/2"]


async def test_a_full_listing_that_ends_before_its_total_removes_nothing(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.pages.pop(3)

    with patch("app.connectors.sources.bookstack.connector._LISTING_PAGE_SIZE", 2):
        source.listing_ends_early_at = 2
        await connector._sync_records_full()
        assert store.deleted == []
        assert source.lookups == 0, "a cut-off listing must not fall back to one lookup per page"
        assert not connector._health.ok

        source.listing_ends_early_at = None
        connector._health = type(connector._health)()
        await connector._sync_records_full()
    assert store.deleted == ["page/3"]


async def test_a_narrowed_book_filter_removes_the_pages_it_now_leaves_out(world) -> None:
    _, store, connector = world
    await connector._sync_records_full()

    connector.sync_filters = _without_book(OTHER_BOOK)
    await connector._sync_records_full()

    assert store.deleted == ["page/4"]
    assert set(store.records) == {"page/1", "page/2", "page/3"}


async def test_a_full_sync_removes_a_page_deleted_while_incremental_syncs_missed_it(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.pages.pop(3)

    await connector._sync_records_full()

    assert store.deleted == ["page/3"]


async def test_a_full_sync_removes_a_trashed_page_bookstack_no_longer_has(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    store.records["page/3"].is_deleted = True
    source.pages.pop(3)

    await connector._sync_records_full()

    assert store.deleted == ["page/3"], "the record scan must see trashed records, or this one stays forever"


async def test_a_listing_that_fails_part_way_removes_nothing_until_a_full_listing(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()

    connector.sync_filters = _without_book(OTHER_BOOK)
    with patch("app.connectors.sources.bookstack.connector._LISTING_PAGE_SIZE", 2):
        source.listing_error_at_offset = 2
        await connector._sync_records_full()

        assert store.deleted == []
        assert "page/4" in store.records
        assert not connector._health.ok

        source.listing_error_at_offset = None
        connector._health = type(connector._health)()
        await connector._sync_records_full()
    assert store.deleted == ["page/4"]


async def test_a_page_the_listing_skipped_is_kept_when_bookstack_still_has_it(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.missed_by_listing.add(2)

    await connector._sync_records_full()

    assert store.deleted == []
    assert "page/2" in store.records


async def test_an_unlisted_page_whose_lookup_fails_is_kept_and_rechecked(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.pages.pop(3)
    source.lookup_error_for.add(3)

    await connector._sync_records_full()
    assert store.deleted == []

    source.lookup_error_for.clear()
    connector._health = type(connector._health)()
    await connector._sync_records_full()
    assert store.deleted == ["page/3"]


async def test_a_listing_bookstack_refuses_outright_removes_nothing(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()

    source.listing_error_at_offset = 0
    await connector._sync_records_full()

    assert store.deleted == []
    assert len(store.records) == 4
    assert not connector._health.ok


async def test_a_record_store_that_cannot_be_read_removes_nothing(world) -> None:
    source, store, connector = world
    await connector._sync_records_full()
    source.pages.pop(3)

    store.fail_scan = True
    await connector._sync_records_full()

    assert store.deleted == []
    assert not connector._health.ok


# ---------------------------------------------------------------------------
# Role deletion (``_handle_role_delete_event``): the unit the audit-event
# pipeline calls for one ``role_delete`` event. A failed delete must raise so
# ``run_sync`` does not record the sync as successful and the next run reads
# the deletion again (see ``test_bookstack_run_cursor.py``).
# ---------------------------------------------------------------------------

class FakeRoleStore:
    """The role methods of ``DataSourceEntitiesProcessor`` the role-delete handler uses."""

    def __init__(self) -> None:
        self.org_id = "org-1"
        self.roles: dict[str, list[str]] = {}
        self.fail_role_delete = False

    async def on_app_role_deleted(self, external_role_id: str, connector_id: str) -> bool:
        # Like the real processor: a delete that fails raises; False when the role isn't stored.
        if self.fail_role_delete:
            raise RuntimeError("graph unavailable")
        return self.roles.pop(external_role_id, None) is not None


async def test_a_role_delete_removes_the_stored_role() -> None:
    store = FakeRoleStore()
    store.roles["8"] = ["ana@acme.com"]
    with _connector(FakeBookStack(), store) as connector:
        await connector._handle_role_delete_event(8)

    assert store.roles == {}


async def test_a_role_delete_the_store_refuses_raises_so_the_run_is_not_recorded_as_successful() -> None:
    store = FakeRoleStore()
    store.roles["8"] = ["ana@acme.com"]
    store.fail_role_delete = True
    with _connector(FakeBookStack(), store) as connector, \
         pytest.raises(RuntimeError, match="graph unavailable"):
        await connector._handle_role_delete_event(8)

    assert "8" in store.roles, "the deleted role still gives its members access"
