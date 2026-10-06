"""L06: copies stored under the flat records/<vrid> layout (before hierarchical
storage) are moved under their record's path once; resumable per org."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.services.flat_storage_migration import FlatStorageMigration

KEY = "/services/storageMigration/flatToHierarchical/org-1"


class _KV:
    def __init__(self):
        self.data = {}

    async def get_config(self, key, use_cache=True, default=None):
        return self.data.get(key, default)

    async def set_config(self, key, value):
        self.data[key] = dict(value)
        return True


def _migration(pages, holders, *, stored_owner=None, path="records/conn/Team/report"):
    kv = _KV()
    gp = AsyncMock()
    gp.get_all_orgs = AsyncMock(return_value=[{"id": "org-1"}])
    gp.get_records_by_virtual_record_id = AsyncMock(side_effect=lambda vrid, **kw: holders.get(vrid, []))
    gp.get_document = AsyncMock(side_effect=lambda key, coll: {"_key": key, "connectorId": "conn"})
    blob = MagicMock()
    blob._get_auth_and_config = AsyncMock(return_value=({}, "http://node", "local"))
    blob.get_record_from_storage = AsyncMock(return_value={"id": stored_owner} if stored_owner else None)
    m = FlatStorageMigration(MagicMock(), kv, gp, blob)
    m._flat_documents = AsyncMock(side_effect=list(pages) + [[]])
    return m, kv, path


@pytest.fixture(autouse=True)
def _records_from_graph_docs():
    # The graph document -> Record conversion is covered elsewhere.
    with patch("app.events.processor.convert_record_dict_to_record", side_effect=lambda d: MagicMock(**d)):
        yield


def _docs(*vrids, start=0):
    out = []
    for i, v in enumerate(vrids):
        out.append({"_id": f"{start + 2 * i:024x}", "documentName": f"record_{v}", "documentPath": f"org-1/PipesHub/records/{v}"})
        out.append({"_id": f"{start + 2 * i + 1:024x}", "documentName": f"metadata_{v}", "documentPath": f"org-1/PipesHub/records/{v}"})
    return out


@pytest.mark.asyncio
async def test_each_vrid_is_moved_once_under_its_records_path():
    m, kv, path = _migration([_docs("v1", "v2")], {"v1": ["r1"], "v2": ["r2"]})
    with patch("app.connectors.services.flat_storage_migration.build_hierarchical_storage_path",
               new_callable=AsyncMock, return_value="records/conn/Team/report"), \
         patch("app.connectors.services.flat_storage_migration.post_move_tree",
               new_callable=AsyncMock, return_value={"moved": 2}) as move:
        await m.run_org("org-1")
    moved = [(c.args[3], c.args[4], c.kwargs) for c in move.await_args_list]
    assert moved == [
        ("records/v1", "records/conn/Team/report", {"virtual_record_ids": ["v1"]}),
        ("records/v2", "records/conn/Team/report", {"virtual_record_ids": ["v2"]}),
    ]
    assert kv.data[KEY]["done"] is True and kv.data[KEY]["moved"] == 2


@pytest.mark.asyncio
async def test_a_finished_org_is_not_scanned_again():
    m, kv, _ = _migration([], {})
    kv.data[KEY] = {"done": True}
    await m.run_org("org-1")
    m._flat_documents.assert_not_awaited()


@pytest.mark.asyncio
async def test_progress_is_saved_after_each_page_and_resumed():
    m, kv, _ = _migration([_docs(*[f"v{i}" for i in range(250)])], {})
    with patch("app.connectors.services.flat_storage_migration._PAGE", 500):
        await m.run_org("org-1")
    assert kv.data[KEY]["after"] == f"{499:024x}"
    m2, _, _ = _migration([], {})
    m2.config_service = kv
    kv.data[KEY]["done"] = False
    await m2.run_org("org-1")
    assert m2._flat_documents.await_args.args[2] == f"{499:024x}"


@pytest.mark.asyncio
async def test_orphans_and_unknown_paths_are_left_in_place():
    m, kv, _ = _migration([_docs("orphan", "nopath")], {"nopath": ["r9"]})
    with patch("app.connectors.services.flat_storage_migration.build_hierarchical_storage_path",
               new_callable=AsyncMock, return_value=None), \
         patch("app.connectors.services.flat_storage_migration.post_move_tree", new_callable=AsyncMock) as move:
        await m.run_org("org-1")
    move.assert_not_awaited()
    assert kv.data[KEY]["skipped"] == 2


@pytest.mark.asyncio
async def test_shared_content_goes_under_the_record_its_copy_describes():
    m, kv, _ = _migration([_docs("v1")], {"v1": ["r-a", "r-b"]}, stored_owner="r-b")
    with patch("app.connectors.services.flat_storage_migration.build_hierarchical_storage_path",
               new_callable=AsyncMock, return_value="records/conn/B/doc"), \
         patch("app.connectors.services.flat_storage_migration.post_move_tree",
               new_callable=AsyncMock, return_value={"moved": 2}):
        await m.run_org("org-1")
    m.graph_provider.get_document.assert_awaited_with("r-b", "records")


@pytest.mark.asyncio
async def test_a_failing_org_does_not_stop_the_others():
    m, kv, _ = _migration([], {})
    m.graph_provider.get_all_orgs = AsyncMock(return_value=[{"id": "bad"}, {"id": "org-1"}])
    seen = []

    async def run_org(org_id):
        seen.append(org_id)
        if org_id == "bad":
            raise RuntimeError("storage down")

    m.run_org = run_org
    await m.run_all_orgs()
    assert seen == ["bad", "org-1"]
