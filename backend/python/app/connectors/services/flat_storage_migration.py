"""One-off move of stored copies still filed under the flat ``records/<vrid>``
layout (records indexed before hierarchical storage) into
``records/<connector>/<groups>/<record path>``.

Pattern match greps ``records/<connector>/`` only, so a flat copy is invisible
to it. Records that are indexed again are relocated by BlobStorage itself; this
covers the ones that never are. Resumable: progress is kept per org in the KV
store, one page of storage documents at a time, so a restart picks up where it
stopped and a finished org is not scanned again.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import CollectionNames
from app.config.constants.http_status_code import HttpStatusCode
from app.config.constants.service import Routes
from app.services.vector_db.membership import remaining_record_keys
from app.utils.storage_move import post_move_tree
from app.utils.storage_path import build_hierarchical_storage_path
from app.utils.time_conversion import get_epoch_timestamp_in_ms

if TYPE_CHECKING:
    import logging

    from app.config.configuration_service import ConfigurationService
    from app.modules.transformers.blob_storage import BlobStorage

PROGRESS_DIRECTORY = "/services/storageMigration/flatToHierarchical/"
_PAGE = 500


def _key(org_id: str) -> str:
    return f"{PROGRESS_DIRECTORY}{org_id}"


class FlatStorageMigration:
    def __init__(
        self,
        logger: logging.Logger,
        config_service: ConfigurationService,
        graph_provider: Any,  # noqa: ANN401 - IGraphDBProvider
        blob_storage: BlobStorage,
    ) -> None:
        self.logger = logger
        self.config_service = config_service
        self.graph_provider = graph_provider
        self.blob_storage = blob_storage

    async def run_all_orgs(self) -> None:
        for org in await self.graph_provider.get_all_orgs() or []:
            org_id = org.get("id") or org.get("_key")
            if not org_id:
                continue
            try:
                await self.run_org(str(org_id))
            except Exception as e:
                # The next run resumes from the last page that finished.
                self.logger.error("Flat storage migration for org %s stopped: %s", org_id, e)

    async def run_org(self, org_id: str) -> dict[str, Any]:
        progress = await self.config_service.get_config(_key(org_id), use_cache=False)
        if not isinstance(progress, dict):
            progress = {"after": None, "done": False, "moved": 0, "skipped": 0}
        if progress.get("done"):
            return progress
        headers, nodejs_endpoint, _ = await self.blob_storage._get_auth_and_config(org_id)
        while True:
            page = await self._flat_documents(nodejs_endpoint, headers, progress.get("after"))
            if not page:
                break
            vrids: OrderedDict[str, None] = OrderedDict()
            for doc in page:
                name = doc.get("documentName") or ""
                vrids[name.split("_", 1)[1] if "_" in name else name] = None
            for vrid in vrids:
                if await self._move_vrid(org_id, nodejs_endpoint, headers, vrid):
                    progress["moved"] = int(progress.get("moved") or 0) + 1
                else:
                    progress["skipped"] = int(progress.get("skipped") or 0) + 1
            progress["after"] = page[-1]["_id"]
            progress["updatedAt"] = get_epoch_timestamp_in_ms()
            await self.config_service.set_config(_key(org_id), progress)
            if len(page) < _PAGE:
                break
        progress["done"] = True
        await self.config_service.set_config(_key(org_id), progress)
        self.logger.info(
            "Flat storage migration for org %s finished: %d moved, %d left in place",
            org_id, progress["moved"], progress["skipped"],
        )
        return progress

    async def _flat_documents(
        self, nodejs_endpoint: str, headers: dict[str, str], after: str | None,
    ) -> list[dict[str, Any]]:
        from app.modules.transformers.blob_storage import get_shared_session

        params = {"limit": str(_PAGE)}
        if after:
            params["after"] = after
        async with get_shared_session().get(
            f"{nodejs_endpoint}{Routes.STORAGE_FLAT_RECORD_DOCUMENTS.value}",
            headers=headers, params=params,
        ) as resp:
            if resp.status != HttpStatusCode.SUCCESS.value:
                raise Exception(f"listing flat documents failed: {resp.status}")
            return list((await resp.json()).get("documents") or [])

    async def _move_vrid(
        self, org_id: str, nodejs_endpoint: str, headers: dict[str, str], vrid: str,
    ) -> bool:
        """Move one vrid's flat copy under its owner's path. False when left in place
        (no live record, path unknown, or the move failed; logged)."""
        try:
            owner_id = await self._owner_record_id(org_id, vrid)
            if not owner_id:
                return False
            doc = await self.graph_provider.get_document(owner_id, CollectionNames.RECORDS.value)
            if not doc:
                return False
            from app.events.processor import convert_record_dict_to_record

            path = await build_hierarchical_storage_path(
                convert_record_dict_to_record(doc), self.graph_provider, logger=self.logger,
            )
            if not path:
                return False
            from app.modules.transformers.blob_storage import get_shared_session

            result = await post_move_tree(
                get_shared_session(), nodejs_endpoint, headers, f"records/{vrid}", path,
                virtual_record_ids=[vrid],
            )
            return bool(result.get("moved")) and not result.get("failed")
        except Exception as e:
            self.logger.warning("Flat copy of vrid %s left in place: %s", vrid, e)
            return False

    async def _owner_record_id(self, org_id: str, vrid: str) -> str | None:
        holders = remaining_record_keys(
            await self.graph_provider.get_records_by_virtual_record_id(vrid, raise_on_error=True)
        )
        if not holders:
            return None
        if len(holders) == 1:
            return holders[0]
        # Shared content is filed under the record its stored copy describes.
        stored = await self.blob_storage.get_record_from_storage(vrid, org_id)
        owner = stored.get("id") if isinstance(stored, dict) else None
        return owner if owner in holders else holders[0]
