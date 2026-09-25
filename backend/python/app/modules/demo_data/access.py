"""Who sees the bundled Acme Corp demo data.

Each person decides for themselves whether the demo's records reach their
answers, search and record listings. Until they choose, the demo is on while
the organization has no data of its own, and off once real records are
indexed, so Acme Corp facts do not mix into real answers by default.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.config.constants.arangodb import Connectors, ProgressStatus

if TYPE_CHECKING:
    from app.config.configuration_service import ConfigurationService
    from app.services.graph_db.interface.graph_db_provider import IGraphDBProvider

DEMO_CONNECTOR_TYPE = "Demo"

_DEMO_IDS_TTL_S = 60.0
# "No real data yet" is asked again soon; once real data exists it stays.
_NO_REAL_DATA_TTL_S = 60.0

_demo_ids_cache: dict[str, tuple[float, tuple[str, ...]]] = {}
_real_data_cache: dict[str, float | bool] = {}


def preference_key(org_id: str, user_id: str) -> str:
    return f"/services/demoData/{org_id}/users/{user_id}"


@dataclass(frozen=True)
class DemoDataStatus:
    demo_connector_ids: tuple[str, ...]
    # The person's own choice; None until they make one.
    chosen: bool | None
    real_data: bool

    @property
    def has_demo(self) -> bool:
        return bool(self.demo_connector_ids)

    @property
    def include(self) -> bool:
        if not self.has_demo:
            return False
        if self.chosen is not None:
            return self.chosen
        return not self.real_data

    def to_dict(self) -> dict[str, Any]:
        return {
            "hasDemo": self.has_demo,
            "include": self.include,
            "chosen": self.chosen,
            "realData": self.real_data,
            "demoConnectorIds": list(self.demo_connector_ids),
        }


def _app_id(app: dict[str, Any]) -> str:
    return str(app.get("_key") or app.get("id") or "")


async def demo_connector_ids(graph_provider: IGraphDBProvider, org_id: str) -> tuple[str, ...]:
    """Ids of the org's Demo connector instances, enabled or not (their records stay searchable)."""
    cached = _demo_ids_cache.get(org_id)
    if cached and time.monotonic() - cached[0] < _DEMO_IDS_TTL_S:
        return cached[1]
    apps = await graph_provider.get_org_apps(org_id, active_only=False)
    ids = tuple(sorted(i for i in (_app_id(a) for a in apps if a.get("type") == DEMO_CONNECTOR_TYPE) if i))
    _demo_ids_cache[org_id] = (time.monotonic(), ids)
    return ids


async def org_has_real_data(graph_provider: IGraphDBProvider, org_id: str) -> bool:
    """Whether any source other than the demo has an indexed record: a connector or a Collection."""
    cached = _real_data_cache.get(org_id)
    if cached is True:
        return True
    if isinstance(cached, float) and time.monotonic() - cached < _NO_REAL_DATA_TTL_S:
        return False
    apps = await graph_provider.get_org_apps(org_id, active_only=False)
    others = [a for a in apps if a.get("type") != DEMO_CONNECTOR_TYPE and _app_id(a)]
    # Connectors first: an org usually has few, while every user owns a Collection.
    others.sort(key=lambda a: a.get("type") == Connectors.KNOWLEDGE_BASE.value)
    for app in others:
        records = await graph_provider.get_records_by_status(
            org_id, _app_id(app), [ProgressStatus.COMPLETED.value], limit=1
        )
        if records:
            _real_data_cache[org_id] = True
            return True
    _real_data_cache[org_id] = time.monotonic()
    return False


async def read_preference(config_service: ConfigurationService, org_id: str, user_id: str) -> bool | None:
    # Uncached: the setting is written by another service, and a stale read
    # would show answers the person just switched off.
    value = await config_service.get_config(preference_key(org_id, user_id), use_cache=False)
    if isinstance(value, dict) and isinstance(value.get("include"), bool):
        return value["include"]
    return None


async def write_preference(
    config_service: ConfigurationService, org_id: str, user_id: str, *, include: bool | None
) -> None:
    key = preference_key(org_id, user_id)
    if include is None:
        # Nothing saved means already on the default; deleting a missing key reports failure.
        if await read_preference(config_service, org_id, user_id) is None:
            return
        saved = await config_service.delete_config(key)
    else:
        saved = await config_service.set_config(key, {"include": include})
    if not saved:
        raise RuntimeError("could not save the demo data setting")


async def demo_data_status(
    graph_provider: IGraphDBProvider,
    config_service: ConfigurationService,
    org_id: str,
    user_id: str,
) -> DemoDataStatus:
    ids = await demo_connector_ids(graph_provider, org_id)
    if not ids:
        return DemoDataStatus(demo_connector_ids=(), chosen=None, real_data=False)
    chosen = await read_preference(config_service, org_id, user_id)
    real_data = await org_has_real_data(graph_provider, org_id)
    return DemoDataStatus(demo_connector_ids=ids, chosen=chosen, real_data=real_data)


async def excluded_demo_connector_ids(
    graph_provider: IGraphDBProvider,
    config_service: ConfigurationService,
    org_id: str,
    user_id: str,
) -> frozenset[str]:
    """Demo connector ids to leave out of what this person sees; empty when the demo is on for them."""
    if not org_id or not user_id:
        return frozenset()
    status = await demo_data_status(graph_provider, config_service, org_id, user_id)
    return frozenset() if status.include else frozenset(status.demo_connector_ids)


def clear_caches() -> None:
    _demo_ids_cache.clear()
    _real_data_cache.clear()
