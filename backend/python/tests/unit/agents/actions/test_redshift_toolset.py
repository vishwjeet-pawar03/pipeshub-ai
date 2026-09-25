"""Redshift agent tools, driven through the real `RedshiftClient` and
`RedshiftDataSource`. Only `redshift_connector.connect` is replaced, by a fake
connection that records every statement and its bound parameters.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, create_autospec, patch

import pytest
import redshift_connector

from app.agents.actions.redshift.redshift import Redshift
from app.modules.transformers.blob_storage import BlobStorage
from app.sources.client.redshift import redshift as redshift_client_module
from app.sources.client.redshift.redshift import RedshiftClient
from app.utils.conversation_tasks import pop_tasks

PASSWORD = "rs-Pa55word!secret"

Responder = Callable[[str, tuple], "list[dict[str, Any]] | None"]


def _flat(query: str) -> str:
    return " ".join(query.split())


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection") -> None:
        self._conn = conn
        self.description: list[tuple] | None = None
        self.rowcount = -1
        self._rows: list[tuple] = []

    def execute(self, query: str, params: tuple | None = None) -> None:
        params = tuple(params or ())
        self._conn.calls.append((_flat(query), params))
        rows = self._conn.responder(_flat(query), params)
        if rows is None:
            self.description, self.rowcount = None, 3
            return
        columns = list(rows[0]) if rows else ["_"]
        self.description = [(c,) for c in columns]
        self._rows = [tuple(r.get(c) for c in columns) for r in rows]

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def close(self) -> None:
        return None


class _FakeConnection:
    def __init__(self, responder: Responder) -> None:
        self.responder = responder
        self.calls: list[tuple[str, tuple]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None

    def statements(self) -> list[str]:
        return [q for q, _ in self.calls]


class _Catalog:
    """Answers the catalog queries `RedshiftDataSource` issues."""

    def __init__(self, schemas: dict[str, dict[str, dict[str, Any]]], views: dict[str, list[str]] | None = None,
                 on_query: Responder | None = None) -> None:
        self.schemas = schemas
        self.views = views or {}
        self.on_query = on_query

    def _table(self, params: tuple) -> dict[str, Any]:
        return self.schemas.get(params[0], {}).get(params[1], {})

    def __call__(self, query: str, params: tuple) -> list[dict[str, Any]] | None:
        if "FROM pg_namespace" in query:
            return [{"name": s, "owner": "admin"} for s in self.schemas]
        if "information_schema.views" in query:
            return [{"name": v, "schema": params[0], "definition": "select 1"} for v in self.views.get(params[0], [])]
        if "check_constraints" in query or "constraint_type = 'UNIQUE'" in query:
            return []
        if "constraint_type = 'PRIMARY KEY'" in query:
            cols = self._table(params).get("pk", [])
            if "constraint_name" in query.split("FROM")[0]:
                return [{"constraint_name": f"{params[1]}_pkey", "column_name": c} for c in cols]
            return [{"column_name": c} for c in cols]
        if "constraint_type = 'FOREIGN KEY'" in query:
            return list(self._table(params).get("fk", []))
        if "information_schema.columns" in query:
            return [{"name": c, "column_name": c, "data_type": "integer", "is_nullable": "NO",
                     "column_default": None, "nullable": False, "not_null": True}
                    for c in self._table(params).get("columns", [])]
        if "information_schema.tables" in query and "table_name = %s" in query:
            return [{"name": params[1], "schema": params[0], "type": "BASE TABLE"}] if self._table(params) else []
        if "information_schema.tables" in query:
            return [{"name": t, "schema": params[0], "type": "BASE TABLE"} for t in self.schemas.get(params[0], {})]
        if self.on_query is not None:
            return self.on_query(query, params)
        raise AssertionError(f"unexpected statement: {query}")


WAREHOUSE = {
    "public": {
        "orders": {"columns": ["id", "customer_id"], "pk": ["id"],
                   "fk": [{"constraint_name": "fk_c", "column_name": "customer_id",
                           "foreign_table_schema": "public", "foreign_table_name": "customers",
                           "foreign_column_name": "id"}]},
        "customers": {"columns": ["id"], "pk": ["id"]},
    },
    "analytics": {"daily": {"columns": ["day"], "pk": []}},
}


@pytest.fixture
def make_tool() -> Callable[..., tuple[Redshift, _FakeConnection]]:
    patches: list[Any] = []

    def _make(responder: Responder | None = None, *, state: dict[str, Any] | None = None,
              connect_error: Exception | None = None) -> tuple[Redshift, _FakeConnection]:
        conn = _FakeConnection(responder or _Catalog(WAREHOUSE))
        p = patch.object(redshift_connector, "connect",
                         side_effect=connect_error if connect_error else (lambda **kw: conn))
        p.start()
        patches.append(p)
        client = RedshiftClient(host="wh.example.redshift.amazonaws.com", database="dev",
                                user="agent", password=PASSWORD)
        return Redshift(client, state if state is not None else {}), conn

    yield _make
    for p in patches:
        p.stop()


def _body(result: tuple[bool, str]) -> dict[str, Any]:
    return json.loads(result[1])


def test_driver_is_the_real_package_not_a_stand_in() -> None:
    assert redshift_client_module.redshift_connector is redshift_connector
    assert not isinstance(redshift_connector, MagicMock)
    assert issubclass(redshift_connector.InterfaceError, redshift_connector.Error)
    assert callable(redshift_connector.connect)


class TestSchemaTools:
    async def test_get_tables_schema_accepts_one_table_name_as_text(self, make_tool) -> None:
        tool, _ = make_tool()
        body = _body(await tool.get_tables_schema(schema_name="public", tables="orders"))
        assert [t["name"] for t in body["tables"]] == ["orders"]
