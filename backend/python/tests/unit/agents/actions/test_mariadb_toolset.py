"""MariaDB agent tools, driven through the real `MariaDBClient` and
`MariaDBDataSource`. Only the driver's connection pool is replaced, by a fake
that records every statement and its bound parameters.
"""

from __future__ import annotations

import importlib
import json
import logging
import sys
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, create_autospec, patch

import pytest

from app.agents.actions.mariadb.mariadb import MariaDB
from app.modules.transformers.blob_storage import BlobStorage
from app.sources.client.mariadb import mariadb as mariadb_client_module
from app.sources.client.mariadb.mariadb import MariaDBClient
from app.utils.conversation_tasks import pop_tasks

if TYPE_CHECKING:
    from types import ModuleType


def _import_real_driver() -> ModuleType:
    # tests/unit/sources/client/test_mariadb_client.py swaps a MagicMock into
    # sys.modules["mariadb"] and into the client module for the whole session.
    current = sys.modules.get("mariadb")
    if current is not None and not isinstance(current, MagicMock):
        return current
    sys.modules.pop("mariadb", None)
    try:
        return importlib.import_module("mariadb")
    finally:
        if current is not None:
            sys.modules["mariadb"] = current


mariadb_driver = _import_real_driver()

PASSWORD = "s3cr3t-Pa55word!"
DATABASE = "shop"

Responder = Callable[[str, tuple], "list[dict[str, Any]] | None"]


def _flat(query: str) -> str:
    return " ".join(query.split())


class _FakeCursor:
    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool
        self.description: list[tuple] | None = None
        self.rowcount = 0
        self._rows: list[dict[str, Any]] = []

    async def execute(self, query: str, params: tuple = ()) -> None:
        params = tuple(params)
        self._pool.calls.append((_flat(query), params))
        rows = self._pool.responder(_flat(query), params)
        if rows is None:
            self.description = None
            self.rowcount = 1
            return
        self.description = [(k,) for k in (rows[0] if rows else {"_": None})]
        self._rows = rows

    async def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)

    async def close(self) -> None:
        return None


class _FakeConnection:
    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool
        self.commit = AsyncMock()
        self.rollback = AsyncMock()

    def cursor(self, dictionary: bool = False) -> _FakeCursor:
        return _FakeCursor(self._pool)


class _FakePool:
    def __init__(self, responder: Responder) -> None:
        self.responder = responder
        self.calls: list[tuple[str, tuple]] = []
        self.connections: list[_FakeConnection] = []

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[_FakeConnection]:
        conn = _FakeConnection(self)
        self.connections.append(conn)
        yield conn

    async def close(self) -> None:
        return None

    def statements(self) -> list[str]:
        return [q for q, _ in self.calls]


class _Catalog:
    """Answers the information_schema queries `MariaDBDataSource` issues."""

    def __init__(self, tables: dict[str, dict[str, Any]], views: list[str] | None = None,
                 on_query: Responder | None = None) -> None:
        self.tables = tables
        self.views = views or []
        self.on_query = on_query

    def __call__(self, query: str, params: tuple) -> list[dict[str, Any]] | None:
        if query.startswith("USE "):
            return None
        if query.startswith("SHOW CREATE TABLE"):
            return [{"Table": "t", "Create Table": f"CREATE TABLE ... /* {query} */"}]
        if "information_schema.VIEWS" in query:
            return [{"name": v, "database": params[0], "definition": "select 1"} for v in self.views]
        if "CONSTRAINT_TYPE = 'PRIMARY KEY'" in query:
            return [{"column_name": c} for c in self.tables.get(params[1], {}).get("pk", [])]
        if "CONSTRAINT_TYPE = 'FOREIGN KEY'" in query:
            return list(self.tables.get(params[1], {}).get("fk", []))
        if "CONSTRAINT_TYPE = 'UNIQUE'" in query or "CHECK_CONSTRAINTS" in query:
            return []
        if "information_schema.COLUMNS" in query:
            return [{"name": c, "data_type": "int", "nullable": 0}
                    for c in self.tables.get(params[1], {}).get("columns", [])]
        if "information_schema.TABLES" in query and "TABLE_NAME = ?" in query:
            name = params[1]
            return [{"name": name, "database": params[0], "type": "BASE TABLE"}] if name in self.tables else []
        if "information_schema.TABLES" in query:
            return [{"name": n, "database": params[0], "type": "BASE TABLE"} for n in self.tables]
        if self.on_query is not None:
            return self.on_query(query, params)
        raise AssertionError(f"unexpected statement: {query}")


SHOP = {
    "orders": {"columns": ["id", "customer_id"], "pk": ["id"],
               "fk": [{"constraint_name": "fk_c", "column_name": "customer_id",
                       "foreign_database": DATABASE, "foreign_table_name": "customers",
                       "foreign_column_name": "id"}]},
    "customers": {"columns": ["id", "email"], "pk": ["id"], "fk": []},
}


@pytest.fixture
def make_tool() -> Callable[..., tuple[MariaDB, _FakePool]]:
    patches: list[Any] = []

    def _make(responder: Responder | None = None, *, database: str | None = DATABASE,
              state: dict[str, Any] | None = None, pool_error: Exception | None = None,
              ) -> tuple[MariaDB, _FakePool]:
        pool = _FakePool(responder or _Catalog(SHOP))
        create_pool = AsyncMock(side_effect=pool_error) if pool_error else AsyncMock(return_value=pool)
        for p in (patch.object(mariadb_client_module, "mariadb", mariadb_driver),
                  patch.object(mariadb_driver, "create_async_pool", create_pool)):
            p.start()
            patches.append(p)
        client = MariaDBClient(host="db.internal", user="agent", password=PASSWORD,
                               database=database, port=3306)
        return MariaDB(client, state if state is not None else {}), pool

    yield _make
    for p in patches:
        p.stop()


def _body(result: tuple[bool, str]) -> dict[str, Any]:
    return json.loads(result[1])


def test_driver_is_the_real_package_not_a_stand_in(make_tool) -> None:
    make_tool()
    assert mariadb_client_module.mariadb is mariadb_driver
    assert not isinstance(mariadb_driver, MagicMock)
    assert "site-packages" in mariadb_driver.__file__
    assert issubclass(mariadb_driver.OperationalError, mariadb_driver.Error)
    assert callable(mariadb_driver.create_async_pool)


class TestSchemaTools:
    async def test_get_tables_schema_accepts_one_table_name_as_text(self, make_tool) -> None:
        tool, pool = make_tool()
        body = _body(await tool.get_tables_schema(tables="orders"))

        assert [t["name"] for t in body["tables"]] == ["orders"]
        assert all(params[1:] in ((), ("orders",)) for _, params in pool.calls)
