"""GET /api/v1/toolsets/tools and GET /api/v1/toolsets/registry/{toolsetName}/tools
driven through a real FastAPI app, so `Depends(require_scopes(OAuthScopes.CONNECTOR_READ))`
actually runs.

Both routes are declared with ``dependencies=[Depends(require_scopes(OAuthScopes.CONNECTOR_READ))]``
(see app/api/routes/toolsets.py, `get_all_tools` and `get_toolset_tools`). A test that calls the
handler function directly (as the rest of the suite does for these two handlers) never resolves
FastAPI dependencies, so it cannot prove the scope check is wired to the route. These tests go
through the app's middleware stack and routing instead.

Covers: backend/python/app/api/middlewares/auth.py
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from app.api.routes import toolsets as routes
from app.config.constants.service import OAuthScopes

if TYPE_CHECKING:
    from collections.abc import Iterator

ORG, USER = "org-1", "user-1"
PREFIX = "/api/v1/toolsets"


class _Registry:
    """The registry calls `get_all_tools` / `get_toolset_tools` make."""

    def list_toolsets(self) -> list[str]:
        return ["jira"]

    def get_toolset_metadata(self, toolset_type: str) -> dict[str, Any]:
        return {
            "name": toolset_type, "display_name": toolset_type.title(), "isInternal": False,
            "tools": [{"name": "search", "description": "Search issues", "parameters": [], "tags": []}],
        }


@pytest.fixture
def client() -> Iterator[TestClient]:
    app = FastAPI()
    app.state.toolset_registry = _Registry()

    @app.middleware("http")
    async def authenticate(request: Request, call_next: Any) -> Any:  # noqa: ANN401
        if "x-anonymous" not in request.headers:
            request.state.user = {"userId": USER, "orgId": ORG}
            if "x-oauth-scope" in request.headers:
                request.state.user |= {"isOAuth": True, "oauthScopes": [request.headers["x-oauth-scope"]]}
        return await call_next(request)

    app.include_router(routes.router)
    yield TestClient(app, raise_server_exceptions=False)


@pytest.mark.parametrize("path", [f"{PREFIX}/tools", f"{PREFIX}/registry/jira/tools"])
def test_registry_tool_routes_without_a_user_are_401(client: TestClient, path: str) -> None:
    resp = client.get(path, headers={"x-anonymous": "1"})
    assert resp.status_code == 401


@pytest.mark.parametrize("path", [f"{PREFIX}/tools", f"{PREFIX}/registry/jira/tools"])
def test_registry_tool_routes_let_a_session_user_through(client: TestClient, path: str) -> None:
    """A non-OAuth (session) token is not scope-restricted."""
    resp = client.get(path)
    assert resp.status_code == 200, resp.text


@pytest.mark.parametrize("path", [f"{PREFIX}/tools", f"{PREFIX}/registry/jira/tools"])
def test_registry_tool_routes_reject_an_oauth_token_without_connector_read(client: TestClient, path: str) -> None:
    resp = client.get(path, headers={"x-oauth-scope": "toolset:write"})
    assert resp.status_code == 403, resp.text


@pytest.mark.parametrize("path", [f"{PREFIX}/tools", f"{PREFIX}/registry/jira/tools"])
def test_registry_tool_routes_accept_an_oauth_token_with_connector_read(client: TestClient, path: str) -> None:
    resp = client.get(path, headers={"x-oauth-scope": OAuthScopes.CONNECTOR_READ})
    assert resp.status_code == 200, resp.text
