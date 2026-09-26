"""Slack token rotation: rotating tokens are renewed, and nothing else is.

A Slack app with token rotation turned on gets access tokens that expire after
12 hours, each paired with a refresh token that works once. These tests run the
real connectors, the real ``TokenRefreshService`` and the real ``OAuthProvider``
against the fake workspace and a fake ``oauth.v2.access`` endpoint.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any

import pytest
from slack_behaviour_fakes import (
    BOT_TOKEN,
    USER_TOKEN,
    FakeCheckpoints,
    FakeConfigService,
    FakeSlackStore,
    SlackWorkspace,
    ts_minutes_ago,
)
from slack_behaviour_setup import (
    PERSONAL_CONNECTOR_ID,
    personal_config,
    personal_connector,
    workspace_config,
    workspace_connector,
)

from app.connectors.core.base.token_service import oauth_service
from app.connectors.core.base.token_service.startup_service import startup_service
from app.connectors.core.base.token_service.token_refresh_service import (
    TokenRefreshService,
)
from app.connectors.sources.slack.individual.connector import SlackIndividualConnector
from tests.support.slack_oauth import FakeSlackOAuth

OLD_ACCESS = "xoxe.xoxp-1-first-access"
OLD_REFRESH = "xoxe-1-first-refresh"
OAUTH_CONFIG_ID = "slack-app-1"
DM_BOB = "D0BOB"


class _Graph:
    """Only reached when a connector is switched off, which these tests never get to."""

    async def update_node(self, *_: object, **__: object) -> bool:
        raise AssertionError("connector was deactivated")


def rotating_credentials(*, issued_hours_ago: float) -> dict[str, Any]:
    return {
        "access_token": OLD_ACCESS,
        "refresh_token": OLD_REFRESH,
        "expires_in": 43200,
        "token_type": "user",
        # OAuthToken keeps naive local times, as the OAuth callback stores them.
        "created_at": (datetime.now() - timedelta(hours=issued_hours_ago)).isoformat(),  # noqa: DTZ005
    }


@pytest.fixture
def workspace(slack: SlackWorkspace) -> SlackWorkspace:
    slack.add_user("U0ALICE", "alice@acme.com", "Alice")
    slack.add_user("U0BOB", "bob@acme.com", "Bob")
    slack.add_channel(DM_BOB, "", kind="im", dm_with="U0BOB")
    slack.auth_user_id = "U0ALICE"
    slack.valid_tokens.add(OLD_ACCESS)
    return slack


@pytest.fixture
def oauth(workspace: SlackWorkspace, monkeypatch: pytest.MonkeyPatch) -> FakeSlackOAuth:
    fake = FakeSlackOAuth(OLD_REFRESH, on_issue=workspace.valid_tokens.add)
    monkeypatch.setattr(oauth_service, "ClientSession", fake.client_session)
    return fake


async def rotating_connector(
    store: FakeSlackStore,
    checkpoints: FakeCheckpoints,
    monkeypatch: pytest.MonkeyPatch,
    credentials: dict[str, Any],
) -> tuple[SlackIndividualConnector, FakeConfigService]:
    """A personal connector signed in through Slack OAuth, as the callback stores it."""
    config_service = FakeConfigService(
        PERSONAL_CONNECTOR_ID,
        {
            "auth": {"authType": "OAUTH", "oauthConfigId": OAUTH_CONFIG_ID, "connectorScope": "personal"},
            "credentials": credentials,
        },
        others={
            "/services/oauth/slack": [
                {"_id": OAUTH_CONFIG_ID, "config": {"clientId": "client-1", "clientSecret": "secret-1"}},
            ],
        },
    )
    monkeypatch.setattr(startup_service, "_token_refresh_service", TokenRefreshService(config_service, _Graph()))
    connector = SlackIndividualConnector(
        logging.getLogger("test.slack_personal"), store, checkpoints, config_service,
        PERSONAL_CONNECTOR_ID, "personal", "creator-1",
    )
    assert await connector.init() is True
    return connector, config_service


def capture_logs(caplog: pytest.LogCaptureFixture) -> None:
    """Set levels on the loggers themselves: another test may have raised the level of a parent."""
    for name in (None, "test.slack_personal", "test.slack_workspace", "connector_service"):
        caplog.set_level(logging.DEBUG, logger=name)


def _tokens_in(text: str) -> set[str]:
    return {t for t in (OLD_ACCESS, OLD_REFRESH, "renewed-1", "renewed-2") if t in text}


class TestRotatingTokenIsRenewed:
    async def test_an_expired_token_is_renewed_before_it_is_used(
        self, workspace, oauth, store, checkpoints, monkeypatch, caplog,
    ) -> None:
        workspace.valid_tokens.discard(OLD_ACCESS)
        workspace.expired_tokens.add(OLD_ACCESS)
        dm = ts_minutes_ago(10)
        workspace.post(DM_BOB, dm, "U0BOB", "hi")
        capture_logs(caplog)

        connector, _ = await rotating_connector(
            store, checkpoints, monkeypatch, rotating_credentials(issued_hours_ago=13),
        )
        await connector.run_sync()

        assert len(oauth.requests) == 1
        sent = oauth.requests[0]
        assert (sent["grant_type"], sent["refresh_token"]) == ("refresh_token", OLD_REFRESH)
        assert (sent["client_id"], sent["client_secret"]) == ("client-1", "secret-1")
        new_access, _ = oauth.issued[0]
        assert {c.token for c in workspace.calls} == {new_access}
        assert dm in store.records
        assert "Refreshing token for connector" in caplog.text
        assert _tokens_in(caplog.text) == set()

    @pytest.mark.parametrize("nested", [False, True], ids=["flat-answer", "authed-user-answer"])
    async def test_the_new_access_and_refresh_tokens_are_saved_together(
        self, workspace, store, checkpoints, monkeypatch, nested,
    ) -> None:
        oauth = FakeSlackOAuth(OLD_REFRESH, nested_under_authed_user=nested, on_issue=workspace.valid_tokens.add)
        monkeypatch.setattr(oauth_service, "ClientSession", oauth.client_session)
        workspace.valid_tokens.discard(OLD_ACCESS)
        workspace.expired_tokens.add(OLD_ACCESS)

        connector, config_service = await rotating_connector(
            store, checkpoints, monkeypatch, rotating_credentials(issued_hours_ago=13),
        )

        first_access, first_refresh = oauth.issued[0]
        assert config_service.writes
        for written in config_service.writes:
            saved = written["credentials"]
            assert (saved["access_token"], saved["refresh_token"]) == (first_access, first_refresh)
            assert saved["expires_in"] == 43200
        assert config_service.config["auth"]["oauthConfigId"] == OAUTH_CONFIG_ID

        # The next renewal must use the refresh token that was saved, since Slack
        # has already retired the first one.
        workspace.expired_tokens.add(first_access)
        assert await connector.test_connection_and_access() is True

        assert [r["refresh_token"] for r in oauth.requests] == [OLD_REFRESH, first_refresh]
        second_access, second_refresh = oauth.issued[1]
        saved = config_service.config["credentials"]
        assert (saved["access_token"], saved["refresh_token"]) == (second_access, second_refresh)

    async def test_a_refused_token_is_renewed_once_and_the_call_is_retried(
        self, workspace, oauth, store, checkpoints, monkeypatch,
    ) -> None:
        connector, _ = await rotating_connector(
            store, checkpoints, monkeypatch, rotating_credentials(issued_hours_ago=1),
        )
        assert oauth.requests == []
        workspace.valid_tokens.discard(OLD_ACCESS)
        calls_before = len(workspace.calls)

        assert await connector.test_connection_and_access() is True

        new_access, _ = oauth.issued[0]
        assert [(c.method, c.token) for c in workspace.calls[calls_before:]] == [
            ("auth.test", OLD_ACCESS), ("auth.test", new_access),
        ]
        assert len(oauth.requests) == 1

    async def test_a_token_refused_right_after_renewal_is_not_renewed_again(
        self, workspace, oauth, store, checkpoints, monkeypatch,
    ) -> None:
        connector, _ = await rotating_connector(
            store, checkpoints, monkeypatch, rotating_credentials(issued_hours_ago=1),
        )
        workspace.valid_tokens.discard(OLD_ACCESS)
        assert await connector.test_connection_and_access() is True
        new_access, _ = oauth.issued[0]

        workspace.valid_tokens.discard(new_access)
        assert await connector.test_connection_and_access() is False
        await connector.run_sync()

        assert len(oauth.requests) == 1

    async def test_two_calls_that_find_the_token_expired_share_one_refresh(
        self, workspace, oauth, store, checkpoints, monkeypatch,
    ) -> None:
        connector, _ = await rotating_connector(
            store, checkpoints, monkeypatch, rotating_credentials(issued_hours_ago=1),
        )
        workspace.valid_tokens.discard(OLD_ACCESS)
        workspace.expired_tokens.add(OLD_ACCESS)
        both_refused = asyncio.Event()
        oauth.hold = both_refused
        calls_before = len(workspace.calls)

        def count_refusals(_: dict[str, str]) -> None:
            if sum(1 for c in workspace.calls[calls_before:] if c.token == OLD_ACCESS) >= 2:
                both_refused.set()

        workspace.on_call("auth.test", count_refusals)

        results = await asyncio.gather(
            connector.test_connection_and_access(), connector.test_connection_and_access(),
        )

        assert results == [True, True]
        assert len(oauth.requests) == 1
        new_access, _ = oauth.issued[0]
        tokens = [c.token for c in workspace.calls[calls_before:]]
        assert sorted(tokens) == sorted([OLD_ACCESS, OLD_ACCESS, new_access, new_access])


class TestRenewalThatCannotWork:
    async def test_a_revoked_refresh_token_asks_for_a_reconnect_and_is_not_retried(
        self, workspace, oauth, store, checkpoints, monkeypatch, caplog,
    ) -> None:
        oauth.live_refresh_token = "xoxe-1-issued-after-a-reinstall"
        connector, config_service = await rotating_connector(
            store, checkpoints, monkeypatch, rotating_credentials(issued_hours_ago=1),
        )
        workspace.valid_tokens.discard(OLD_ACCESS)
        workspace.expired_tokens.add(OLD_ACCESS)
        capture_logs(caplog)

        assert await connector.test_connection_and_access() is False
        assert await connector.test_connection_and_access() is False
        await connector.run_sync()

        assert len(oauth.requests) == 1
        assert "Reconnect Slack" in caplog.text
        assert config_service.config["credentials"]["refresh_token"] == OLD_REFRESH
        assert _tokens_in(caplog.text) == set()


class TestTokensThatDoNotRotate:
    async def test_a_classic_user_token_never_triggers_a_refresh(
        self, workspace, oauth, store, checkpoints, monkeypatch,
    ) -> None:
        connector, _ = await personal_connector(store, checkpoints, personal_config(token=USER_TOKEN))
        workspace.valid_tokens.discard(USER_TOKEN)

        assert await connector.test_connection_and_access() is False
        await connector.run_sync()

        assert oauth.requests == []

    async def test_a_classic_bot_token_never_triggers_a_refresh(
        self, workspace, oauth, store, checkpoints,
    ) -> None:
        connector, _ = await workspace_connector(store, checkpoints, workspace_config(token=BOT_TOKEN))
        workspace.expired_tokens.add(BOT_TOKEN)

        assert await connector.test_connection_and_access() is False
        await connector.run_sync()

        assert oauth.requests == []

    async def test_an_expired_pasted_rotating_token_explains_what_to_do(
        self, workspace, oauth, store, checkpoints, caplog,
    ) -> None:
        pasted = "xoxe.xoxb-1-pasted-access"
        workspace.valid_tokens.add(pasted)
        connector, _ = await workspace_connector(store, checkpoints, workspace_config(token=pasted))
        workspace.valid_tokens.discard(pasted)
        workspace.expired_tokens.add(pasted)
        capture_logs(caplog)

        assert await connector.test_connection_and_access() is False
        await connector.run_sync()

        assert oauth.requests == []
        explained = [r for r in caplog.records if "token rotation" in r.getMessage()]
        assert len(explained) == 1
        assert "paste" in explained[0].getMessage()
        assert pasted not in caplog.text
