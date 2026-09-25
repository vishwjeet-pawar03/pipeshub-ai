"""Behaviour tests for the Slack agent tools.

Each test drives a tool the way the agent does and checks what Slack would
receive and what the agent is told back. See ``slack_behaviour_fakes`` for
what is real and what is faked.
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

import pytest
from slack_behaviour_fakes import (
    ME,
    USER_TOKEN,
    FakeSlackApi,
    build_slack_tool,
    rate_limited,
    result,
    slack_error,
    user,
)

if TYPE_CHECKING:
    from app.agents.actions.slack.slack import Slack

GENERAL = "C0GENERAL1"
RANDOM = "C0RANDOM01"
DM = "D0DMCHAN01"
ANN = user("U0ANNAAAAA", "Ann", "ann@example.com")
JOANNA = user("U0JOANNA00", "Joanna Park", "joanna@example.com")
SAM = user("U0SAMAAAAA", "Sam", "sam@example.com")
SAMANTHA = user("U0SAMANTHA", "Samantha Lee", "samantha@example.com")


@pytest.fixture
def api() -> FakeSlackApi:
    return FakeSlackApi()


@pytest.fixture
def slack(api: FakeSlackApi) -> Slack:
    return build_slack_tool(api)


def failure(outcome: tuple[bool, str]) -> dict[str, Any]:
    """A failed call, checked to be safe to show: no token, no raw SDK dump."""
    ok, payload = outcome
    assert ok is False, payload
    for leaked in (USER_TOKEN, "Bearer", "The server responded with", "slack.com/api"):
        assert leaked not in payload, f"{leaked!r} leaked into: {payload}"
    data = json.loads(payload)
    assert data["success"] is False
    return data


def explanation(data: dict[str, Any]) -> str:
    """What the agent will relay: the human message, else the error text."""
    return data.get("message") or data["error"]


def members_page(members: list[dict], next_cursor: str = "") -> dict[str, Any]:
    return {"members": members, "response_metadata": {"next_cursor": next_cursor}}


# ---------------------------------------------------------------------------
# Sending to channels
# ---------------------------------------------------------------------------


class TestSendMessage:
    async def test_posts_markdown_as_mrkdwn_with_the_users_token(self, slack, api) -> None:
        api.on("chat.postMessage", {"channel": GENERAL, "ts": "1700000000.000100"})

        ok, data = result(await slack.send_message(GENERAL, "**Release** is out, see [notes](https://x.test)"))

        assert ok is True
        call = api.called("chat.postMessage")[0]
        assert call.args == {"channel": GENERAL, "text": "*Release* is out, see <https://x.test|notes>", "mrkdwn": True}
        assert call.headers["authorization"] == f"Bearer {USER_TOKEN}"
        assert data["data"]["ts"] == "1700000000.000100"

    async def test_channel_name_is_resolved_across_conversation_pages(self, slack, api) -> None:
        api.on("conversations.list",
               {"channels": [{"id": RANDOM, "name": "random"}], "response_metadata": {"next_cursor": "c2"}},
               {"channels": [{"id": GENERAL, "name": "general"}], "response_metadata": {"next_cursor": ""}})
        api.on("chat.postMessage", {"channel": GENERAL, "ts": "1.1"})

        ok, _ = result(await slack.send_message("#general", "hi"))

        assert ok is True
        pages = api.called("conversations.list")
        assert [p.args.get("cursor") for p in pages] == [None, "c2"]
        assert api.called("chat.postMessage")[0].args["channel"] == GENERAL

    async def test_rate_limit_says_how_long_to_wait(self, slack, api) -> None:
        api.on("chat.postMessage", rate_limited(retry_after=12))

        data = failure(await slack.send_message(GENERAL, "hi"))

        assert data["error"] == "ratelimited"
        assert data["retry_after"] == "12"
        message = explanation(data)
        assert "12 seconds" in message and "try again" in message.lower()

    @pytest.mark.parametrize("code", ["invalid_auth", "token_revoked", "not_authed"])
    async def test_rejected_sign_in_asks_the_user_to_reconnect(self, slack, api, code) -> None:
        api.on("chat.postMessage", slack_error(code))

        data = failure(await slack.send_message(GENERAL, "hi"))

        assert data["error"] == code
        assert "Reconnect the Slack toolset" in explanation(data)

    async def test_missing_permission_asks_to_reconnect_and_approve(self, slack, api) -> None:
        api.on("chat.postMessage", slack_error("missing_scope", needed="chat:write"))

        data = failure(await slack.send_message(GENERAL, "hi"))

        message = explanation(data)
        assert "permission" in message and "Reconnect the Slack toolset" in message
        assert "xoxb" not in message and "bot" not in message.lower()

    async def test_not_in_channel_speaks_to_the_user_not_a_bot(self, slack, api) -> None:
        api.on("chat.postMessage", slack_error("not_in_channel"))

        data = failure(await slack.send_message(GENERAL, "hi"))

        assert data["error"] == "not_in_channel"
        message = explanation(data)
        assert "not a member" in message and "bot" not in message.lower()

    async def test_unknown_channel_points_to_fetch_channels(self, slack, api) -> None:
        api.on("chat.postMessage", slack_error("channel_not_found"))

        data = failure(await slack.send_message(GENERAL, "hi"))

        assert data["error"] == "channel_not_found"
        assert "fetch_channels" in explanation(data)

    async def test_uncommon_error_code_is_explained_without_the_sdk_dump(self, slack, api) -> None:
        api.on("chat.postMessage", slack_error("is_archived"))

        data = failure(await slack.send_message(GENERAL, "hi"))

        assert data["error"] == "is_archived"
        assert "archived" in explanation(data)

    async def test_slack_outage_is_reported_as_temporary(self, slack, api) -> None:
        api.on("chat.postMessage", slack_error("fatal_error", status=500))

        data = failure(await slack.send_message(GENERAL, "hi"))

        assert "try again" in explanation(data).lower()


class TestSendToMultipleChannels:
    async def test_each_channel_gets_the_message_and_failures_are_per_channel(self, slack, api) -> None:
        api.on("chat.postMessage", lambda args: {"channel": args["channel"], "ts": "1.1"} if args["channel"] == GENERAL else slack_error("channel_not_found"))

        ok, data = result(await slack.send_message_to_multiple_channels([GENERAL, RANDOM], "hi"))

        assert ok is False
        results = {r["channel"]: r for r in data["data"]["results"]}
        assert results[GENERAL]["success"] is True
        assert results[RANDOM]["success"] is False
        assert results[RANDOM]["error"] == "channel_not_found"

class TestReplyAndSchedule:
    async def test_reply_to_latest_message_threads_under_it(self, slack, api) -> None:
        api.on("conversations.history", {"messages": [{"ts": "1700000000.000200", "text": "q?"}]})
        api.on("chat.postMessage", {"ts": "1700000001.000000"})

        ok, _ = result(await slack.reply_to_message(GENERAL, "answer", latest_message=True))

        assert ok is True
        assert api.called("conversations.history")[0].args["limit"] == "1"
        assert api.called("chat.postMessage")[0].args["thread_ts"] == "1700000000.000200"

    async def test_reply_without_a_thread_is_refused(self, slack, api) -> None:
        data = failure(await slack.reply_to_message(GENERAL, "answer"))

        assert "thread" in data["error"].lower()
        assert api.called("chat.postMessage") == []

    async def test_schedule_converts_the_time_to_epoch_seconds(self, slack, api) -> None:
        api.on("chat.scheduleMessage", {"scheduled_message_id": "Q1", "post_at": 1790762400})

        ok, data = result(await slack.schedule_message(GENERAL, "standup", "2026-09-30T10:00:00Z"))

        assert ok is True
        assert api.called("chat.scheduleMessage")[0].args["post_at"] == 1790762400
        assert data["data"]["post_at_date"] == "2026-09-30T10:00:00Z"

    async def test_schedule_rejects_an_unreadable_time_before_calling_slack(self, slack, api) -> None:
        data = failure(await slack.schedule_message(GENERAL, "standup", "tomorrow morning"))

        assert "tomorrow morning" in data["error"]
        assert api.calls == []

    async def test_schedule_in_the_past_is_explained(self, slack, api) -> None:
        api.on("chat.scheduleMessage", slack_error("time_in_past"))

        data = failure(await slack.schedule_message(GENERAL, "standup", "2020-01-01T00:00:00Z"))

        assert "future" in explanation(data)


# ---------------------------------------------------------------------------
# Reading channels
# ---------------------------------------------------------------------------


class TestChannelHistory:
    async def test_time_window_and_limit_reach_slack_and_authors_get_names(self, slack, api) -> None:
        api.on("conversations.history", {"messages": [{"ts": "1777000000.000100", "user": ANN["id"], "text": "hi <@U0SAMAAAAA>"}], "has_more": False})
        api.on("users.info", lambda args: {"user": ANN if args["user"] == ANN["id"] else SAM})

        ok, data = result(await slack.get_channel_history(GENERAL, limit=20, oldest="2026-04-30", latest="2026-05-01T00:00:00Z"))

        assert ok is True
        args = api.called("conversations.history")[0].args
        assert args["limit"] == "20"
        assert args["oldest"] == "1777507200.000000"
        assert args["latest"] == "1777593600.000000"
        message = data["data"]["messages"][0]
        assert message["user_display_name"] == "Ann"
        assert "@Sam" in message["resolved_text"]

    async def test_unreadable_date_is_refused_before_calling_slack(self, slack, api) -> None:
        data = failure(await slack.get_channel_history(GENERAL, oldest="last week"))

        assert "last week" in data["error"]
        assert api.calls == []

    async def test_history_failure_is_reported(self, slack, api) -> None:
        api.on("conversations.history", slack_error("not_in_channel"))

        data = failure(await slack.get_channel_history(GENERAL))

        assert data["error"] == "not_in_channel"

    async def test_channel_info(self, slack, api) -> None:
        api.on("conversations.info", {"channel": {"id": GENERAL, "name": "general", "creator": ANN["id"]}})
        api.on("users.info", {"user": ANN})

        ok, data = result(await slack.get_channel_info(GENERAL))

        assert ok is True
        assert data["data"]["channel"]["name"] == "general"


# ---------------------------------------------------------------------------
# Direct messages and who gets them
# ---------------------------------------------------------------------------


class TestDirectMessages:
    async def test_email_is_looked_up_and_the_dm_channel_opened(self, slack, api) -> None:
        api.on("users.lookupByEmail", {"user": ANN})
        api.on("conversations.open", {"channel": {"id": DM}})
        api.on("chat.postMessage", {"channel": DM, "ts": "1.1"})

        ok, _ = result(await slack.send_direct_message("ann@example.com", "hello"))

        assert ok is True
        assert api.called("users.lookupByEmail")[0].args["email"] == "ann@example.com"
        assert api.called("conversations.open")[0].args["users"] == ANN["id"]
        assert api.called("chat.postMessage")[0].args["channel"] == DM

    async def test_exact_name_is_messaged(self, slack, api) -> None:
        api.on("users.list", members_page([SAMANTHA, SAM]))
        api.on("conversations.open", {"channel": {"id": DM}})
        api.on("chat.postMessage", {"ts": "1.1"})

        ok, _ = result(await slack.send_direct_message("Sam", "hello"))

        assert ok is True
        assert api.called("conversations.open")[0].args["users"] == SAM["id"]

    async def test_ambiguous_name_sends_nothing(self, slack, api) -> None:
        api.on("users.list", members_page([SAMANTHA, user("U0SAMPATEL", "Samuel Patel")]))

        data = failure(await slack.send_direct_message("Sam", "hello"))

        assert "Multiple users" in data["error"]
        assert api.called("chat.postMessage") == []

    async def test_email_nobody_has_is_not_matched_to_someone_by_name(self, slack, api) -> None:
        # sam@partner.test is not in the workspace; the message must not go to "Sam".
        api.on("users.lookupByEmail", slack_error("users_not_found"))
        api.on("users.list", members_page([SAM, ANN]))

        data = failure(await slack.send_direct_message("sam@partner.test", "contract attached"))

        assert "not found" in data["error"]
        assert api.called("conversations.open") == []
        assert api.called("chat.postMessage") == []

    async def test_enterprise_grid_user_id_is_messaged_directly(self, slack, api) -> None:
        api.on("conversations.open", {"channel": {"id": DM}})
        api.on("chat.postMessage", {"ts": "1.1"})

        ok, _ = result(await slack.send_direct_message("W0ENTGRID1", "hello"))

        assert ok is True
        assert api.called("users.list") == []
        assert api.called("conversations.open")[0].args["users"] == "W0ENTGRID1"

    async def test_longer_name_is_not_matched_to_a_shorter_directory_name(self, slack, api) -> None:
        # "Joanna" contains "ann"; that must not make Ann the recipient.
        api.on("users.list", members_page([ANN]))

        data = failure(await slack.send_direct_message("Joanna", "your review"))

        assert "not found" in data["error"]
        assert api.called("chat.postMessage") == []

    async def test_directory_failure_is_not_reported_as_unknown_user(self, slack, api) -> None:
        api.on("users.list", rate_limited(retry_after=20))

        data = failure(await slack.send_direct_message("Joanna Park", "hello"))

        message = explanation(data)
        assert "not found" not in data["error"]
        assert "20 seconds" in message
        assert api.called("chat.postMessage") == []

    async def test_email_lookup_failure_is_not_reported_as_unknown_user(self, slack, api) -> None:
        api.on("users.lookupByEmail", slack_error("missing_scope"))

        data = failure(await slack.get_dm_history("ann@example.com"))

        assert data["error"] == "missing_scope"
        assert "Reconnect the Slack toolset" in explanation(data)
        assert api.called("conversations.open") == []

    @pytest.mark.xfail(strict=True, reason=(
        "A single partial name match ('Sam' -> 'Samantha Lee') is messaged without asking. "
        "The Teams tool asks for confirmation instead; changing Slack to match is a product call."
    ))
    async def test_single_partial_match_is_confirmed_before_messaging(self, slack, api) -> None:
        api.on("users.list", members_page([SAMANTHA, ANN]))
        api.on("conversations.open", {"channel": {"id": DM}})
        api.on("chat.postMessage", {"ts": "1.1"})

        await slack.send_direct_message("Sam", "your review is due")

        assert api.called("chat.postMessage") == []

    async def test_dm_history_reads_the_opened_dm_channel(self, slack, api) -> None:
        api.on("users.lookupByEmail", {"user": ANN})
        api.on("conversations.open", {"channel": {"id": DM}})
        api.on("conversations.history", {"messages": []})

        ok, _ = result(await slack.get_dm_history("ann@example.com", limit=5))

        assert ok is True
        assert api.called("conversations.history")[0].args == {"channel": DM, "limit": "5"}

    async def test_mentions_resolve_to_user_ids(self, slack, api) -> None:
        api.on("users.lookupByEmail", {"user": ANN})
        api.on("chat.postMessage", {"ts": "1.1"})

        ok, _ = result(await slack.send_message_with_mentions(GENERAL, "ping @ann@example.com", mentions=["ann@example.com"]))

        assert ok is True
        assert api.called("chat.postMessage")[0].args["text"] == f"ping <@{ANN['id']}>"


# ---------------------------------------------------------------------------
# Listing: pagination and limits
# ---------------------------------------------------------------------------


class TestListing:
    async def test_without_limit_every_page_is_read(self, slack, api) -> None:
        api.on("users.list", members_page([ANN], "c2"), members_page([SAM]))

        ok, data = result(await slack.get_users_list())

        assert ok is True
        assert data["data"]["count"] == 2

    async def test_first_page_failure_is_a_failure(self, slack, api) -> None:
        api.on("users.list", slack_error("missing_scope"))

        failure(await slack.get_users_list())

    async def test_user_channels_are_listed_for_the_signed_in_user(self, slack, api) -> None:
        api.on("auth.test", {"user_id": ME})
        api.on("users.conversations", {"channels": [{"id": GENERAL, "name": "general"}], "response_metadata": {"next_cursor": ""}})

        ok, data = result(await slack.get_user_channels())

        assert ok is True
        assert api.called("users.conversations")[0].args["user"] == ME
        assert data["data"]["count"] == 1

    async def test_signed_in_user_unknown_is_a_failure(self, slack, api) -> None:
        api.on("auth.test", slack_error("invalid_auth"))

        failure(await slack.get_user_channels())

        assert api.called("users.conversations") == []


# ---------------------------------------------------------------------------
# Status, search, message edits
# ---------------------------------------------------------------------------


class TestStatusSearchAndEdits:
    async def test_status_expiry_is_now_plus_duration(self, slack, api) -> None:
        api.on("users.profile.set", {"profile": {}})
        before = int(time.time())

        ok, _ = result(await slack.set_user_status("In a meeting", "calendar", duration_seconds=3600))

        assert ok is True
        args = api.called("users.profile.set")[0].args
        assert args["profile"] == {"status_text": "In a meeting", "status_emoji": ":calendar:"}
        assert before + 3600 <= args["status_expiration"] <= int(time.time()) + 3600

    async def test_clearing_status_blanks_text_and_emoji(self, slack, api) -> None:
        api.on("users.profile.set", {"profile": {}})

        await slack.set_user_status("", "")

        args = api.called("users.profile.set")[0].args
        assert args["profile"] == {"status_text": "", "status_emoji": ""}
        assert args["status_expiration"] == 0

    async def test_search_builds_modifiers_from_a_person_and_dates(self, slack, api) -> None:
        api.on("users.lookupByEmail", {"user": ANN})
        api.on("users.info", {"user": ANN})
        api.on("search.messages", {"messages": {"matches": []}})

        ok, _ = result(await slack.search_messages("launch", channel="#general", from_user="ann@example.com", after="2026-01-01", count=5))

        assert ok is True
        args = api.called("search.messages")[0].args
        assert args["query"] == "in:general after:2026-01-01 from:@ann launch"
        assert args["count"] == "5"

    async def test_editing_someone_elses_message_is_explained(self, slack, api) -> None:
        api.on("chat.update", slack_error("cant_update_message"))

        data = failure(await slack.update_message(GENERAL, "1.1", "edited"))

        assert "your own" in explanation(data)

    async def test_reacting_twice_is_explained(self, slack, api) -> None:
        api.on("reactions.add", slack_error("already_reacted"))

        data = failure(await slack.add_reaction(GENERAL, "1.1", "thumbsup"))

        assert "already" in explanation(data)
