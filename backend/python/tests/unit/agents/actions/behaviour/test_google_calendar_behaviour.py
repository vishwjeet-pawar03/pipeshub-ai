"""Behaviour tests for the Google Calendar agent tools.

Each test drives a tool the way the agent does and checks what Google would
receive and what the agent is told back. See ``gcal_behaviour_fakes`` for what
is real and what is faked.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from gcal_behaviour_fakes import (
    ACCESS_TOKEN,
    FakeGoogleHttp,
    GoogleResponse,
    build_calendar_tool,
    google_error,
    result,
)

from app.agents.actions.google.calendar.calendar import (
    CreateCalendarEventInput,
    GoogleCalendar,
)

EVENTS = "/calendars/primary/events"


@pytest.fixture
def http() -> FakeGoogleHttp:
    return FakeGoogleHttp()


@pytest.fixture
def cal(http: FakeGoogleHttp) -> GoogleCalendar:
    return build_calendar_tool(http)


def instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def assert_safe_error(payload: dict[str, Any]) -> str:
    """The error is plain text the agent can relay: no secrets, no raw library dump."""
    message = payload["error"]
    assert isinstance(message, str) and message
    for leaked in (ACCESS_TOKEN, "Bearer", "HttpError", "googleapis.com", "<"):
        assert leaked not in message, f"{leaked!r} leaked into: {message}"
    return message


def created_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "id": "evt-1",
        "summary": "Design review",
        "start": {"dateTime": "2026-09-30T10:00:00Z"},
        "end": {"dateTime": "2026-09-30T11:00:00Z"},
        "organizer": {"email": "me@example.com"},
        "attendees": [{"email": "a@example.com"}],
    }
    event.update(overrides)
    return event


# ---------------------------------------------------------------------------
# get_calendar_events
# ---------------------------------------------------------------------------


class TestGetCalendarEvents:
    async def test_filters_reach_google_as_query_parameters_on_the_users_primary_calendar(self, cal, http) -> None:
        http.on("GET", EVENTS, {"items": [{"id": "e1", "summary": "Standup", "location": "Room 1"}, {"id": "e2"}]})

        ok, data = result(await cal.get_calendar_events(
            max_results=5, time_min="2026-09-30T00:00:00Z", time_max="2026-10-01T00:00:00Z",
            order_by="startTime", single_events=True, query="standup", show_deleted=False, time_zone="UTC",
        ))

        assert ok is True
        request = http.calls("GET", EVENTS)[0]
        query = dict(request.query)
        assert instant(query.pop("timeMin")) == instant("2026-09-30T00:00:00Z")
        assert instant(query.pop("timeMax")) == instant("2026-10-01T00:00:00Z")
        assert query == {
            "maxResults": "5", "orderBy": "startTime", "singleEvents": "true", "q": "standup",
            "showDeleted": "false", "timeZone": "UTC", "alt": "json",
        }
        assert request.headers["authorization"] == f"Bearer {ACCESS_TOKEN}"
        assert [e["location"] for e in data["items"]] == ["Room 1", ""]

    async def test_named_calendar_and_unset_filters_are_not_sent(self, cal, http) -> None:
        http.on("GET", "/calendars/team@group.calendar.google.com/events", {"items": []})

        ok, _ = result(await cal.get_calendar_events(calendar_id="team@group.calendar.google.com"))

        assert ok is True
        assert http.calls("GET")[0].query == {"alt": "json"}

    async def test_next_page_token_is_passed_back_to_the_agent(self, cal, http) -> None:
        http.on("GET", EVENTS, {"items": [{"id": "e1"}], "nextPageToken": "page-2"})

        _, data = result(await cal.get_calendar_events(max_results=1))

        assert data["nextPageToken"] == "page-2"

    async def test_time_without_offset_is_sent_as_rfc3339_in_the_requested_zone(self, cal, http) -> None:
        # Google rejects timeMin/timeMax without an offset, so a bare local time must gain one.
        http.on("GET", EVENTS, {"items": []})

        ok, _ = result(await cal.get_calendar_events(
            time_min="2026-09-30T09:00:00", time_max="2026-09-30T18:00:00", time_zone="Asia/Kolkata",
        ))

        assert ok is True
        query = http.calls("GET", EVENTS)[0].query
        assert instant(query["timeMin"]) == instant("2026-09-30T09:00:00+05:30")
        assert instant(query["timeMax"]) == instant("2026-09-30T18:00:00+05:30")

    async def test_unreadable_time_is_refused_before_calling_google(self, cal, http) -> None:
        ok, data = result(await cal.get_calendar_events(time_min="next tuesday"))

        assert ok is False
        message = assert_safe_error(data)
        assert "next tuesday" in message and "ISO" in message
        assert http.requests == []

# ---------------------------------------------------------------------------
# create_calendar_event
# ---------------------------------------------------------------------------


class TestCreateCalendarEvent:
    async def test_creates_event_on_primary_calendar_and_invites_attendees(self, cal, http) -> None:
        http.on("POST", EVENTS, created_event())

        ok, data = result(await cal.create_calendar_event(
            event_start_time="2026-09-30T10:00:00Z", event_end_time="2026-09-30T11:00:00Z",
            event_title="Design review", event_description="Agenda", event_location="Room 1",
            event_attendees_emails=["a@example.com", "b@example.com"],
        ))

        assert ok is True
        post = http.calls("POST", EVENTS)[0]
        assert post.query["sendUpdates"] == "all"
        assert post.body["summary"] == "Design review"
        assert post.body["description"] == "Agenda"
        assert post.body["location"] == "Room 1"
        assert post.body["attendees"] == [{"email": "a@example.com"}, {"email": "b@example.com"}]
        assert instant(post.body["start"]["dateTime"]) == instant("2026-09-30T10:00:00Z")
        assert instant(post.body["end"]["dateTime"]) == instant("2026-09-30T11:00:00Z")
        assert data["event_id"] == "evt-1"
        assert data["event_title"] == "Design review"
        assert data["event_start_time"] == "2026-09-30T10:00:00Z"

    async def test_time_with_offset_keeps_its_instant(self, cal, http) -> None:
        http.on("POST", EVENTS, created_event())

        await cal.create_calendar_event(event_start_time="2026-09-30T10:00:00+05:30", event_end_time="2026-09-30T11:00:00+05:30")

        body = http.calls("POST", EVENTS)[0].body
        assert instant(body["start"]["dateTime"]) == instant("2026-09-30T04:30:00Z")

    async def test_local_time_is_read_in_the_requested_timezone(self, cal, http) -> None:
        # "10:00" for a user in India is 10:00 IST, not 10:00 on the server's clock.
        http.on("POST", EVENTS, created_event())

        ok, _ = result(await cal.create_calendar_event(
            event_start_time="2026-09-30T10:00:00", event_end_time="2026-09-30T11:00:00",
            event_timezone="Asia/Kolkata",
        ))

        assert ok is True
        body = http.calls("POST", EVENTS)[0].body
        assert instant(body["start"]["dateTime"]) == instant("2026-09-30T10:00:00+05:30")
        assert instant(body["end"]["dateTime"]) == instant("2026-09-30T11:00:00+05:30")
        assert body["start"]["timeZone"] == "Asia/Kolkata"
        assert body["end"]["timeZone"] == "Asia/Kolkata"

    async def test_unix_timestamp_is_accepted_as_the_parameter_promises(self, cal, http) -> None:
        http.on("POST", EVENTS, created_event())
        start = int(datetime(2026, 9, 30, 10, tzinfo=timezone.utc).timestamp())

        ok, _ = result(await cal.create_calendar_event(event_start_time=str(start), event_end_time=str(start + 3600)))

        assert ok is True
        body = http.calls("POST", EVENTS)[0].body
        assert instant(body["start"]["dateTime"]) == instant("2026-09-30T10:00:00Z")
        assert instant(body["end"]["dateTime"]) == instant("2026-09-30T11:00:00Z")

    async def test_unknown_timezone_is_refused_before_calling_google(self, cal, http) -> None:
        ok, data = result(await cal.create_calendar_event(
            event_start_time="2026-09-30T10:00:00", event_end_time="2026-09-30T11:00:00", event_timezone="Mars/Olympus",
        ))

        assert ok is False
        message = assert_safe_error(data)
        assert "Mars/Olympus" in message and "America/New_York" in message
        assert http.requests == []

    async def test_unreadable_date_is_refused_before_calling_google(self, cal, http) -> None:
        ok, data = result(await cal.create_calendar_event(event_start_time="tomorrow at 3", event_end_time="2026-09-30T11:00:00Z"))

        assert ok is False
        assert "tomorrow at 3" in assert_safe_error(data)
        assert http.requests == []

    async def test_end_before_start_is_refused_before_calling_google(self, cal, http) -> None:
        ok, data = result(await cal.create_calendar_event(event_start_time="2026-09-30T11:00:00Z", event_end_time="2026-09-30T10:00:00Z"))

        assert ok is False
        assert "end after it starts" in assert_safe_error(data)
        assert http.requests == []

    async def test_missing_start_time_is_refused(self, cal, http) -> None:
        ok, data = result(await cal.create_calendar_event(event_start_time="", event_end_time="2026-09-30T11:00:00Z"))

        assert ok is False
        assert "start time" in data["error"].lower()
        assert http.requests == []

    async def test_all_day_event_uses_the_dates_the_user_gave(self, cal, http) -> None:
        # Midnight in India is the previous evening in UTC; the event must still land on the 30th.
        http.on("POST", EVENTS, created_event(start={"date": "2026-09-30"}, end={"date": "2026-10-01"}))

        ok, data = result(await cal.create_calendar_event(
            event_start_time="2026-09-30T00:00:00+05:30", event_end_time="2026-10-01T00:00:00+05:30", event_all_day=True,
        ))

        assert ok is True
        body = http.calls("POST", EVENTS)[0].body
        assert body["start"] == {"date": "2026-09-30"}
        assert body["end"] == {"date": "2026-10-01"}
        assert data["event_start_time"] == "2026-09-30"
        assert data["event_end_time"] == "2026-10-01"
        assert data["event_all_day"] is True

    async def test_single_day_all_day_event_ends_the_next_day(self, cal, http) -> None:
        # Google's all-day end date is exclusive; start == end would be rejected as an empty range.
        http.on("POST", EVENTS, created_event(start={"date": "2026-09-30"}, end={"date": "2026-10-01"}))

        ok, _ = result(await cal.create_calendar_event(event_start_time="2026-09-30", event_end_time="2026-09-30", event_all_day=True))

        assert ok is True
        body = http.calls("POST", EVENTS)[0].body
        assert body["start"] == {"date": "2026-09-30"}
        assert body["end"] == {"date": "2026-10-01"}

    @pytest.mark.xfail(strict=True, reason=(
        "A meeting link passed by the agent is sent as a Meet createRequest id without "
        "conferenceDataVersion=1, so Google drops it and the event has no link. Fixing it means "
        "choosing between storing the given URL or generating a Meet room; left for a product call."
    ))
    async def test_meeting_link_given_by_the_agent_ends_up_on_the_event(self, cal, http) -> None:
        http.on("POST", EVENTS, created_event())

        await cal.create_calendar_event(
            event_start_time="2026-09-30T10:00:00Z", event_end_time="2026-09-30T11:00:00Z",
            event_meeting_link="https://zoom.us/j/123",
        )

        post = http.calls("POST", EVENTS)[0]
        assert post.query.get("conferenceDataVersion") == "1" or "https://zoom.us/j/123" in str(post.body.get("location"))

    def test_attendee_objects_from_an_earlier_tool_are_reduced_to_emails(self) -> None:
        parsed = CreateCalendarEventInput(
            event_start_time="s", event_end_time="e",
            event_attendees_emails=[{"email": "a@example.com", "responseStatus": "accepted"}, {"emailAddress": "b@example.com"}, "c@example.com"],
        )
        assert parsed.event_attendees_emails == ["a@example.com", "b@example.com", "c@example.com"]

    @pytest.mark.parametrize("raw, expected", [
        ("['a@example.com', 'b@example.com']", ["a@example.com", "b@example.com"]),
        ("a@example.com", ["a@example.com"]),
        ([{"responseStatus": "accepted"}], None),
        (None, None),
    ])
    def test_attendee_strings_are_coerced(self, raw, expected) -> None:
        assert CreateCalendarEventInput(event_start_time="s", event_end_time="e", event_attendees_emails=raw).event_attendees_emails == expected


# ---------------------------------------------------------------------------
# update_calendar_event
# ---------------------------------------------------------------------------


class TestUpdateCalendarEvent:
    async def test_reads_then_writes_back_the_merged_event_with_notifications(self, cal, http) -> None:
        http.on("GET", f"{EVENTS}/evt-1", created_event(description="old"))
        http.on("PUT", f"{EVENTS}/evt-1", created_event(summary="Renamed"))

        ok, data = result(await cal.update_calendar_event(
            event_id="evt-1", event_title="Renamed", event_location="Room 2", event_attendees_emails=["z@example.com"],
        ))

        assert ok is True
        put = http.calls("PUT", f"{EVENTS}/evt-1")[0]
        assert put.query["sendUpdates"] == "all"
        assert put.body["summary"] == "Renamed"
        assert put.body["description"] == "old"
        assert put.body["location"] == "Room 2"
        assert put.body["attendees"] == [{"email": "z@example.com"}]
        assert put.body["start"] == {"dateTime": "2026-09-30T10:00:00Z"}
        assert data["success"] is True and data["event_title"] == "Renamed"

    async def test_moving_an_event_reads_local_times_in_the_requested_timezone(self, cal, http) -> None:
        http.on("GET", f"{EVENTS}/evt-1", created_event())
        http.on("PUT", f"{EVENTS}/evt-1", created_event())

        ok, _ = result(await cal.update_calendar_event(
            event_id="evt-1", event_start_time="2026-10-01T15:00:00", event_end_time="2026-10-01T16:00:00",
            event_timezone="America/New_York",
        ))

        assert ok is True
        body = http.calls("PUT")[0].body
        assert instant(body["start"]["dateTime"]) == instant("2026-10-01T15:00:00-04:00")
        assert body["start"]["timeZone"] == "America/New_York"

# ---------------------------------------------------------------------------
# create_meet_link
# ---------------------------------------------------------------------------


class TestCreateMeetLink:
    async def test_asks_google_to_generate_a_meet_room_and_returns_its_link(self, cal, http) -> None:
        http.on("POST", EVENTS, created_event(summary="Meeting", conferenceData={"entryPoints": [
            {"entryPointType": "phone", "uri": "tel:+1"}, {"entryPointType": "video", "uri": "https://meet.google.com/abc"},
        ]}))

        ok, data = result(await cal.create_meet_link(event_start_time="2026-09-30T10:00:00Z", event_end_time="2026-09-30T11:00:00Z"))

        assert ok is True
        post = http.calls("POST", EVENTS)[0]
        assert post.query["conferenceDataVersion"] == "1"
        assert post.query["sendUpdates"] == "all"
        assert post.body["summary"] == "Meeting"
        assert post.body["conferenceData"]["createRequest"]["conferenceSolutionKey"] == {"type": "hangoutsMeet"}
        assert data["meet_link"] == "https://meet.google.com/abc"

    async def test_falls_back_to_hangout_link(self, cal, http) -> None:
        http.on("POST", EVENTS, created_event(hangoutLink="https://meet.google.com/old"))

        _, data = result(await cal.create_meet_link(event_start_time="2026-09-30T10:00:00Z", event_end_time="2026-09-30T11:00:00Z"))

        assert data["meet_link"] == "https://meet.google.com/old"

    async def test_local_time_is_read_in_the_requested_timezone(self, cal, http) -> None:
        http.on("POST", EVENTS, created_event())

        await cal.create_meet_link(event_start_time="2026-09-30T10:00:00", event_end_time="2026-09-30T10:30:00", event_timezone="Europe/Berlin")

        body = http.calls("POST", EVENTS)[0].body
        assert instant(body["start"]["dateTime"]) == instant("2026-09-30T10:00:00+02:00")
        assert body["start"]["timeZone"] == "Europe/Berlin"

    async def test_missing_end_time_is_refused(self, cal, http) -> None:
        ok, _ = result(await cal.create_meet_link(event_start_time="2026-09-30T10:00:00Z", event_end_time=""))

        assert ok is False
        assert http.requests == []

# ---------------------------------------------------------------------------
# delete, calendar list
# ---------------------------------------------------------------------------


class TestDeleteCalendarEvent:
    async def test_deletes_from_primary_calendar(self, cal, http) -> None:
        http.on("DELETE", f"{EVENTS}/evt-1", GoogleResponse(204))

        ok, data = result(await cal.delete_calendar_event(event_id="evt-1"))

        assert ok is True
        assert "evt-1" in data["message"]
        assert len(http.calls("DELETE", f"{EVENTS}/evt-1")) == 1

class TestCalendarList:
    async def test_lists_the_users_calendars(self, cal, http) -> None:
        http.on("GET", "/users/me/calendarList", {"items": [{"id": "primary", "summary": "Me"}]})

        ok, data = result(await cal.get_calendar_list())

        assert ok is True
        assert data["items"][0]["summary"] == "Me"

    async def test_gets_one_calendar_defaulting_to_primary(self, cal, http) -> None:
        http.on("GET", "/calendars/primary", {"id": "primary", "timeZone": "UTC"})

        ok, data = result(await cal.get_calendar_list_by_id())

        assert ok is True
        assert data["timeZone"] == "UTC"

