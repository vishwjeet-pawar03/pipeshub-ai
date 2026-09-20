"""Stop raw exception text reaching people through API errors.

A line ending ``# user-written message`` is skipped: a few handlers re-raise our
own exceptions whose text was written for the person asking.

The web app shows an API error's ``message``/``reason``/``detail`` word for word,
so ``detail=str(e)`` puts a Python repr in a toast. These files still hold a few
such spots; this test fails when a file gains one, so the count only goes down.

To lower a baseline after cleaning a file up, run this test — the failure message
prints the new count — and set it here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[3] / "app"

# Raw exception text handed to the caller: detail=str(e), detail=f"…{e}",
# "reason": str(e), "message": str(exc).
RAW_EXCEPTION = re.compile(
    r"""(?:detail|"reason"|"message")\s*[=:]\s*"""
    r"""(?:str\((?:e|exc|err|error|\w*_error)\)"""
    r"""|f"[^"]*\{\s*(?:str\()?(?:e|exc|err|error|\w*_error)\)?\s*\}[^"]*")"""
)

# path -> how many such spots that file is still allowed to have.
BASELINE = {
    "connectors/sources/localKB/api/kb_router.py": 0,
    "connectors/sources/localKB/handlers/kb_service.py": 0,
    "connectors/api/router.py": 0,
    "api/routes/agent.py": 0,
    "api/routes/toolsets.py": 0,
    "api/routes/entity.py": 0,
    "connectors_main.py": 0,
}


def count_raw(path: Path) -> list[str]:
    return [
        f"{path.name}:{n}: {line.strip()}"
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if RAW_EXCEPTION.search(line) and "user-written message" not in line
    ]


@pytest.mark.parametrize(("relative", "allowed"), sorted(BASELINE.items()))
def test_file_hands_no_new_exception_text_to_people(relative: str, allowed: int) -> None:
    found = count_raw(APP / relative)
    assert len(found) <= allowed, (
        f"{relative} now returns raw exception text to people in {len(found)} place(s), "
        f"baseline {allowed}. Use app.utils.user_messages.action_failed()/not_found() "
        f"and log the exception instead:\n  " + "\n  ".join(found)
    )


def test_the_pattern_catches_the_shapes_it_claims_to() -> None:
    caught = [
        'detail=str(e)',
        'detail=f"Unexpected error: {str(e)}"',
        '"reason": str(e)',
        '"message": str(exc),',
        'detail=f"Failed to update user roles: {str(e)}"',
        'detail=f"Failed to publish reindex event: {str(event_error)}"',
    ]
    for line in caught:
        assert RAW_EXCEPTION.search(line), line
    ignored = [
        'detail=action_failed("open this file")',
        'detail=not_found("This folder")',
        # names that merely start with "e" are not exception text
        'detail=f"Cannot change auth type from {existing_auth_type} to {new_auth_type}."',
    ]
    for line in ignored:
        assert not RAW_EXCEPTION.search(line), line
