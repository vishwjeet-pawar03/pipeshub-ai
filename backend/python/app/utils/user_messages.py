"""Plain-language messages for API failures people see in PipesHub.

The web app shows an API error's ``message``/``reason``/``detail`` word for word in
a toast (``frontend/lib/api/api-error.ts``), so these strings must say what failed
in the user's terms and what to do next. Exception text never goes there: it names
internals and is often a repr. Callers log the exception and return one of these.

For a file that failed to index, use :mod:`app.utils.user_errors` instead — those
reasons carry indexing-specific next steps ("Try Reindex …").
"""

from __future__ import annotations

RETRY_HINT = "Please try again; if it keeps failing, contact your admin."

SOMETHING_WENT_WRONG = f"Something went wrong. {RETRY_HINT}"


PEOPLE_GONE = (
    "Some people you picked are no longer in this workspace. "
    "Remove them and try sharing again."
)


def action_failed(action: str) -> str:
    """Why an action failed, in the user's words.

    ``action`` completes "We couldn't …", e.g. ``"delete this folder"``.
    """
    return f"We couldn't {action}. {RETRY_HINT}"


def not_found(thing: str) -> str:
    """A thing the caller asked for is gone or was never theirs.

    Says nothing about whether it exists, so it is safe for records another
    workspace owns. ``thing`` starts the sentence, e.g. ``"This folder"``.
    """
    return f"{thing} was removed, or you no longer have access. Refresh the page and try again."
