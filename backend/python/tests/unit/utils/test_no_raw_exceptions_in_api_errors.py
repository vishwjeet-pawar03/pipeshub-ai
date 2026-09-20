"""Stop raw exception text reaching people through API errors.

Two shapes are caught: the exception handed over on the spot (``detail=str(e)``)
and the exception parked in a variable first (``error_msg = f"…{e}"`` … then
``detail=error_msg``), which reads as a fixed message on the raising line.

A line ending ``# user-written message`` is skipped: a few handlers re-raise our
own exceptions whose text was written for the person asking.

The web app shows an API error's ``message``/``reason``/``detail`` word for word,
so ``detail=str(e)`` puts a Python repr in a toast. These files still hold a few
such spots; this test fails when a file gains one, so the count only goes down.

To lower a baseline after cleaning a file up, run this test — the failure message
prints the new count — and set it here.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

APP = Path(__file__).resolve().parents[3] / "app"

# Raw exception text handed to the caller: detail=str(e), detail=f"…{e}",
# "reason": str(e), "message": str(exc) — in either quote style.
_EXC_NAME = r"(?:e|exc|err|error|\w*_error)"
_FIELD = r"""(?:detail|["']reason["']|["']message["'])"""
_F_STRING = (
    r"f\"[^\"]*\{\s*(?:str\()?" + _EXC_NAME + r"\)?\s*\}[^\"]*\""
    r"|f'[^']*\{\s*(?:str\()?" + _EXC_NAME + r"\)?\s*\}[^']*'"
)
RAW_EXCEPTION = re.compile(
    _FIELD + r"\s*[=:]\s*(?:str\(" + _EXC_NAME + r"\)|" + _F_STRING + r")"
)

# path -> how many such spots that file is still allowed to have.
BASELINE = {
    "connectors/sources/localKB/api/kb_router.py": 0,
    "connectors/sources/localKB/api/knowledge_hub_router.py": 0,
    # Not cleaned up yet: these hand back ValueErrors that are sometimes written
    # for the reader ("Invalid npm package name") and sometimes not. Lower the
    # number as each one is given wording of its own.
    "api/routes/skills.py": 10,
    "api/routes/mcp_servers.py": 3,
    "api/routes/search.py": 1,
    "connectors/sources/localKB/handlers/kb_service.py": 0,
    "connectors/api/router.py": 0,
    "api/routes/agent.py": 0,
    "api/routes/toolsets.py": 0,
    "api/routes/entity.py": 0,
    "connectors_main.py": 0,
}


def count_raw(path: Path) -> list[str]:
    source = path.read_text(encoding="utf-8")
    lines = source.splitlines()
    found = [
        f"{path.name}:{n}: {line.strip()}"
        for n, line in enumerate(lines, 1)
        if RAW_EXCEPTION.search(line) and "user-written message" not in line
    ]
    return found + count_aliased(path.name, source, lines)


def _holds_exception_text(node: ast.AST) -> bool:
    """``str(e)`` or an f-string interpolating an exception."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "str":
        return bool(node.args) and isinstance(node.args[0], ast.Name) and _is_exc(node.args[0].id)
    if isinstance(node, ast.JoinedStr):
        for part in node.values:
            if not isinstance(part, ast.FormattedValue):
                continue
            inner = part.value
            if isinstance(inner, ast.Name) and _is_exc(inner.id):
                return True
            if _holds_exception_text(inner):
                return True
    return False


def _is_exc(name: str) -> bool:
    return bool(re.fullmatch(_EXC_NAME, name))


def count_aliased(filename: str, source: str, lines: list[str]) -> list[str]:
    """Exception text that reaches the caller through a variable.

    ``error_msg = f"…{e}"`` then ``detail=error_msg`` reads as a fixed message on
    the raising line, so the line scan above cannot see it.
    """
    hits: list[str] = []
    for func in ast.walk(ast.parse(source)):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        carriers: dict[str, list[int]] = {}
        for node in ast.walk(func):
            if not isinstance(node, ast.Assign) or not _holds_exception_text(node.value):
                continue
            if "user-written message" in lines[node.lineno - 1]:
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    carriers.setdefault(target.id, []).append(node.lineno)
        if not carriers:
            continue
        for node in ast.walk(func):
            used: list[ast.expr] = []
            if isinstance(node, ast.keyword) and node.arg == "detail":
                used.append(node.value)
            elif isinstance(node, ast.Dict):
                used += [
                    value
                    for key, value in zip(node.keys, node.values)
                    if isinstance(key, ast.Constant) and key.value in {"reason", "message"}
                ]
            for named in used:
                if not isinstance(named, ast.Name) or named.id not in carriers:
                    continue
                # the assignment that reaches this use is the closest one above it
                above = [n for n in carriers[named.id] if n <= named.lineno]
                assigned = above[-1] if above else carriers[named.id][0]
                hits.append(
                    f"{filename}:{named.lineno}: {named.id} carries the exception "
                    f"from line {assigned}: {lines[assigned - 1].strip()}"
                )
    return sorted(set(hits))


@pytest.mark.parametrize(("relative", "allowed"), sorted(BASELINE.items()))
def test_file_hands_no_new_exception_text_to_people(relative: str, allowed: int) -> None:
    found = count_raw(APP / relative)
    assert len(found) <= allowed, (
        f"{relative} now returns raw exception text to people in {len(found)} place(s), "
        f"baseline {allowed}. Use app.utils.user_messages.action_failed()/not_found() "
        f"and log the exception instead:\n  " + "\n  ".join(found)
    )


def test_exception_text_parked_in_a_variable_is_caught() -> None:
    source = (
        "async def handler():\n"
        "    try:\n"
        "        await work()\n"
        "    except Exception as e:\n"
        '        error_msg = f"Connection test failed: {str(e)}"\n'
        "        raise HTTPException(status_code=500, detail=error_msg) from e\n"
    )
    found = count_aliased("handler.py", source, source.splitlines())
    assert len(found) == 1, found
    assert "carries the exception" in found[0]


def test_a_fixed_message_in_a_variable_is_left_alone() -> None:
    source = (
        "async def handler():\n"
        "    try:\n"
        "        await work()\n"
        "    except Exception as e:\n"
        '        error_msg = action_failed("connect to this connector")\n'
        "        raise HTTPException(status_code=500, detail=error_msg) from e\n"
    )
    assert count_aliased("handler.py", source, source.splitlines()) == []


def test_the_pattern_catches_the_shapes_it_claims_to() -> None:
    caught = [
        'detail=str(e)',
        'detail=f"Unexpected error: {str(e)}"',
        '"reason": str(e)',
        '"message": str(exc),',
        'detail=f"Failed to update user roles: {str(e)}"',
        'detail=f"Failed to publish reindex event: {str(event_error)}"',
        "detail=f'Failure: {e}'",
        "'reason': str(exc),"
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
