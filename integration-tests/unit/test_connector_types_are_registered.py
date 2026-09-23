"""Every connector type a suite asks for must be one the service registers.

The registry is a plain dict keyed on the name passed to @ConnectorBuilder,
matched exactly, spaces included. A suite that spells it differently gets
"Connector type ... not found in registry" -- but only once it runs, and the
suites that need a real tenant (SharePoint needs Microsoft 365) run only in the
nightly. The SharePoint suite asked for "SharePointOnline" against a connector
registered as "SharePoint Online", and nothing said so until a nightly got far
enough to try.

This reads source rather than importing it: no stack, no connector
dependencies, so it can run on every pull request.
"""

from __future__ import annotations

import ast
import difflib
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_SOURCES = _REPO / "backend" / "python" / "app" / "connectors" / "sources"
_APP = _REPO / "backend" / "python" / "app"
_SUITES = _REPO / "integration-tests"


def _parse(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return None


def _string_constants() -> dict[str, str]:
    """Module-level ``NAME = "..."`` and class-level ``Class.ATTR = "..."``.

    Some connectors register under a named constant rather than a literal
    (``OutlookConnectorNames.TEAM``); resolving those is what lets this tell a
    real registration from a missing one.
    """
    table: dict[str, str] = {}

    def record(target: ast.expr, value: ast.expr, prefix: str) -> None:
        if isinstance(target, ast.Name) and isinstance(value, ast.Constant) and isinstance(value.value, str):
            table[prefix + target.id] = value.value

    for path in _APP.rglob("*.py"):
        tree = _parse(path)
        if tree is None:
            continue
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    record(target, node.value, "")
            elif isinstance(node, ast.AnnAssign) and node.value is not None:
                record(node.target, node.value, "")
            elif isinstance(node, ast.ClassDef):
                for item in node.body:
                    if isinstance(item, ast.Assign):
                        for target in item.targets:
                            record(target, item.value, node.name + ".")
                    elif isinstance(item, ast.AnnAssign) and item.value is not None:
                        record(item.target, item.value, node.name + ".")
    return table


def _registered_names() -> tuple[set[str], list[str]]:
    """Names passed to ConnectorBuilder, and any this cannot resolve."""
    constants = _string_constants()
    names: set[str] = set()
    unresolved: list[str] = []
    for path in _SOURCES.rglob("*.py"):
        tree = _parse(path)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "ConnectorBuilder"):
                continue
            arg = node.args[0] if node.args else None
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                names.add(arg.value)
            elif arg is not None and ast.unparse(arg) in constants:
                names.add(constants[ast.unparse(arg)])
            else:
                where = f"{path.relative_to(_REPO)}:{node.lineno}"
                unresolved.append(f"{where}  ConnectorBuilder({ast.unparse(arg) if arg else ''})")
    return names, unresolved


def _requested_types() -> list[tuple[str, str]]:
    """``connector_type=`` values the suites pass, with where each is.

    Literals, and module-level string constants in the same file. A bare name
    that is not one of those is a helper forwarding its own parameter -- the
    real value is counted at the call site that supplied it.
    """
    requested: list[tuple[str, str]] = []
    for path in _SUITES.rglob("*.py"):
        if "__pycache__" in path.parts or path == Path(__file__).resolve():
            continue
        tree = _parse(path)
        if tree is None:
            continue
        module_constants = {
            target.id: node.value.value
            for node in tree.body
            if isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            for kw in node.keywords:
                if kw.arg != "connector_type":
                    continue
                value = kw.value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    literal = value.value
                elif isinstance(value, ast.Name) and value.id in module_constants:
                    literal = module_constants[value.id]
                else:
                    continue
                requested.append((literal, f"{path.relative_to(_REPO)}:{value.lineno}"))
    return requested


def test_every_registration_resolves_to_a_name() -> None:
    _, unresolved = _registered_names()
    assert not unresolved, (
        "These @ConnectorBuilder registrations pass something this guard cannot "
        "read as a string, so it cannot check the suites against them. Pass a "
        "string literal, or a module- or class-level string constant:\n  "
        + "\n  ".join(unresolved)
    )


def test_every_requested_connector_type_is_registered() -> None:
    registered, _ = _registered_names()
    requested = _requested_types()
    assert requested, "found no connector_type= in any suite -- this guard is reading the wrong place"

    unknown = [(name, where) for name, where in requested if name not in registered]
    lines = []
    for name, where in unknown:
        close = difflib.get_close_matches(name, registered, n=1, cutoff=0.6)
        hint = f' -- did you mean "{close[0]}"?' if close else ""
        lines.append(f'{where}  "{name}"{hint}')
    assert not unknown, (
        "These suites ask for a connector type the service does not register. "
        "The registry matches the @ConnectorBuilder name exactly, spaces "
        "included, so each would fail with \"not found in registry\" the first "
        "time it ran:\n  " + "\n  ".join(lines)
    )
