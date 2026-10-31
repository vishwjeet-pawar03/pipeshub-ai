"""Model adapters. Each reads plan-input.json and writes plan-llm.json; any
failure is reported as a status, and the validator falls back to the floor."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from qa import paths
from qa.report import redact

MAX_INPUT_BYTES = 200_000
# Read from the engine checkout (the base branch in CI), so a PR cannot rewrite the prompt that plans it.
PROMPT = "scripts/qa/prompts/planner.md"
READ_ONLY_TOOLS = "Read,Glob,Grep,Bash(git diff:*),Bash(git log:*),Bash(git show:*),Bash(grep:*)"
FENCED = re.compile(r"```(?:json)?\s*(\{.*?\})\s*```", re.DOTALL)


def extract_json(text: str) -> dict[str, Any]:
    text = text.strip()
    try:
        value = json.loads(text)
        if isinstance(value, dict):
            return value
    except json.JSONDecodeError:
        pass
    for block in reversed(FENCED.findall(text)):
        try:
            return json.loads(block)
        except json.JSONDecodeError:
            continue
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        return json.loads(text[start : end + 1])
    raise ValueError("no JSON object in model output")


def _notes() -> str:
    # Policy: from the engine checkout like the prompt, never from the tree under test.
    return "".join(
        "\n\n## Notes for this repository\n\n" + n.read_text(encoding="utf-8")
        for n in (o / "planner_notes.md" for o in paths.overlays(paths.REPO)) if n.is_file()
    )


def _instructions(root: Path, input_path: Path) -> str:
    prompt = (paths.REPO / PROMPT).read_text(encoding="utf-8")
    prompt += _notes()
    return (
        f"{prompt}\n\n## This run\n\nRead `{input_path}` and `{input_path.parent / 'changeset.diff'}`. "
        "Reply with the JSON object only."
    )


def _none(root: Path, input_path: Path, timeout: int) -> dict[str, Any]:
    return {"schema": "qa-plan-llm/v1", "add": [], "add_matrix": [], "missing": [], "risk": {"level": "low", "reasons": []}}


def _claude_code(root: Path, input_path: Path, timeout: int) -> dict[str, Any]:
    exe = shutil.which("claude")
    if not exe:
        raise RuntimeError("claude CLI not found")
    cmd = [exe, "-p", _instructions(root, input_path), "--output-format", "json",
           "--max-turns", os.environ.get("QA_PLANNER_MAX_TURNS", "15"), "--allowedTools", READ_ONLY_TOOLS]
    if os.environ.get("QA_PLANNER_MODEL"):
        cmd += ["--model", os.environ["QA_PLANNER_MODEL"]]
    out = subprocess.run(cmd, check=False, cwd=root, capture_output=True, text=True, timeout=timeout)
    try:
        envelope = json.loads(out.stdout)
    except json.JSONDecodeError:
        envelope = None
    if out.returncode != 0 or not isinstance(envelope, dict) or envelope.get("is_error"):
        # The CLI reports max-turns and API errors in the stdout envelope, not stderr.
        detail = out.stderr.strip()
        if isinstance(envelope, dict):
            detail = " ".join(str(envelope.get(k)) for k in ("subtype", "result") if envelope.get(k)) or detail
        raise RuntimeError(f"claude exited {out.returncode}: {(detail or out.stdout.strip())[:300]}")
    return extract_json(envelope.get("result", ""))


def _command(root: Path, input_path: Path, timeout: int) -> dict[str, Any]:
    """Any agent CLI: QA_PLANNER_CMD with {prompt_file}, {input} and {output} placeholders."""
    template = os.environ.get("QA_PLANNER_CMD")
    if not template:
        raise RuntimeError("QA_PLANNER_CMD is not set")
    prompt_file = input_path.parent / "planner-prompt.md"
    prompt_file.write_text(_instructions(root, input_path), encoding="utf-8")
    output = input_path.parent / "plan-llm.raw"
    output.unlink(missing_ok=True)
    cmd = (template.replace("{prompt_file}", shlex.quote(str(prompt_file)))
           .replace("{input}", shlex.quote(str(input_path)))
           .replace("{output}", shlex.quote(str(output))))
    out = subprocess.run(cmd, check=False, shell=True, cwd=root, capture_output=True, text=True, timeout=timeout)
    if out.returncode != 0:
        raise RuntimeError(f"planner command exited {out.returncode}: {out.stderr.strip()[:300]}")
    text = output.read_text(encoding="utf-8") if output.is_file() else out.stdout
    return extract_json(text)


INLINE = (
    "\n\n## This run\n\nYou cannot read files or run commands here: plan-input.json and the diff "
    "are in the next message. The catalogs are not available, so add only ids that appear in "
    "`candidates` or `floor`, exactly as written; any other id is dropped. Reply with the JSON object only."
)


def _capped(plan_input: dict[str, Any]) -> str:
    """The floor goes as ids only (the model adds, never explains it); candidates are ranked, so the tail is cut."""
    plan_input = {**plan_input, "floor": {"tests": plan_input["floor"]["tests"]}}
    candidates, plan_input["candidates"] = plan_input.get("candidates", []), []
    # shortcut: floor ids alone can exceed MAX_INPUT_BYTES and are sent anyway; trim or summarize them if a provider rejects such a payload.
    room = MAX_INPUT_BYTES - len(json.dumps(plan_input))
    for c in candidates:
        room -= len(json.dumps(c)) + 2
        if room < 0:
            break
        plan_input["candidates"].append(c)
    return json.dumps(plan_input)


def _litellm(root: Path, input_path: Path, timeout: int) -> dict[str, Any]:
    """Any provider LiteLLM supports (OpenAI, Azure OpenAI, Gemini, Bedrock, ...), keyed by its usual env vars."""
    model = os.environ.get("QA_PLANNER_MODEL")
    if not model:
        raise RuntimeError("QA_PLANNER_MODEL is not set")
    import litellm  # optional dependency; only this adapter needs it

    prompt = (paths.REPO / PROMPT).read_text(encoding="utf-8")
    prompt += _notes()
    payload = _capped(json.loads(input_path.read_text(encoding="utf-8")))
    diff_path = input_path.parent / "changeset.diff"
    diff = diff_path.read_text(encoding="utf-8")[:60000] if diff_path.is_file() else ""
    response = litellm.completion(
        model=model,
        timeout=timeout,
        drop_params=True,
        messages=[
            {"role": "system", "content": prompt + INLINE},
            {"role": "user", "content": f"plan-input.json:\n{payload}\n\nchangeset.diff:\n{diff}"},
        ],
    )
    content = response.choices[0].message.content
    if not content:
        raise RuntimeError(f"{model} returned no text")
    return extract_json(content)


ADAPTERS: dict[str, Callable[[Path, Path, int], dict[str, Any]]] = {
    "none": _none,
    "claude_code": _claude_code,
    "command": _command,
    "litellm": _litellm,
}


def default_adapter() -> str:
    return os.environ.get("QA_PLANNER", "claude_code")


def run(root: Path, adapter: str, input_path: Path, output_path: Path, timeout: int = 600) -> dict[str, Any]:
    """Always writes output_path; the status says whether the model contributed."""
    started = time.monotonic()
    status: dict[str, Any] = {"adapter": adapter, "model": os.environ.get("QA_PLANNER_MODEL")}
    try:
        if adapter not in ADAPTERS:
            raise RuntimeError(f"unknown adapter {adapter!r}; known: {sorted(ADAPTERS)}")
        result = ADAPTERS[adapter](root, input_path, timeout)
        status["status"] = "skipped" if adapter == "none" else "ok"
    except Exception as exc:  # noqa: BLE001 - any failure means "floor only"
        result = _none(root, input_path, timeout)
        status.update(status="failed", error=redact(f"{type(exc).__name__}: {exc}")[:500])
    status["seconds"] = round(time.monotonic() - started, 1)
    result["_planner"] = status
    output_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return status
