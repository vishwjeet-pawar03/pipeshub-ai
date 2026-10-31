"""Run history: one JSON line per run. Feeds "failing on main", flaky rates and
measured durations back into selection, and is what miss attribution reads."""

from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def record(store: Path, plan: dict[str, Any], results: dict[str, Any], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    entry = {
        "ts": int(time.time()),
        "head": plan.get("head"),
        "base": plan.get("base"),
        "branch": plan.get("branch"),
        "mode": plan.get("mode"),
        "profile": plan.get("profile"),
        "edition": plan.get("edition"),
        "planner": {k: plan.get("planner", {}).get(k) for k in ("adapter", "status", "model")},
        "selected": {t["id"]: t["source"] for t in plan["tests"]},
        "outcomes": {t["id"]: t["outcome"] for t in results["tests"]},
        "seconds": {t["id"]: t["seconds"] for t in results["tests"] if t.get("seconds")},
        **(extra or {}),
    }
    store.parent.mkdir(parents=True, exist_ok=True)
    with store.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")
    return entry


def load(store: Path) -> list[dict[str, Any]]:
    if not store.is_file():
        return []
    out = []
    for line in store.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def failing_on_main(entries: Iterable[dict[str, Any]], branch: str = "main", last: int = 3) -> list[str]:
    runs = [e for e in entries if e.get("branch") == branch][-last:]
    return sorted({tid for e in runs for tid, o in e.get("outcomes", {}).items() if o in ("fail", "error")})


def durations(entries: Iterable[dict[str, Any]]) -> dict[str, float]:
    total: dict[str, list[float]] = defaultdict(list)
    for e in entries:
        for tid, sec in e.get("seconds", {}).items():
            total[tid].append(float(sec))
    return {tid: round(sum(v[-10:]) / len(v[-10:]), 2) for tid, v in total.items()}


def flaky_rates(entries: Iterable[dict[str, Any]]) -> dict[str, float]:
    runs: Counter[str] = Counter()
    flakes: Counter[str] = Counter()
    for e in entries:
        for tid, o in e.get("outcomes", {}).items():
            if o in ("pass", "fail", "flaky"):
                runs[tid] += 1
                flakes[tid] += o == "flaky"
    return {tid: round(flakes[tid] / n, 3) for tid, n in runs.items() if flakes[tid]}


def misses(entries: list[dict[str, Any]], failing: Iterable[str], culprit_head: str) -> list[str]:
    """Failing tests the culprit run did not select: selection misses."""
    run = next((e for e in reversed(entries) if e.get("head") == culprit_head), None)
    if run is None:
        return []
    return sorted(t for t in failing if t not in run.get("selected", {}))
