"""Unit tests for the scale and stress runs.

The runs themselves need a stack. What can be checked here is everything that
would quietly produce wrong numbers: batched generation that drifts from the
whole-corpus one, memory that grows with the corpus, a trend that misses a
slowdown, and overload verdicts that call a lost file a pass.
"""

from __future__ import annotations

import json
import sys
import threading
import tracemalloc
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "perf"))

import bench_scale  # noqa: E402
import bench_stress  # noqa: E402
import compare  # noqa: E402
from corpus import generate_corpus, iter_corpus_batches, plan_corpus  # noqa: E402
from scale_metrics import (  # noqa: E402
    Sample,
    Verdict,
    drift,
    latency_windows,
    overload_verdicts,
    render_verdicts,
    throughput_windows,
)

pytestmark = pytest.mark.unit

TEXT_KINDS = ("txt", "md")  # fast to render, and enough to compare the two paths


class FakeState:
    """The run state the benchmarks read, without a stack to fill it."""

    def __init__(self) -> None:
        self.uploaded_at: dict[str, float] = {}
        self.file_of: dict[str, object] = {}
        self.finished_at: dict[str, float] = {}
        self.status: dict[str, str] = {}
        self.reason: dict[str, str] = {}
        self.poll_errors = 0
        self.ended_at = 0.0
        self.stopped_early = ""
        self.upload_failures: list[dict[str, str]] = []
        self.lock = threading.Lock()


def seeded_state(count: int, *, t0: float = 0.0, seconds_each: float = 30.0,
                 status: str = "COMPLETED") -> FakeState:
    state = FakeState()
    for i in range(count):
        record = f"rec-{i}"
        state.uploaded_at[record] = t0 + i
        state.finished_at[record] = t0 + i + seconds_each
        state.status[record] = status
    state.ended_at = t0 + count + seconds_each
    return state


# --- the corpus, in batches ------------------------------------------------


def test_batches_are_the_same_corpus_as_one_go() -> None:
    whole = generate_corpus(60, seed=7, salt="x", kinds=TEXT_KINDS)
    batched = [f for c in iter_corpus_batches(60, 7, seed=7, salt="x", kinds=TEXT_KINDS) for f in c.files]

    assert [f.name for f in whole.files] == [f.name for f in batched]
    assert [f.folder for f in whole.files] == [f.folder for f in batched]
    assert [f.content for f in whole.files] == [f.content for f in batched]


def test_every_batch_carries_the_whole_folder_tree() -> None:
    plan = plan_corpus(60, seed=7, kinds=TEXT_KINDS)
    for batch in iter_corpus_batches(60, 7, seed=7, kinds=TEXT_KINDS, plan=plan):
        # A file late in the run can sit in a folder planned at the start.
        assert batch.folders == plan.folders


def test_a_plan_describes_the_corpus_without_building_it() -> None:
    plan = plan_corpus(500, seed=7, kinds=TEXT_KINDS)
    described = plan.describe()

    assert described["docs"] == 500
    assert set(described["by_kind"]) <= set(TEXT_KINDS)
    assert described["planned_bytes"] > 0
    assert all(not hasattr(e, "content") for e in plan.entries)


def test_batch_size_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        list(iter_corpus_batches(10, 0, seed=1, kinds=TEXT_KINDS))


def test_memory_stays_flat_however_many_documents_are_asked_for() -> None:
    """The point of batching: 4x the documents must not mean 4x the memory."""
    def peak_batched(docs: int) -> int:
        tracemalloc.reset_peak()
        for batch in iter_corpus_batches(docs, 25, seed=5, kinds=TEXT_KINDS):
            sum(len(f.content) for f in batch.files)
        return tracemalloc.get_traced_memory()[1]

    tracemalloc.start()
    try:
        small = peak_batched(500)
        large = peak_batched(2000)
        tracemalloc.reset_peak()
        whole = generate_corpus(2000, seed=5, kinds=TEXT_KINDS)
        assert whole.total_bytes > 0
        whole_peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()

    assert large < small * 2, f"batched memory grew {large / small:.1f}x for 4x the documents"
    assert large < whole_peak / 2, "batching should cost far less than building the corpus in one go"


# --- the shape of a long run ----------------------------------------------


def test_throughput_is_reported_per_slice_including_an_idle_one() -> None:
    samples = [
        Sample(0, 0, 0), Sample(60, 100, 60),      # 60 in the first minute
        Sample(120, 200, 120), Sample(180, 300, 120),  # then nothing at all
    ]
    windows = throughput_windows(samples, windows=3)

    assert [w["records_finished"] for w in windows] == [60, 60, 0]
    assert windows[0]["records_per_minute"] == 60
    assert windows[-1]["records_per_minute"] == 0
    assert windows[-1]["backlog_at_end"] == 180


def test_latency_is_reported_per_slice() -> None:
    finished = [(10, 5.0), (20, 5.0), (80, 50.0), (100, 70.0)]
    windows = latency_windows(finished, windows=2)

    assert windows[0]["p50_seconds"] == 5.0
    assert windows[-1]["p50_seconds"] == 60.0


def test_a_run_that_slowed_to_a_crawl_is_called_out() -> None:
    # 600 records in the first ten minutes, 100 in the next, with files queued
    # the whole time — so the slowdown is real, not the run finishing up.
    samples = [Sample(0, 1000, 0), Sample(600, 1000, 600), Sample(1200, 1000, 700)]

    verdict = drift(throughput_windows(samples, windows=2), latency_windows([]), samples)

    assert verdict["steady"] is False
    assert verdict["throughput_change"] < -0.3
    joined = " ".join(verdict["notes"])
    assert "slowed while files were still queued" in joined and "records a minute" in joined


def test_a_run_finishing_its_queue_is_not_called_a_slowdown() -> None:
    """The tail of any completed run is idle; that must not read as a regression."""
    samples = [Sample(0, 400, 0), Sample(60, 400, 200), Sample(120, 400, 400), Sample(180, 400, 400)]

    verdict = drift(throughput_windows(samples, windows=3), latency_windows([]), samples)

    assert verdict["steady"] is True, verdict["notes"]


def test_files_waiting_behind_a_growing_queue_are_not_called_slower() -> None:
    """Queueing explains a longer time-to-indexed; only flag it when it does not."""
    growing = [Sample(0, 100, 0), Sample(600, 900, 200), Sample(1200, 1800, 400)]
    latency = latency_windows([(60, 20.0), (1100, 200.0)], windows=2)

    assert drift(throughput_windows(growing, windows=2), latency, growing)["latency_p50_change"] > 0.5
    assert not any("took longer" in n for n in drift(throughput_windows(growing, 2), latency, growing)["notes"])

    steady_queue = [Sample(0, 100, 0), Sample(600, 300, 200), Sample(1200, 500, 400)]
    notes = drift(throughput_windows(steady_queue, windows=2), latency, steady_queue)["notes"]
    assert any("took longer" in n for n in notes)


def test_a_steady_run_says_so() -> None:
    samples = [Sample(i * 60, i * 100, i * 100, None, 500.0) for i in range(5)]
    throughput = throughput_windows(samples, windows=4)
    latency = latency_windows([(i * 60, 30.0) for i in range(1, 5)], windows=4)

    verdict = drift(throughput, latency, samples)

    assert verdict["steady"] is True
    assert verdict["notes"] == []
    assert verdict["memory"]["growth_mb"] == 0


def test_memory_that_climbs_all_run_is_called_out() -> None:
    samples = [Sample(i * 600, i * 100, i * 100, None, 400.0 + i * 300) for i in range(5)]
    verdict = drift(throughput_windows(samples), latency_windows([]), samples)

    assert verdict["steady"] is False
    note = " ".join(verdict["notes"])
    assert "memory grew" in note and "leak" in note
    assert verdict["memory"]["growth_mb"] == 1200
    assert verdict["memory"]["growth_mb_per_hour"] == 1800


def test_memory_is_not_judged_when_it_was_never_measured() -> None:
    samples = [Sample(i * 60, i * 10, i * 10) for i in range(4)]
    verdict = drift(throughput_windows(samples), latency_windows([]), samples)

    assert verdict["memory"]["growth_mb"] is None
    assert verdict["steady"] is True


# --- overload --------------------------------------------------------------


def _verdict(verdicts: list[Verdict], fragment: str) -> Verdict:
    return next(v for v in verdicts if fragment in v.check)


def test_overload_passes_when_refusals_are_explicit_and_nothing_is_lost() -> None:
    verdicts = overload_verdicts(
        attempted=500, uploaded=480, upload_failures=20, listed=480, terminal=480,
        peak_backlog=300, recovery_seconds=120.0, rejected=20,
    )

    assert all(v.passed for v in verdicts)
    assert "asking us to slow down" in _verdict(verdicts, "accepted or refused").detail


def test_overload_fails_when_an_accepted_file_never_appears() -> None:
    verdicts = overload_verdicts(
        attempted=100, uploaded=100, upload_failures=0, listed=97, terminal=97,
        peak_backlog=80, recovery_seconds=30.0,
    )

    appears = _verdict(verdicts, "appears in the knowledge base")
    assert appears.passed is False
    assert "3 missing" in appears.detail


def test_overload_fails_when_uploads_vanish_without_an_error() -> None:
    verdicts = overload_verdicts(
        attempted=100, uploaded=90, upload_failures=0, listed=90, terminal=90,
        peak_backlog=50, recovery_seconds=10.0,
    )

    assert _verdict(verdicts, "accepted or refused").passed is False


def test_overload_fails_when_files_are_still_in_flight_at_the_end() -> None:
    verdicts = overload_verdicts(
        attempted=100, uploaded=100, upload_failures=0, listed=100, terminal=94,
        peak_backlog=60, recovery_seconds=None,
    )

    assert _verdict(verdicts, "finished, one way or another").passed is False
    cleared = _verdict(verdicts, "backlog cleared")
    assert cleared.passed is False
    assert "never cleared" in cleared.detail


def test_verdicts_render_as_a_readable_table() -> None:
    table = render_verdicts([Verdict("Nothing was lost", False, "3 missing")])

    assert "| Nothing was lost | ❌ no | 3 missing |" in table


def test_backpressure_refusals_are_told_apart_from_breakage() -> None:
    failures = [
        {"file": "a.txt", "error": "429 Client Error: Too Many Requests"},
        {"file": "b.txt", "error": "503 Server Error: Service Unavailable"},
        {"file": "c.txt", "error": "Connection reset by peer"},
    ]

    assert bench_stress.counted_rejections(failures) == 2


def test_recovery_is_measured_from_the_last_upload() -> None:
    samples = [Sample(0, 0, 0), Sample(100, 300, 100), Sample(160, 300, 300), Sample(200, 300, 300)]

    assert bench_stress.recovery_seconds(samples, 100.0) == 60.0
    assert bench_stress.recovery_seconds([Sample(0, 10, 0)], 0.0) is None
    assert bench_stress.recovery_seconds(samples, None) is None


# --- the results the workflow saves ---------------------------------------


def scale_args(**overrides: object):
    args = bench_scale.build_parser().parse_args(["--label", "unit-test", "--docs", "10"])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_a_scale_result_carries_the_totals_and_the_shape_of_the_run() -> None:
    state = seeded_state(10, seconds_each=30.0)
    samples = [Sample(i * 60, min(10, i * 4), min(10, i * 3), 900.0, 500.0) for i in range(5)]

    result = bench_scale.build_result(
        scale_args(), plan_corpus(10, seed=1, kinds=TEXT_KINDS), state, 0.0, samples,
        {"llm": "openai/gpt-4o-mini", "embedding": "local"},
        __import__("datetime").datetime.now(__import__("datetime").timezone.utc), 1234, "",
    )

    assert result["benchmark"] == "scale"
    assert result["metrics"]["records_completed"] == 10
    assert result["metrics"]["failures"]["total"] == 0
    assert result["trend"]["throughput_windows"]
    assert result["trend"]["steady"] is True, result["trend"]["notes"]
    assert result["corpus"]["generated_bytes"] == 1234
    # Same metric names as the indexing benchmark, so compare.py can read both.
    for key in ("wall_seconds", "records_per_minute", "time_to_indexed_seconds", "peak_indexing_rss_mb"):
        assert key in result["metrics"]


def test_a_scale_result_can_be_compared_with_a_baseline_of_itself() -> None:
    """The result and the comparison must not drift apart."""
    state = seeded_state(10)
    samples = [Sample(i * 60, 10, min(10, i * 3), None, 500.0) for i in range(4)]
    result = bench_scale.build_result(
        scale_args(), plan_corpus(10, seed=1, kinds=TEXT_KINDS), state, 0.0, samples,
        {"llm": "openai/gpt-4o-mini", "embedding": "local"},
        __import__("datetime").datetime.now(__import__("datetime").timezone.utc), 10, "",
    )
    baseline = json.loads(json.dumps(result))

    rows, mismatches = compare.compare(baseline, result)

    assert mismatches == []
    assert rows and not any(r.regressed for r in rows)


def test_a_scale_run_that_stopped_early_says_so_in_its_summary() -> None:
    state = seeded_state(4)
    state.stopped_early = "the run hit its 60s limit with 6 file(s) still indexing"
    result = bench_scale.build_result(
        scale_args(), plan_corpus(10, seed=1, kinds=TEXT_KINDS), state, 0.0,
        [Sample(0, 10, 0), Sample(60, 10, 4)],
        {"llm": "x", "embedding": "y"},
        __import__("datetime").datetime.now(__import__("datetime").timezone.utc), 10, "",
    )

    summary = bench_scale.render_summary(result)

    assert "Stopped early" in summary and "still indexing" in summary


def stress_args(**overrides: object):
    args = bench_stress.build_parser().parse_args(["--label", "unit-test", "--docs", "10"])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_a_stress_result_says_it_held_up_when_nothing_was_lost() -> None:
    state = seeded_state(10, seconds_each=5.0)
    samples = [Sample(0, 10, 0), Sample(30, 10, 5), Sample(60, 10, 10)]

    result = bench_stress.build_result(
        stress_args(), plan_corpus(10, seed=1, kinds=TEXT_KINDS), state, 0.0, samples,
        {"llm": "x", "embedding": "y"},
        __import__("datetime").datetime.now(__import__("datetime").timezone.utc), 10, 10.0,
    )

    assert result["benchmark"] == "stress"
    assert result["metrics"]["held_up"] is True
    assert result["metrics"]["peak_backlog"] == 10
    assert result["metrics"]["recovery_seconds"] == 50.0
    assert "Nothing was lost under overload." in bench_stress.render_summary(result)


def test_a_stress_result_reports_a_file_that_never_finished() -> None:
    state = seeded_state(10, seconds_each=5.0)
    lost = "rec-9"
    del state.finished_at[lost]  # accepted, listed, never reached a final state
    samples = [Sample(0, 10, 0), Sample(60, 10, 9)]

    result = bench_stress.build_result(
        stress_args(), plan_corpus(10, seed=1, kinds=TEXT_KINDS), state, 0.0, samples,
        {"llm": "x", "embedding": "y"},
        __import__("datetime").datetime.now(__import__("datetime").timezone.utc), 10, 10.0,
    )
    summary = bench_stress.render_summary(result)

    assert result["metrics"]["held_up"] is False
    assert result["metrics"]["failures"]["total"] >= 1
    assert "did not survive the overload" in summary
    assert "What to do next" in summary
