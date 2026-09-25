"""A chat answer cut off at the model's output-token limit and continued is
saved and shown as one answer.

Drives the same pieces `stream_bridge.py` wires together for a chat
request: a real `Agent` streaming a scripted model, `TerminalAnswerStreamer`
for the live answer, `TranscriptCollector` for the saved parts (the newer
chat client), and `AnswerFinalizer` for the saved answer and citations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.agent_loop_lib.agent import Agent
from app.agent_loop_lib.agent.loops import ReActLoop
from app.agent_loop_lib.agent.spec import AgentSpec, ModelSpec
from app.agent_loop_lib.core.types import Goal
from app.agent_loop_lib.runtime.runtime import AgentRuntime
from app.agent_loop_lib.tools.registry import ToolRegistry
from app.agent_loop_lib.transport.registry import TransportRegistry
from app.agents.agent_loop.answer_streamer import TerminalAnswerStreamer
from app.agents.agent_loop.hooks.citations import CitationCollector
from app.agents.agent_loop.protocol.transcript_collector import TranscriptCollector
from app.agents.agent_loop.respond import AnswerFinalizer
from tests.unit.agent_loop_lib.agent.test_cut_off_reply_continuation import (
    _TokenStream,
    _turn,
)
from tests.unit.agents.adapter.conftest import make_context

if TYPE_CHECKING:
    from app.agents.agent_loop.context import AgentContext

_REPORT = "https://example.com/report"
_BLOG = "https://example.com/blog"


class _Sink:
    def __init__(self) -> None:
        self.events: list[dict] = []

    async def write(self, event: dict) -> bool:
        self.events.append(event)
        return True


def _context(protocol: str) -> AgentContext:
    overrides: dict = {"protocol": protocol}
    if protocol == "agui":
        overrides["transcript_collector"] = TranscriptCollector()
    context = make_context(**overrides)
    context.tool_state["web_records"] = [
        {"url": _REPORT, "title": "Report", "content": "Report body."},
        {"url": _BLOG, "title": "Blog", "content": "Blog body."},
    ]
    return context


async def _chat(context: AgentContext, transport: _TokenStream) -> tuple[dict, TerminalAnswerStreamer, _Sink]:
    transports = TransportRegistry()
    transports.register("scripted", lambda: transport)
    spec = AgentSpec(
        name="chat-agent",
        system_prompt="You are a helpful assistant.",
        model=ModelSpec(provider="scripted", model="scripted-model"),
        loop=ReActLoop(),
        max_turns=6,
    )
    agent = Agent(spec, AgentRuntime(
        transport_registry=transports, tool_registry=ToolRegistry(),
        event_emitter=context.transcript_collector,
    ))
    sink = _Sink()
    collector = CitationCollector(context)
    streamer = TerminalAnswerStreamer(context, collector, sink)
    streamer._emit_interval = 0.0
    async for event in agent.stream(Goal(description="How did the launch go?")):
        await streamer.on_event(event)
    result = agent.last_stream_result
    completion = await AnswerFinalizer(context, collector).run(
        agent_success=result.success, agent_error=result.error, agent_output=result.output,
        streamed_answer=streamer.streamed_answer, reasoning_turns=streamer.reasoning_turns,
        event_sink=sink,
    )
    return completion, streamer, sink


def _text_parts(completion: dict) -> list[dict]:
    return [p for p in completion["parts"] if p["type"] == "text"]


class TestSavedAnswerIsWhole:
    async def test_one_cut_off_then_continuation(self) -> None:
        context = _context("agui")
        transport = _TokenStream([
            _turn("The launch went well overall, ", cut_off=True),
            _turn("with sign-ups ahead of plan."),
        ])

        completion, streamer, _ = await _chat(context, transport)

        whole = "The launch went well overall, with sign-ups ahead of plan."
        assert completion["answer"] == whole
        assert streamer.streamed_answer == whole
        [final] = _text_parts(completion)
        assert final == {"type": "text", "content": whole, "runId": final["runId"], "isFinal": True}

    async def test_two_continuations_are_joined_in_order(self) -> None:
        context = _context("agui")
        transport = _TokenStream([
            _turn("First, sign-ups beat plan. ", cut_off=True),
            _turn("Second, churn stayed flat. ", cut_off=True),
            _turn("Third, support load was light."),
        ])

        completion, _, _ = await _chat(context, transport)

        whole = "First, sign-ups beat plan. Second, churn stayed flat. Third, support load was light."
        assert completion["answer"] == whole
        assert [p["content"] for p in _text_parts(completion)] == [whole]

    async def test_cut_off_mid_sentence(self) -> None:
        context = _context("agui")
        transport = _TokenStream([
            _turn("Sign-ups reached 4,2", cut_off=True),
            _turn("00 in the first week."),
        ])

        completion, _, _ = await _chat(context, transport)

        assert completion["answer"] == "Sign-ups reached 4,200 in the first week."

    async def test_thinking_from_both_turns_is_kept_and_the_answer_is_not_narration(self) -> None:
        context = _context("agui")
        transport = _TokenStream([
            _turn("The launch went well overall, ", cut_off=True, thinking="Lead with the verdict."),
            _turn("with sign-ups ahead of plan.", thinking="Now the numbers."),
        ])

        completion, streamer, _ = await _chat(context, transport)

        assert [t["content"] for t in streamer.reasoning_turns] == ["Lead with the verdict.", "Now the numbers."]
        [final] = _text_parts(completion)
        assert final["isFinal"] is True
        assert final["content"] == completion["answer"] == "The launch went well overall, with sign-ups ahead of plan."

    async def test_normal_reply_is_unchanged(self) -> None:
        context = _context("agui")
        transport = _TokenStream([_turn("The launch went well overall.")])

        completion, streamer, _ = await _chat(context, transport)

        assert completion["answer"] == "The launch went well overall."
        assert streamer.streamed_answer == "The launch went well overall."
        assert [p["content"] for p in _text_parts(completion)] == ["The launch went well overall."]


class TestCitationsAcrossTheCut:
    async def test_citations_in_both_parts_share_one_numbering(self) -> None:
        context = _context("agui")
        transport = _TokenStream([
            _turn(f"Per the [report]({_REPORT}), sign-ups beat plan; the [blog](https://example.com/bl", cut_off=True),
            _turn(f"og) agrees, and the [report]({_REPORT}) adds that churn was flat."),
        ])

        completion, _, _ = await _chat(context, transport)

        answer = completion["answer"]
        assert "[blog]" not in answer and "[report]" not in answer
        assert len(completion["citations"]) == 2
        assert answer.count("[1]") == 2 and answer.count("[2]") == 1
        assert answer.index("[1]") < answer.index("[2]") < answer.rindex("[1]")
        assert [p["content"] for p in _text_parts(completion)] == [answer]

    async def test_live_answer_keeps_the_first_part_on_screen(self) -> None:
        context = _context("legacy")
        transport = _TokenStream([
            _turn(f"Per the [report]({_REPORT}), sign-ups ", cut_off=True),
            _turn(f"beat plan and the [blog]({_BLOG}) agrees."),
        ])

        completion, _, sink = await _chat(context, transport)

        chunks = [e["data"] for e in sink.events if e["event"] == "answer_chunk"]
        # Once the continuation starts, what is on screen still begins with part one.
        continuation = [c for c in chunks if "beat plan" in c["accumulated"]]
        assert continuation
        part_one_on_screen = f"Per the [1]({_REPORT}), sign-ups "
        assert all(c["accumulated"].startswith(part_one_on_screen) for c in continuation)
        assert completion["answer"].startswith(part_one_on_screen)
        assert chunks[-1]["accumulated"] == completion["answer"]
        assert len(completion["citations"]) == 2
