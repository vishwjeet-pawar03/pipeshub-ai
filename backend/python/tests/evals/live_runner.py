"""Run the golden cases against a real model.

``live_harness.run_golden_evals`` takes a ``run_agent`` callable and marks every
case skipped without one. This module supplies that callable: it builds a real
``Agent`` on the real runtime, gives it the tools each case grants, and turns
the run's result into the ``TraceResult`` the assertions read.

The tools are stubs that return fixed text. What the golden cases ask is which
tool the model reaches for, in what order, and whether it answers without being
asked to write — questions about the agent's behaviour, not about what the
knowledge graph contains. Stubbing keeps a scheduled run to one model call path
with no stack, no data and no per-run seeding, and keeps the answer to "did
behaviour change?" from depending on whether a corpus indexed correctly.

What this does NOT measure, and should not be read as measuring: retrieval
quality, citation correctness, or answers over real customer-shaped data. Those
need the stack and belong with the browser and integration tests.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from app.agent_loop_lib.agent import Agent
from app.agent_loop_lib.agent.loops import ReActLoop
from app.agent_loop_lib.agent.spec import AgentSpec, ModelSpec
from app.agent_loop_lib.core.types import Goal
from app.agent_loop_lib.runtime.runtime import AgentRuntime
from app.agent_loop_lib.tools.base import ParameterType, Tool, ToolOutput, ToolParameter
from app.agent_loop_lib.tools.registry import ToolRegistry
from app.agent_loop_lib.transport.registry import TransportRegistry
from tests.evals.live_harness import GoldenCase, TraceResult

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from langchain_core.language_models import BaseChatModel

    from app.agent_loop_lib.core.responses import RunUsage
    from app.agent_loop_lib.core.types import AgentResult

# Enough system prompt to be a fair test of tool choice without scripting the
# answer: a prompt that named the expected tool would make every case pass.
SYSTEM_PROMPT = (
    "You are PipesHub's assistant. Answer the user's question using the tools "
    "you have been given. Ask before taking any action that changes data."
)

MAX_TURNS = 8

# What a stub tool hands back. Short and obviously synthetic: the assertions
# are about the model's choices, and a long fake document would only add
# tokens (and cost) to every run.
_STUB_RESULT = "No matching records were found."


class StubTool(Tool):
    """A tool that exists so the model can choose it, and returns fixed text."""

    def __init__(self, tool_name: str) -> None:
        self._name = tool_name

    @property
    def name(self) -> str:
        return self._name

    @property
    def short_description(self) -> str:
        return f"Stub of {self._name} for behaviour evaluation."

    @property
    def description(self) -> str:
        return (
            f"{self._name}: available during an evaluation run. It returns a "
            "fixed result and changes nothing."
        )

    @property
    def path(self) -> str:
        return f"/evals/{self._name}"

    @property
    def parameters(self) -> list[ToolParameter]:
        return [
            ToolParameter(
                name="input",
                type=ParameterType.STRING,
                description="Whatever this tool would normally be given.",
                required=False,
                default="",
            )
        ]

    async def execute(self, **kwargs: Any) -> ToolOutput:  # noqa: ANN401 - the Tool ABC's own signature
        return ToolOutput(success=True, data=_STUB_RESULT)


class MissingModelError(RuntimeError):
    """No model to run against — a run without one measures nothing."""


def build_chat_model(provider: str, model: str, api_key: str | None) -> "BaseChatModel":
    """A LangChain chat model for ``provider``.

    Kept to the providers the scheduled run uses. A new provider is a few lines
    here rather than a new abstraction.
    """
    if not api_key:
        raise MissingModelError(
            f"No API key for '{provider}'. Set the key in the workflow's "
            "environment (TEST_OPENAI_API_KEY for OpenAI) and run again."
        )
    if provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=model, api_key=api_key, temperature=0)
    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        return ChatAnthropic(model=model, api_key=api_key, temperature=0)
    raise MissingModelError(
        f"'{provider}' is not a provider this runner knows. Use 'openai' or "
        "'anthropic', or add it to build_chat_model in tests/evals/live_runner.py."
    )


def _registry_for(case: GoldenCase) -> ToolRegistry:
    registry = ToolRegistry()
    for tool_name in case.granted_tools:
        registry.register_tool(StubTool(tool_name))
    return registry


def _trace_from(result: "AgentResult") -> TraceResult:
    """The agent's run, in the shape the golden assertions read."""
    tool_calls: list[str] = []
    for turn in getattr(result, "turns", []) or []:
        for call in getattr(turn, "tool_calls", []) or []:
            name = getattr(call, "name", None)
            if name:
                tool_calls.append(name)
    output = getattr(result, "output", "")
    confidence = getattr(result, "confidence", None)
    return TraceResult(
        first_tool=tool_calls[0] if tool_calls else None,
        tool_calls=tool_calls,
        final_answer=output if isinstance(output, str) else str(output or ""),
        # Confidence is an enum on the result ("very_high"); the assertions
        # compare against its own spelling, so hand over the plain value and
        # let the assertion decide.
        confidence=getattr(confidence, "value", confidence),
        completion_data={
            "confidence": getattr(confidence, "value", confidence),
            "record_ids": list(getattr(result, "record_ids", []) or []),
            "needs_input": getattr(result, "needs_input", None),
            "success": getattr(result, "success", None),
            "error": getattr(result, "error", None),
        },
    )


class UsageTally:
    """Tokens across every case in a run, for costing."""

    def __init__(self) -> None:
        self.input_tokens = 0
        self.output_tokens = 0
        self.requests = 0

    def add(self, usage: "RunUsage | None") -> None:
        self.input_tokens += int(getattr(usage, "input_tokens", 0) or 0)
        self.output_tokens += int(getattr(usage, "output_tokens", 0) or 0)
        self.requests += int(getattr(usage, "requests", 0) or 0)


def make_run_agent(
    chat_model: "BaseChatModel", model_name: str, tally: UsageTally
) -> "Callable[[GoldenCase, object], Awaitable[TraceResult]]":
    """The ``run_agent`` callable ``run_golden_evals`` expects."""

    async def run_agent(case: GoldenCase, _model: object) -> TraceResult:
        from app.agents.agent_loop.langchain_transport import LangChainTransport

        transport_registry = TransportRegistry()
        transport_registry.register(
            "langchain",
            lambda: LangChainTransport(chat_model, model_name=model_name),
        )
        registry = _registry_for(case)
        runtime = AgentRuntime(
            transport_registry=transport_registry, tool_registry=registry
        )
        spec = AgentSpec(
            name="answer-quality-eval",
            system_prompt=SYSTEM_PROMPT,
            tool_names=registry.names(),
            model=ModelSpec(provider="langchain", model=model_name),
            loop=ReActLoop(),
            max_turns=MAX_TURNS,
        )
        agent = Agent(spec, runtime)
        result = await agent.run(Goal(description=case.query))
        tally.add(getattr(result, "usage", None))
        return _trace_from(result)

    return run_agent


def model_from_env() -> tuple[str, str, str | None]:
    """Provider, model and key from the environment.

    Reads the same variables the integration workflows already set, so a
    scheduled run needs no new secret.
    """
    provider = os.getenv("EVAL_PROVIDER", "openai")
    if provider == "openai":
        model = os.getenv("EVAL_MODEL") or os.getenv("TEST_OPENAI_LLM_MODEL") or "gpt-4o-mini"
        return provider, model, os.getenv("TEST_OPENAI_API_KEY")
    if provider == "anthropic":
        model = os.getenv("EVAL_MODEL") or "claude-haiku-4-5"
        return provider, model, os.getenv("TEST_ANTHROPIC_API_KEY")
    return provider, os.getenv("EVAL_MODEL", ""), None


__all__ = [
    "MAX_TURNS",
    "SYSTEM_PROMPT",
    "MissingModelError",
    "StubTool",
    "UsageTally",
    "build_chat_model",
    "make_run_agent",
    "model_from_env",
]
