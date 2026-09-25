"""`ScriptedTransport` for plan/verify flows: structured calls (the critics
behind `critique_plan` and `verify_result`) return scripted verdicts in
order, without using up a slot of the main `complete()` script. Once the
verdicts run out, a structured call returns `{}`, which both critics read
as a pass."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.agent_loop_lib.core.responses import StructuredResponse, TokenUsage
from tests.unit.agents.adapter.support.scripted_transport import ScriptedTransport

if TYPE_CHECKING:
    from app.agent_loop_lib.core.messages import Message


class VerdictTransport(ScriptedTransport):
    def __init__(self, verdicts: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self._verdicts = list(verdicts or [])
        self.structured_prompts: list[str] = []

    async def complete_structured(
        self,
        messages: list[Message],
        output_schema: dict,
        system: str | None = None,
        model: str | None = None,
    ) -> StructuredResponse:
        self.structured_prompts.append("\n".join(str(m.content) for m in messages))
        data = self._verdicts.pop(0) if self._verdicts else {}
        return StructuredResponse(data=data, usage=TokenUsage(), model=self._model)


__all__ = ["VerdictTransport"]
