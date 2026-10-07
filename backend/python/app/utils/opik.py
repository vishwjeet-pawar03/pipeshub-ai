"""Shared Opik helpers for query and indexing LLM calls.

Gated on OPIK_API_KEY + OPIK_WORKSPACE. Soft-fails to a no-op when unset
or when the SDK fails to initialize.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_opik_tracer: Any | None = None
_api_key = os.getenv("OPIK_API_KEY")
_workspace = os.getenv("OPIK_WORKSPACE")
if _api_key and _workspace:
    try:
        from opik.integrations.langchain import OpikTracer

        _opik_tracer = OpikTracer()
        logger.info("Opik tracer initialized")
    except Exception as e:
        logger.warning("Failed to initialize Opik tracer: %s", e)
        _opik_tracer = None
else:
    logger.info(
        "OPIK_API_KEY and/or OPIK_WORKSPACE not set. Skipping Opik configuration."
    )


def get_opik_tracer() -> Any | None:  # noqa: ANN401
    """Return the shared OpikTracer, or None if not configured."""
    return _opik_tracer


def _collect_llm_callbacks(llm: Any) -> list[Any]:  # noqa: ANN401
    """Pull constructor callbacks off an LLM (and common wrapper attrs)."""
    callbacks: list[Any] = []
    seen: set[int] = set()
    for obj in (
        llm,
        getattr(llm, "bound", None),
        getattr(llm, "bound_llm", None),
    ):
        if obj is None:
            continue
        raw = getattr(obj, "callbacks", None)
        if not isinstance(raw, list):
            continue
        for cb in raw:
            if cb is None:
                continue
            marker = id(cb)
            if marker in seen:
                continue
            seen.add(marker)
            callbacks.append(cb)
    return callbacks


def get_llm_invoke_config(llm: Any = None, *, include_opik: bool = True) -> dict[str, Any]:  # noqa: ANN401
    """Invoke config that keeps the model's constructor callbacks + Opik on
    wrapped LLM calls. ``include_opik=False`` leaves out only the Opik tracer.

    ``with_structured_output()`` returns a runnable that does not inherit the
    base chat model's constructor callbacks. Passing only Opik in
    ``config["callbacks"]`` therefore drops them. Merge the model's callbacks
    with Opik so all fire.
    """
    callbacks = _collect_llm_callbacks(llm) if llm is not None else []
    if include_opik and _opik_tracer is not None and all(cb is not _opik_tracer for cb in callbacks):
        callbacks.append(_opik_tracer)
    return {"callbacks": callbacks} if callbacks else {}
