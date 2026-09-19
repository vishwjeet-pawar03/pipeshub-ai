"""Plain-language reasons for why a file wasn't indexed.

A record's ``reason`` is shown to people in the knowledge base ("Failed - <reason>"),
so it must say what went wrong in their terms and what to do next. Exception text
never goes there: it names internals (vector store, batches, record ids) and is
often a repr. Callers log the exception and store ``to_user_reason(exc)``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from app.agents.agent_loop.error_classification import classify_error
from app.exceptions.indexing_exceptions import (
    BlockContainerValidationError,
    ChunkingError,
    DocumentProcessingError,
    EmbeddingError,
    EmbeddingModelUnavailableError,
    EmbeddingNotConfiguredError,
    ExtractionError,
    IndexingError,
    ProcessingError,
    RecordStatusUpdateError,
    VectorStoreError,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

# "Reindex" is the action's label in the knowledge base.
GENERIC_FAILURE = (
    "We couldn't index this file. Try Reindex; if it keeps failing, ask your admin for help."
)
RETRIES_EXHAUSTED = (
    "Indexing kept failing after several tries. Try Reindex; if it keeps failing, "
    "ask your admin for help."
)
TEMPORARY_PROBLEM = (
    "PipesHub had a temporary problem while indexing this file. Try Reindex in a few minutes."
)
RETRY_SCHEDULED = "Indexing hit a temporary problem and will retry automatically."
UNREADABLE_FILE = (
    "We couldn't read this file. It may be damaged or in a format we can't open. "
    "Open it on your computer, save a fresh copy (or export it as PDF), and upload it again."
)
PASSWORD_PROTECTED = (
    "This file is password-protected, so we can't read it. Remove the password, "
    "then upload it again."
)
FILE_TOO_LARGE = "This file is too large to index. Split it into smaller files and upload them."
PROCESSING_TIMED_OUT = (
    "This file took too long to process. Try Reindex; if it keeps failing, split it "
    "into smaller files and upload them."
)
SCANNED_DOCUMENT_NEEDS_OCR = (
    "This looks like a scanned document, and reading scanned pages needs an AI model "
    "that can read images. An admin can add one in Workspace → AI Models, then Reindex the file."
)
EMBEDDING_NOT_CONFIGURED = (
    "No embedding model is set up for this workspace, so this file couldn't be made "
    "searchable. An admin can add one in Workspace → AI Models, then Reindex the file."
)
EMBEDDING_UNAVAILABLE = (
    "PipesHub couldn't reach the embedding model, so this file wasn't made searchable. "
    "Try Reindex; if it keeps failing, an admin can check the embedding model in "
    "Workspace → AI Models."
)
CONNECTOR_OFF = (
    "This file's connector is turned off, so it wasn't indexed. Turn the connector on "
    "again (or ask your admin to), and the file will be indexed."
)
CONNECTOR_REMOVED = "The connector this file came from was removed, so the file won't be indexed."
FOLDER_NOTHING_TO_INDEX = "Folders have no content of their own to index."
ENRICHMENT_FAILED = (
    "This file is searchable, but PipesHub couldn't add extra details such as its "
    "topics and categories. Try Reindex to add them."
)
RECOVERY_REQUEUED = "Indexing was interrupted and has been queued to run again."
RECOVERY_RETRY = "Indexing was interrupted and will be retried automatically."
STORED_CONTENT_MISSING = (
    "We couldn't find this file's saved contents. Try Reindex; if that fails, "
    "upload the file again."
)
STORED_CONTENT_DAMAGED = (
    "This file's processed contents are missing or damaged. Try Reindex; if that fails, "
    "upload the file again."
)


def unsupported_file_type(extension: str | None) -> str:
    ext = (extension or "").strip().lstrip(".").lower()
    what = f".{ext} files" if ext and ext not in {"unknown", "none"} else "this type of file"
    return f"PipesHub can't read {what} yet. Convert the file to PDF, DOCX or TXT to make it searchable."


def duplicate_failed(primary_reason: str | None) -> str:
    base = "An identical file failed to index, so this copy was skipped."
    return f"{base} {primary_reason}" if primary_reason else base


def _ai_messages(model: str) -> dict[str, str]:
    unreachable = (
        f"The {model} didn't respond, so this file wasn't indexed. Reindex it in a few "
        "minutes; if it keeps failing, ask your admin to check the model in "
        "Workspace → AI Models."
    )
    return {
        "rate_limit": (
            f"The {model} is busy right now, so this file wasn't indexed. Reindex it in "
            "a few minutes."
        ),
        "auth_error": (
            f"The {model} rejected PipesHub's API key, so this file wasn't indexed. An "
            "admin can update the key in Workspace → AI Models, then Reindex the file."
        ),
        "server_error": unreachable,
        "timeout": unreachable,
        "content_filter": (
            f"The {model} refused to process this file. Ask your admin to check the "
            "model's settings in Workspace → AI Models."
        ),
        "request_too_large": FILE_TOO_LARGE,
    }


# Packages whose errors come from an AI provider rather than from PipesHub itself.
# Matched as a whole package ("openai" or "openai.x"), except "langchain*", which
# covers every langchain_<provider> package. Google's AI SDKs are named exactly:
# googleapiclient errors come from connectors, not models.
_PROVIDER_MODULES = (
    "openai", "anthropic", "langchain*", "google.genai", "google.generativeai",
    "vertexai", "groq", "mistralai", "cohere", "litellm", "voyageai", "together",
    "fireworks", "ollama",
)
# Packages whose errors mean one of PipesHub's own services was briefly unreachable.
_INFRA_MODULES = (
    "neo4j", "arango", "qdrant_client", "redis", "aiokafka", "kafka", "pymongo",
    "motor", "opensearchpy", "grpc", "etcd3", "httpx", "aiohttp",
)
_CONTENT_ERRORS = (
    DocumentProcessingError, ExtractionError, ChunkingError, ProcessingError,
    BlockContainerValidationError,
)
_PASSWORD_HINTS = ("password", "encrypted", "decrypt")
_TOO_LARGE_HINTS = ("file too large", "file is too large", "file size exceeds", "exceeds the maximum")


def _chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen and len(seen) < 20:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _module(exc: BaseException) -> str:
    return type(exc).__module__ or ""


def _from(exc: BaseException, packages: tuple[str, ...]) -> bool:
    module = _module(exc)
    for package in packages:
        if package.endswith("*"):
            if module.startswith(package[:-1]):
                return True
        elif module == package or module.startswith(package + "."):
            return True
    return False


def _status_code(exc: BaseException) -> int | None:
    for attr in ("status_code", "status"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            return value
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    return value if isinstance(value, int) else None


def _provider_code(chain: list[BaseException]) -> str:
    """How an AI provider call failed: by status code, then SDK type, then text."""
    for e in chain:
        status = _status_code(e)
        if status == 429:
            return "rate_limit"
        if status in (401, 403):
            return "auth_error"
        if status == 413:
            return "request_too_large"
        if status is not None and status >= 500:
            return "server_error"
    for e in chain:
        name = type(e).__name__
        if "RateLimit" in name:
            return "rate_limit"
        if "Authentication" in name or "PermissionDenied" in name:
            return "auth_error"
        if isinstance(e, (asyncio.TimeoutError, TimeoutError)) or "Timeout" in name:
            return "timeout"
        if isinstance(e, ConnectionError) or any(
            part in name for part in ("Connection", "ConnectError", "InternalServer", "ServiceUnavailable")
        ):
            return "server_error"
    # Deepest first, and never PipesHub's own wrappers: their messages carry
    # counts ("45000 chars") that read as status codes.
    for e in reversed(chain):
        if isinstance(e, IndexingError):
            continue
        code, _ = classify_error(f"{type(e).__name__}: {e}")
        if code != "unknown":
            return code
    return "unknown"


def _llm_not_configured(exc: BaseException) -> bool:
    try:
        from app.utils.llm import LLMNotConfiguredError
    except ImportError:  # pragma: no cover - llm.py always ships with the indexer
        return False
    return isinstance(exc, LLMNotConfiguredError)


def to_user_reason(exc: BaseException | None) -> str:
    """The reason to store on a record that failed to index because of ``exc``."""
    if exc is None:
        return GENERIC_FAILURE
    chain = list(_chain(exc))

    for e in chain:
        if _llm_not_configured(e):
            return str(e)
        if e.args and e.args[0] == SCANNED_DOCUMENT_NEEDS_OCR:
            return SCANNED_DOCUMENT_NEEDS_OCR
        if isinstance(e, EmbeddingNotConfiguredError):
            return EMBEDDING_NOT_CONFIGURED

    root = chain[-1]
    embedding = any(isinstance(e, (EmbeddingError, EmbeddingModelUnavailableError)) for e in chain)
    provider = [e for e in chain if _from(e, _PROVIDER_MODULES)]
    if embedding or provider:
        # Only the provider's own errors are classified: PipesHub's wrappers add
        # ids and counts that would trip the status-code hints.
        start = chain.index(provider[0]) if provider else 0
        code = _provider_code(chain[start:])
        message = _ai_messages("embedding model" if embedding else "AI model").get(code)
        if message:
            return message
        if embedding:
            return EMBEDDING_UNAVAILABLE

    # Before the text hints: a database's "invalid password" is not a locked file.
    if isinstance(root, (ConnectionError, asyncio.TimeoutError, TimeoutError)) or _from(root, _INFRA_MODULES):
        return TEMPORARY_PROBLEM

    text = " ".join(str(e) for e in chain).lower()
    if isinstance(root, MemoryError) or any(h in text for h in _TOO_LARGE_HINTS):
        return FILE_TOO_LARGE
    if any(h in text for h in _PASSWORD_HINTS):
        return PASSWORD_PROTECTED
    if any(isinstance(e, (VectorStoreError, RecordStatusUpdateError)) for e in chain):
        return TEMPORARY_PROBLEM
    if any(isinstance(e, _CONTENT_ERRORS) for e in chain):
        return PROCESSING_TIMED_OUT if "timed out" in text else UNREADABLE_FILE
    return GENERIC_FAILURE
