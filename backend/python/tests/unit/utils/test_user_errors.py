"""What a person sees when a file fails to index.

Each case builds the exception chain the indexer actually produces (a PipesHub
wrapper raised ``from`` the real cause) and pins the reason stored on the record.
"""

import asyncio
import re
from pathlib import Path

import httpx
import openai
import pytest

from app.exceptions.indexing_exceptions import (
    DocumentProcessingError,
    EmbeddingError,
    EmbeddingModelUnavailableError,
    EmbeddingNotConfiguredError,
    IndexingError,
    VectorStoreError,
)
from app.utils import user_errors as ue
from app.utils.llm import LLMNotConfiguredError

_REQUEST = httpx.Request("POST", "https://api.example.com/v1/embeddings")


def _wrapped(outer: BaseException, cause: BaseException) -> BaseException:
    try:
        try:
            raise cause
        except BaseException as inner:
            raise outer from inner
    except BaseException as raised:
        return raised


def _status_error(cls: type, status: int) -> BaseException:
    response = httpx.Response(status, request=_REQUEST, json={"error": {"message": "x"}})
    return cls(f"Error code: {status}", response=response, body=None)


class _QdrantDown(Exception):
    pass


_QdrantDown.__module__ = "qdrant_client.http.exceptions"


class _RedisWrongPass(Exception):
    pass


_RedisWrongPass.__module__ = "redis.exceptions"


class TestSpecificCauses:
    def test_nothing_known_gives_the_generic_reason(self) -> None:
        assert ue.to_user_reason(None) == ue.GENERIC_FAILURE
        assert ue.to_user_reason(KeyError("llm")) == ue.GENERIC_FAILURE

    def test_missing_language_model_keeps_its_own_message_even_when_wrapped(self) -> None:
        missing = LLMNotConfiguredError()
        assert ue.to_user_reason(missing) == str(missing)
        wrapped = _wrapped(DocumentProcessingError("Failed to process document: 'llm'"), missing)
        assert ue.to_user_reason(wrapped) == str(missing)

    def test_scanned_document(self) -> None:
        assert ue.to_user_reason(IndexingError(ue.SCANNED_DOCUMENT_NEEDS_OCR)) == ue.SCANNED_DOCUMENT_NEEDS_OCR

    def test_no_embedding_model(self) -> None:
        exc = _wrapped(
            IndexingError("Failed to get embedding model instance"),
            EmbeddingNotConfiguredError("no config and no fallback"),
        )
        assert ue.to_user_reason(exc) == ue.EMBEDDING_NOT_CONFIGURED

    def test_password_protected_file(self) -> None:
        exc = _wrapped(DocumentProcessingError("Failed to process document"), ValueError("File has not been decrypted"))
        assert ue.to_user_reason(exc) == ue.PASSWORD_PROTECTED

    def test_file_too_large(self) -> None:
        assert ue.to_user_reason(_wrapped(DocumentProcessingError("x"), MemoryError())) == ue.FILE_TOO_LARGE

    def test_processing_timeout_is_not_called_a_damaged_file(self) -> None:
        exc = DocumentProcessingError("Text processing timed out after 600s for record r1 (4 blocks)")
        assert ue.to_user_reason(exc) == ue.PROCESSING_TIMED_OUT

    def test_unreadable_file(self) -> None:
        exc = _wrapped(DocumentProcessingError("Failed to process document"), ValueError("bad xref table"))
        assert ue.to_user_reason(exc) == ue.UNREADABLE_FILE


class TestAIProviders:
    @pytest.mark.parametrize(
        "cause",
        [
            openai.APIConnectionError(request=_REQUEST),
            openai.APITimeoutError(request=_REQUEST),
            httpx.ConnectTimeout("timed out"),
            httpx.ReadTimeout("timed out"),
            asyncio.TimeoutError(),
        ],
        ids=["connection", "sdk-timeout", "connect-timeout", "read-timeout", "asyncio-timeout"],
    )
    def test_embedding_model_not_answering(self, cause: BaseException) -> None:
        exc = _wrapped(
            EmbeddingError("Dense embedding failed after 3 attempts (60s each) for batch of 12 texts / 45000 chars (record r1)"),
            cause,
        )
        assert ue.to_user_reason(exc) == ue._ai_messages("embedding model")["timeout"]

    def test_embedding_rate_limited(self) -> None:
        exc = _wrapped(EmbeddingError("Dense embedding failed"), _status_error(openai.RateLimitError, 429))
        assert ue.to_user_reason(exc) == ue._ai_messages("embedding model")["rate_limit"]

    @pytest.mark.parametrize(
        ("cause", "code"),
        [
            (openai.APIConnectionError(request=_REQUEST), "server_error"),
            (_status_error(openai.InternalServerError, 503), "server_error"),
            (_status_error(openai.RateLimitError, 429), "rate_limit"),
            (_status_error(openai.AuthenticationError, 401), "auth_error"),
        ],
        ids=["connection", "5xx", "429", "401"],
    )
    def test_language_model_failures_while_processing(self, cause: BaseException, code: str) -> None:
        # Table summaries and image checks call the LLM inside the processor,
        # which wraps whatever it raises as a content error.
        exc = _wrapped(DocumentProcessingError("Failed to process document: Connection error."), cause)
        assert ue.to_user_reason(exc) == ue._ai_messages("AI model")[code]

    def test_openai_connection_error_wrapping_an_httpx_error(self) -> None:
        sdk = _wrapped(openai.APIConnectionError(request=_REQUEST), httpx.ConnectError("refused"))
        exc = _wrapped(DocumentProcessingError("Failed to process document"), sdk)
        assert ue.to_user_reason(exc) == ue._ai_messages("AI model")["server_error"]

    def test_embedding_model_that_fails_its_probe(self) -> None:
        exc = _wrapped(EmbeddingModelUnavailableError("Failed to get embedding model"), ValueError("dimension probe"))
        assert ue.to_user_reason(exc) == ue.EMBEDDING_UNAVAILABLE

    def test_wrapper_counts_are_not_read_as_status_codes(self) -> None:
        exc = _wrapped(
            EmbeddingError("Dense embedding failed for batch of 12 texts / 45000 chars (record r429)"),
            ValueError("unexpected embedding shape"),
        )
        assert ue.to_user_reason(exc) == ue.EMBEDDING_UNAVAILABLE


class TestPipesHubServices:
    def test_vector_database_down(self) -> None:
        exc = _wrapped(VectorStoreError("Failed to store batch 0"), _QdrantDown("connection refused"))
        assert ue.to_user_reason(exc) == ue.TEMPORARY_PROBLEM

    def test_database_password_error_is_not_a_locked_file(self) -> None:
        exc = _wrapped(DocumentProcessingError("Error updating record status"), _RedisWrongPass("invalid username-password pair"))
        assert ue.to_user_reason(exc) == ue.TEMPORARY_PROBLEM

    def test_connection_error(self) -> None:
        assert ue.to_user_reason(_wrapped(VectorStoreError("x"), ConnectionError())) == ue.TEMPORARY_PROBLEM


class TestHelpers:
    @pytest.mark.parametrize(("ext", "expected"), [("xyz", ".xyz files"), (".KEY", ".key files"), (None, "this type of file"), ("unknown", "this type of file")])
    def test_unsupported_file_type_names_the_extension(self, ext, expected) -> None:
        assert expected in ue.unsupported_file_type(ext)
        assert "PDF, DOCX or TXT" in ue.unsupported_file_type(ext)

    def test_duplicate_failure_carries_the_primary_reason(self) -> None:
        assert ue.duplicate_failed(ue.FILE_TOO_LARGE).endswith(ue.FILE_TOO_LARGE)
        assert ue.duplicate_failed(None).startswith("An identical file failed to index")


_JARGON = re.compile(
    r"Error\b|Exception|vector store|batch|kafka|consumer|graph|blob|record id|traceback|\bid\b|status code|\d{3}",
    re.IGNORECASE,
)


def test_no_reason_shown_to_people_uses_internal_words() -> None:
    texts = [v for k, v in vars(ue).items() if k.isupper() and isinstance(v, str)]
    texts += list(ue._ai_messages("AI model").values()) + [ue.unsupported_file_type("xyz")]
    for text in texts:
        assert not _JARGON.search(text), text


# Record reasons in the indexing path come from the mapper or a constant, never
# from exception text.
_INDEXING_FILES = [
    "app/services/messaging/kafka/handlers/record.py",
    "app/events/events.py",
    "app/indexing_main.py",
]
_RAW_REASON = re.compile(r"""(reason\s*=|["']reason["']\s*:|\[["']reason["']\]\s*=)\s*(str\(|f["'][^"']*\{|error_msg\b)""")


@pytest.mark.parametrize("path", _INDEXING_FILES)
def test_indexing_code_never_stores_exception_text_as_a_reason(path: str) -> None:
    source = (Path(__file__).resolve().parents[3] / path).read_text(encoding="utf-8")
    assert not _RAW_REASON.findall(source)
