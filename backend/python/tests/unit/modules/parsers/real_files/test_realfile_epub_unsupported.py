"""EPUB is reported as an unsupported format: once, with a plain reason, and
without anything treating it as an outage.

LibreOffice, which the EPUB path converted with, cannot open EPUB in any
release. Reported as a server error, every EPUB was retried and then counted
against the parsing circuit breaker, so a few books could stall indexing of
every other file.
"""

from __future__ import annotations

import io
import shutil
import zipfile
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes.parsing import router as parsing_router
from app.config.constants.arangodb import ExtensionTypes
from app.events.events import DedupDecision, EventProcessor
from app.modules.parsers.epub.epub_parser import EPUBParser
from app.services.messaging.error_classifier import (
    MessageErrorClassifier,
    MessageErrorType,
)
from app.services.parsing.client import ParsingClient, ParsingClientError
from app.services.parsing.interface import ParseError, ParseErrorCode, ParserProvider
from app.services.parsing.registry import ParserRegistry
from app.services.resource_governor import ResourceGovernor
from app.services.resource_governor.models import ResourceSnapshot
from app.utils.libreoffice_convert import _run_libreoffice, convert_with_libreoffice
from app.utils.user_errors import to_user_reason, unsupported_file_type

if TYPE_CHECKING:
    from pathlib import Path

EPUB_REASON = unsupported_file_type("epub")


def _make_epub() -> bytes:
    """A small, valid EPUB 3 book."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as book:
        book.writestr("mimetype", "application/epub+zip", compress_type=zipfile.ZIP_STORED)
        book.writestr(
            "META-INF/container.xml",
            '<?xml version="1.0"?><container version="1.0" '
            'xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles>'
            '<rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/>'
            "</rootfiles></container>",
        )
        book.writestr(
            "OEBPS/content.opf",
            '<?xml version="1.0"?><package xmlns="http://www.idpf.org/2007/opf" version="3.0" '
            'unique-identifier="id"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/">'
            '<dc:identifier id="id">book-1</dc:identifier><dc:title>Handbook</dc:title>'
            "<dc:language>en</dc:language></metadata><manifest>"
            '<item id="c1" href="c1.xhtml" media-type="application/xhtml+xml"/></manifest>'
            '<spine><itemref idref="c1"/></spine></package>',
        )
        book.writestr(
            "OEBPS/c1.xhtml",
            '<?xml version="1.0"?><html xmlns="http://www.w3.org/1999/xhtml"><head>'
            "<title>Chapter 1</title></head><body><p>Leave policy: twenty days.</p></body></html>",
        )
    return buffer.getvalue()


def _assert_libreoffice_never_ran(tmp_path: Path) -> None:
    assert not (tmp_path / "libreoffice-args.log").exists(), "LibreOffice should not have been started"


def _assert_reported_as_unsupported(exc: BaseException) -> None:
    assert MessageErrorClassifier.classify_by_exception(exc) == MessageErrorType.TERMINAL
    assert to_user_reason(exc) == EPUB_REASON


async def test_epub_parser_reports_unsupported_without_running_libreoffice(fake_libreoffice, tmp_path) -> None:
    # The stand-in fails the way LibreOffice does on EPUB, in case it is reached.
    fake_libreoffice(probe_ok=False)
    pdf_parser = MagicMock()
    pdf_parser.parse = AsyncMock()

    with pytest.raises(ParseError) as caught:
        await EPUBParser(pdf_parser).parse(_make_epub(), "handbook.epub")

    assert caught.value.code == ParseErrorCode.UNSUPPORTED_FORMAT
    assert caught.value.message == EPUB_REASON
    pdf_parser.parse.assert_not_called()
    _assert_libreoffice_never_ran(tmp_path)
    _assert_reported_as_unsupported(caught.value)


def _event_processor() -> tuple[EventProcessor, MagicMock]:
    processor = MagicMock()
    processor.indexing_pipeline = AsyncMock()
    graph_provider = AsyncMock()
    graph_provider.update_node = AsyncMock(return_value=True)
    graph_provider.get_document.return_value = {"_key": "rec-1", "recordType": "FILE"}
    return EventProcessor(MagicMock(), processor, graph_provider, MagicMock()), processor


async def test_in_process_indexing_path_reports_unsupported(fake_libreoffice, tmp_path) -> None:
    fake_libreoffice(probe_ok=False)
    events, processor = _event_processor()
    event_data = {
        "payload": {
            "recordId": "rec-1",
            "orgId": "org-1",
            "virtualRecordId": None,
            "version": 1,
            "connectorName": "",
            "extension": ExtensionTypes.EPUB.value,
            "mimeType": "application/epub+zip",
            "recordName": "handbook.epub",
            "buffer": _make_epub(),
        }
    }

    with patch.object(
        events, "_check_duplicate_by_md5", new_callable=AsyncMock,
        return_value=DedupDecision(virtual_record_id=None, skip_indexing=False),
    ), pytest.raises(ParseError) as caught:
        async for _ in events.on_event(event_data):
            pass

    assert caught.value.code == ParseErrorCode.UNSUPPORTED_FORMAT
    processor.process_pdf_with_docling.assert_not_called()
    processor.process_pdf_document_with_ocr.assert_not_called()
    _assert_libreoffice_never_ran(tmp_path)
    _assert_reported_as_unsupported(caught.value)


def _parsing_app() -> FastAPI:
    snapshot = ResourceSnapshot(
        cpu_quota=4.0, cpu_utilisation=0.1, cpu_throttled_ratio=0.0, cpu_pressure=0.0,
        mem_limit_bytes=8 * 1024 ** 3, mem_working_set_bytes=1024 ** 3, source="test",
    )
    probe = MagicMock()
    probe.snapshot.return_value = snapshot
    registry = ParserRegistry()
    registry.register("epub", ParserProvider.DEFAULT, EPUBParser(MagicMock()))
    app = FastAPI()
    app.state.parser_registry = registry
    app.state.governor = ResourceGovernor(logger=MagicMock(), env_parse=5, probe=probe)
    app.include_router(parsing_router)
    return app


def test_parsing_service_answers_with_a_non_retryable_unsupported_format(fake_libreoffice) -> None:
    fake_libreoffice(probe_ok=False)
    response = TestClient(_parsing_app()).post(
        "/api/v1/parse",
        files={"file": ("handbook.epub", _make_epub(), "application/epub+zip")},
        data={"record_name": "handbook.epub", "mime_type": "application/epub+zip",
              "extension": "epub", "provider": "default"},
    )
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == ParseErrorCode.UNSUPPORTED_FORMAT.value
    assert error["message"] == EPUB_REASON


async def test_indexer_client_fails_once_and_leaves_the_circuit_breaker_alone(fake_libreoffice, tmp_path) -> None:
    fake_libreoffice(probe_ok=False)
    app = _parsing_app()
    requests: list[int] = []

    async def count(response: httpx.Response) -> None:
        requests.append(response.status_code)

    client = ParsingClient(service_url="http://parsing.test", max_retries=3, retry_delay=0.0)
    client._make_client = lambda: httpx.AsyncClient(  # type: ignore[method-assign]
        transport=httpx.ASGITransport(app=app), event_hooks={"response": [count]}
    )

    with pytest.raises(ParsingClientError) as caught:
        await client.parse(
            file_content=_make_epub(), record_name="handbook.epub",
            mime_type="application/epub+zip", extension="epub", provider=ParserProvider.DEFAULT,
        )

    assert requests == [422]
    assert caught.value.code == ParseErrorCode.UNSUPPORTED_FORMAT
    assert client.circuit_breaker._consecutive_failures == 0
    assert not client.circuit_open
    _assert_libreoffice_never_ran(tmp_path)
    _assert_reported_as_unsupported(caught.value)


async def test_other_legacy_formats_still_go_through_libreoffice(fake_libreoffice, tmp_path) -> None:
    fake_libreoffice(probe_ok=False)
    with pytest.raises(Exception) as caught:
        await convert_with_libreoffice(b"damaged", "ppt", "pptx")
    assert not isinstance(caught.value, ParseError)
    assert (tmp_path / "libreoffice-args.log").exists()


@pytest.mark.skipif(shutil.which("libreoffice") is None, reason="LibreOffice is not installed")
async def test_real_libreoffice_still_cannot_open_epub() -> None:
    # The premise of reporting EPUB as unsupported. If a LibreOffice release
    # ever gains an EPUB import filter, this fails and the decision can be revisited.
    run = await _run_libreoffice(_make_epub(), "epub", "pdf")
    assert run.output is None
    assert "could not be loaded" in run.stderr
