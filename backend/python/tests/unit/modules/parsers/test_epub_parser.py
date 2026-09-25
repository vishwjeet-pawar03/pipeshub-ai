"""Unit tests for app.modules.parsers.epub.epub_parser.EPUBParser."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.modules.parsers.epub.epub_parser import EPUBParser
from app.services.parsing.interface import ParseError, ParseErrorCode


class TestEPUBParserInit:
    def test_default_pdf_parser_is_none(self):
        parser = EPUBParser()
        assert parser.pdf_parser is None

    def test_stores_provided_pdf_parser(self):
        mock_inner = MagicMock()
        parser = EPUBParser(pdf_parser=mock_inner)
        assert parser.pdf_parser is mock_inner


class TestParse:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("with_pdf_parser", [True, False])
    async def test_reports_unsupported_format_and_never_parses(self, with_pdf_parser):
        pdf_parser = MagicMock()
        pdf_parser.parse = AsyncMock()
        parser = EPUBParser(pdf_parser if with_pdf_parser else None)

        with pytest.raises(ParseError) as exc_info:
            await parser.parse(b"epub bytes", "book.epub", {"extension": "epub"})

        assert exc_info.value.code == ParseErrorCode.UNSUPPORTED_FORMAT
        assert exc_info.value.details == {"extension": "epub"}
        assert ".epub" in exc_info.value.message
        pdf_parser.parse.assert_not_called()

    def test_never_imports_pymupdf(self):
        import app.modules.parsers.epub.epub_parser as epub_parser_module

        assert "fitz" not in dir(epub_parser_module)
        assert "pymupdf" not in dir(epub_parser_module)
