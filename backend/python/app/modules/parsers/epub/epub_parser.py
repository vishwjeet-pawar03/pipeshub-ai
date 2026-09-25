from typing import Any

from app.services.parsing.interface import (
    IParser,
    ParseResult,
    UnsupportedFormatError,
)
from app.utils.user_errors import unsupported_file_type


class EPUBParser:
    """Parser for EPUB e-books: reports every book as an unsupported format.

    EPUB used to be converted to PDF with LibreOffice, but LibreOffice cannot
    open EPUB in any release (its only EPUB filter exports). That failure came
    back as a server error, which the indexer retried and counted against the
    parsing circuit breaker, so a few books could stall every other file.
    Until EPUB is read directly, each book fails once with a plain reason.
    """

    def __init__(self, pdf_parser: IParser | None = None) -> None:
        # Kept so the parsing service's registration is unchanged.
        self.pdf_parser = pdf_parser

    async def parse(
        self, content: bytes, record_name: str, config: dict[str, Any] | None = None,
    ) -> ParseResult:
        raise UnsupportedFormatError("epub", unsupported_file_type("epub"))
