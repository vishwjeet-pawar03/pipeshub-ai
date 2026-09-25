"""What the crawler stores for each kind of response: HTML pages, documents,
oversized downloads, and pages the user's filters exclude.
"""

import base64

import pytest
from web_behaviour_fakes import (
    START_URL,
    FakeRecordsDb,
    FakeWeb,
    MakeConnector,
    Page,
    html_page,
)

from app.config.constants.arangodb import MimeTypes, ProgressStatus

MB = 1024 * 1024
OFFICE_TYPES = "application/vnd.openxmlformats-officedocument"


def _extensions(operator: str, *extensions: str) -> dict:
    return {"sync": {"values": {"file_extensions": {"operator": operator, "value": list(extensions), "type": "multiselect"}}}}


@pytest.mark.parametrize(
    ("path", "content_type", "mime", "extension"),
    [
        ("/files/report.pdf", "application/pdf", MimeTypes.PDF, "pdf"),
        ("/files/report.pdf", "application/octet-stream", MimeTypes.PDF, "pdf"),
        ("/download?id=7", "application/pdf", MimeTypes.PDF, "pdf"),
        ("/files/notes.txt", "text/plain; charset=utf-8", MimeTypes.PLAIN_TEXT, "txt"),
        ("/files/letter.docx", f"{OFFICE_TYPES}.wordprocessingml.document", MimeTypes.DOCX, "docx"),
        ("/files/budget.xlsx", f"{OFFICE_TYPES}.spreadsheetml.sheet", MimeTypes.XLSX, "xlsx"),
        ("/files/deck.pptx", f"{OFFICE_TYPES}.presentationml.presentation", MimeTypes.PPTX, "pptx"),
        ("/files/old.doc", "application/msword", MimeTypes.DOC, "doc"),
    ],
)
async def test_linked_documents_are_stored_with_their_real_type_and_bytes(
    path: str, content_type: str, mime: MimeTypes, extension: str,
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector,
) -> None:
    url = f"http://site.test{path}"
    payload = b"PK\x03\x04 binary document bytes \x00\x01"
    site.html(START_URL, "Home", path)
    site.add(url, Page(body=payload, content_type=content_type))

    await (await make_connector()).run_sync()

    record = db.pages()[url]
    assert (record.mime_type, record.extension) == (mime.value, extension)
    assert site.storage_docs[record.storage_document_id] == payload


async def test_a_stored_html_page_keeps_its_text_and_drops_scripts_and_navigation(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    site.add(START_URL, Page(body=html_page("Home", text="Welcome to the handbook").replace(
        b"</body>", b"<nav>Menu links</nav><script>track()</script></body>")))

    await (await make_connector(crawl_type="single")).run_sync()

    stored = site.storage_docs[db.pages()[START_URL].storage_document_id]
    assert b"Welcome to the handbook" in stored
    assert b"track()" not in stored and b"Menu links" not in stored


async def test_page_images_are_embedded_so_the_stored_copy_is_self_contained(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
    )
    site.add(START_URL, Page(body=html_page("Home", body='<img src="/pic.png">')))
    site.add("http://site.test/pic.png", Page(body=png, content_type="image/png"))

    await (await make_connector(crawl_type="single")).run_sync()

    stored = site.storage_docs[db.pages()[START_URL].storage_document_id]
    assert b'src="data:image/png;base64,' in stored


async def test_with_image_indexing_off_images_are_dropped_not_downloaded(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    site.add(START_URL, Page(body=html_page("Home", body='<img src="/pic.png">')))
    filters = {"indexing": {"values": {"images": {"operator": "is", "value": False, "type": "boolean"}}}}

    await (await make_connector(crawl_type="single", filters=filters)).run_sync()

    stored = site.storage_docs[db.pages()[START_URL].storage_document_id]
    assert b"<img" not in stored
    assert site.gets("http://site.test/pic.png") == 0


async def test_with_webpage_indexing_off_pages_are_stored_but_not_queued_for_indexing(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    site.html(START_URL, "Home", "/guide.pdf")
    site.add("http://site.test/guide.pdf", Page(body=b"%PDF-1.4", content_type="application/pdf"))
    filters = {"indexing": {"values": {"webpages": {"operator": "is", "value": False, "type": "boolean"}}}}

    await (await make_connector(filters=filters)).run_sync()

    assert db.pages()[START_URL].indexing_status == ProgressStatus.AUTO_INDEX_OFF.value
    assert db.pages()["http://site.test/guide.pdf"].indexing_status != ProgressStatus.AUTO_INDEX_OFF.value


async def test_an_oversized_download_is_skipped_without_fetching_its_body(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    big = "http://site.test/huge.pdf"
    site.html(START_URL, "Home", "/huge.pdf", "/small")
    site.add(big, Page(body=b"x" * (2 * MB), content_type="application/pdf"))
    site.html("http://site.test/small", "Small")

    await (await make_connector(max_size_mb=1)).run_sync()

    assert site.gets(big) == 0
    assert big not in db.pages()
    assert "http://site.test/small" in db.pages()


async def test_an_oversized_download_does_not_start_the_headless_browser(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    big = "http://site.test/huge.pdf"
    site.html(START_URL, "Home", "/huge.pdf")
    site.add(big, Page(body=b"x" * (2 * MB), content_type="application/pdf"))
    connector = await make_connector(max_size_mb=1)
    starts_after_init = site.browser_starts

    await connector.run_sync()

    assert big not in site.browser_visits
    assert site.browser_starts == starts_after_init


async def test_an_oversized_page_without_a_declared_size_is_not_stored(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    big = "http://site.test/stream.pdf"
    site.html(START_URL, "Home", "/stream.pdf")
    site.add(big, Page(body=b"x" * (2 * MB), content_type="application/pdf", chunked=True))

    await (await make_connector(max_size_mb=1)).run_sync()

    assert big not in db.pages()


async def test_a_page_that_grew_too_big_keeps_its_stored_record(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    doc = "http://site.test/manual.pdf"
    site.html(START_URL, "Home", "/manual.pdf")
    site.add(doc, Page(body=b"%PDF small", content_type="application/pdf"))
    connector = await make_connector(max_size_mb=1)
    await connector.run_sync()
    first = db.pages()[doc]

    site.add(doc, Page(body=b"x" * (2 * MB), content_type="application/pdf"))
    await connector.run_sync()

    assert db.pages()[doc].external_revision_id == first.external_revision_id
    assert db.deleted == []


@pytest.mark.xfail(strict=True, reason="bug: pages the extension filter excludes are not searched for links")
async def test_an_only_pdfs_filter_still_finds_pdfs_linked_from_pages(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    site.html(START_URL, "Home", "/docs/")
    site.html("http://site.test/docs/", "Docs", "/docs/manual.pdf")
    site.add("http://site.test/docs/manual.pdf", Page(body=b"%PDF-1.4 manual", content_type="application/pdf"))

    await (await make_connector(filters=_extensions("in", "pdf"))).run_sync()

    assert set(db.pages()) == {"http://site.test/docs/manual.pdf"}


async def test_an_exclude_pdfs_filter_skips_pdfs_and_keeps_pages(
    site: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    site.html(START_URL, "Home", "/manual.pdf", "/about")
    site.add("http://site.test/manual.pdf", Page(body=b"%PDF-1.4 manual", content_type="application/pdf"))
    site.html("http://site.test/about", "About")

    await (await make_connector(filters=_extensions("not_in", "pdf"))).run_sync()

    assert set(db.pages()) == {START_URL, "http://site.test/about"}
