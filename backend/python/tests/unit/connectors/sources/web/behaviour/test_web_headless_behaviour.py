"""Crawling through the headless browser: Robust Mode, automatic detection of
script-rendered sites, and what happens when no browser can be started.

The browser is the fake from web_behaviour_fakes; ``Crawl4AIFetcher`` and the
connector's batching run for real.
"""

import pytest
from web_behaviour_fakes import (
    START_URL,
    FakeRecordsDb,
    FakeWeb,
    MakeConnector,
    Page,
    VirtualClock,
    html_page,
)

from app.connectors.sources.web import crawl4ai_fetcher

SHELL = b"<html><head><title>App</title></head><body><div id='root'></div></body></html>"
LONG_TEXT = "This paragraph is only there once the page's scripts have run. " * 5


async def test_robust_mode_crawls_every_page_through_the_browser(
    browser: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    browser.html(START_URL, "Home", "/a", "/b")
    browser.html("http://site.test/a", "A", "/b")
    browser.html("http://site.test/b", "B")

    await (await make_connector(use_headless_browser=True)).run_sync()

    assert set(db.pages()) == {START_URL, "http://site.test/a", "http://site.test/b"}
    assert sorted(browser.browser_visits) == [START_URL, "http://site.test/a", "http://site.test/b"]
    assert browser.fetched_urls() == set()


async def test_robust_mode_respects_the_page_cap(
    browser: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    links = [f"/p{i}" for i in range(12)]
    browser.html(START_URL, "Home", *links)
    for link in links:
        browser.html(f"http://site.test{link}", link)

    await (await make_connector(use_headless_browser=True, max_pages=5)).run_sync()

    assert len(browser.browser_visits) == 5
    assert len(db.pages()) == 5


async def test_robust_mode_backs_off_and_retries_a_rate_limited_page(
    browser: FakeWeb, db: FakeRecordsDb, clock: VirtualClock, make_connector: MakeConnector
) -> None:
    browser.html(START_URL, "Home", "/busy")
    browser.add("http://site.test/busy", [Page(status=429), Page(body=html_page("Busy"))])

    await (await make_connector(use_headless_browser=True)).run_sync()

    assert db.pages()["http://site.test/busy"].record_name == "Busy"
    assert 15.0 in clock.sleeps


async def test_a_script_rendered_site_is_detected_and_crawled_with_the_browser(
    browser: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    rendered = html_page("App", "/inside", text=LONG_TEXT)
    browser.add(START_URL, Page(body=SHELL, rendered=rendered, pre_render_text_len=0))
    browser.add("http://site.test/inside", Page(body=SHELL, rendered=html_page("Inside", text=LONG_TEXT)))

    connector = await make_connector()
    assert connector.use_headless_browser is True
    await connector.run_sync()

    assert db.pages()["http://site.test/inside"].record_name == "Inside"


async def test_a_server_rendered_site_is_crawled_without_the_browser(
    browser: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    browser.html(START_URL, "Home", "/next", text=LONG_TEXT)
    browser.html("http://site.test/next", "Next")

    connector = await make_connector()
    await connector.run_sync()

    assert connector.use_headless_browser is False
    assert browser.browser_visits == [START_URL]
    assert set(db.pages()) == {START_URL, "http://site.test/next"}


@pytest.mark.xfail(strict=True, reason="bug: init fails when the CSR probe cannot start a browser")
async def test_without_a_working_browser_a_plain_site_still_syncs(
    browser: FakeWeb, db: FakeRecordsDb, make_connector: MakeConnector
) -> None:
    browser.browser_broken = True
    browser.html(START_URL, "Home", "/next")
    browser.html("http://site.test/next", "Next")

    connector = await make_connector()
    await connector.run_sync()

    assert set(db.pages()) == {START_URL, "http://site.test/next"}


async def test_robust_mode_without_a_working_browser_fails_to_start(
    browser: FakeWeb, make_connector: MakeConnector
) -> None:
    browser.browser_broken = True
    browser.html(START_URL, "Home")

    await make_connector(use_headless_browser=True, expect_init=False)


async def test_the_shared_browser_is_closed_when_the_last_connector_is_cleaned_up(
    browser: FakeWeb, make_connector: MakeConnector
) -> None:
    browser.html(START_URL, "Home")
    first = await make_connector(use_headless_browser=True)
    second = await make_connector(use_headless_browser=True)
    assert first.crawl4ai_fetcher is second.crawl4ai_fetcher

    await first.cleanup()
    assert crawl4ai_fetcher._shared_instance is second.crawl4ai_fetcher

    await second.cleanup()
    assert crawl4ai_fetcher._shared_instance is None
