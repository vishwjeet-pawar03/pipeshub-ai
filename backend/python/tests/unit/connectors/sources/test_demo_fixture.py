"""Guards on the Acme Corp demo fixture that are cheap enough for every pull request.

The demo's acceptance test asks the golden questions against a running
instance, so it only runs nightly. These catch the ways the demo has already
gone wrong, or nearly did, without an instance:

- links that point at real third-party accounts instead of a reserved domain;
- suggested questions on the chat landing that drift from the questions the
  acceptance test actually scores;
- expectations that name records the fixture no longer has;
- the permissions lesson (only the pricing committee sees pricing) quietly
  disappearing.
"""

from __future__ import annotations

import json
import re
import urllib.parse
from pathlib import Path

import pytest
import yaml

import app.connectors.sources.demo.connector as demo_connector

FIXTURE = Path(demo_connector.__file__).resolve().parent / "fixture" / "acme-corp.yaml"
RESERVED_DOMAIN = "acme-demo.example"
CHAT_LOCALES = ("en-US", "en-IN")


def _repo_root() -> Path | None:
    for parent in FIXTURE.parents:
        if (parent / "frontend" / "lib" / "i18n" / "locales").is_dir():
            return parent
    return None


@pytest.fixture(scope="module")
def fixture_text() -> str:
    return FIXTURE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def fx(fixture_text: str) -> dict:
    return yaml.safe_load(fixture_text)


def test_every_link_stays_on_the_reserved_demo_domain(fixture_text: str) -> None:
    # Demo records show "Open in GitHub/Jira/Slack" links. A real host there
    # (github.com/acme-demo, acme-demo.slack.com, ...) sends a first-time user
    # to an account somebody else can register and fill.
    urls = re.findall(r"https?://[^\s\"')\]]+", fixture_text)
    assert urls, "the fixture should carry source links"
    outside = sorted({
        u for u in urls
        if not urllib.parse.urlparse(u).netloc.endswith(RESERVED_DOMAIN)
    })
    assert not outside, f"links outside {RESERVED_DOMAIN}: {outside}"


@pytest.mark.parametrize("locale", CHAT_LOCALES)
def test_chat_suggestions_are_the_questions_the_acceptance_test_scores(fx: dict, locale: str) -> None:
    root = _repo_root()
    if root is None:
        pytest.skip("frontend sources are not in this checkout")
    messages = json.loads((root / "frontend" / "lib" / "i18n" / "locales" / f"{locale}.json").read_text(encoding="utf-8"))
    chips = [item["text"] for item in messages["chat"]["demoSuggestions"].values()]
    assert chips == [q["ask"] for q in fx["questions"]], (
        "the chat landing's demo questions must match fixture.questions word for word, "
        "or the questions people click are not the ones the acceptance test checks"
    )


def test_expectations_name_records_that_exist(fx: dict) -> None:
    known = {r["id"] for r in fx["records"]} | {t["id"] for t in fx.get("threads", [])}
    keys = ("must_cite", "must_cite_any_of", "must_cite_any_of_2", "stretch_cite_any_of", "must_not_cite", "restricted")
    missing = {
        q["id"]: sorted(x for k in keys for x in q.get(k, []) if x not in known)
        for q in fx["questions"]
    }
    assert not {k: v for k, v in missing.items() if v}, missing


def test_only_the_pricing_committee_can_see_pricing(fx: dict) -> None:
    containers = {c["id"]: c for c in fx["containers"]}
    people = {p["id"]: p for p in fx["people"]}
    groups = {g["id"]: g for g in fx["groups"]}
    restricted_q = next(q for q in fx["questions"] if q.get("restricted"))
    group_of_record = {
        r["id"]: containers[r["container"]]["group"] for r in fx["records"]
    } | {t["id"]: containers[t["container"]]["group"] for t in fx.get("threads", [])}

    restricted_groups = {group_of_record[x] for x in restricted_q["restricted"]}
    assert restricted_groups == {"pricing-committee"}
    assert not groups["pricing-committee"].get("installer_joins"), "the installer must not see pricing"
    assert "pricing-committee" in people["bob"].get("groups", [])
    assert "pricing-committee" not in people["alice"].get("groups", [])
    assert restricted_q["personas"] == {"alice": "none", "bob": "cites"}
