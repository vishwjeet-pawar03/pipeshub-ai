"""SECRET_KEY startup check: missing, empty and published placeholder values are refused."""

import hashlib
import logging
from unittest.mock import AsyncMock, patch

import pytest

from app.config import secret_key as sk
from app.config.secret_key import (
    ALLOW_PLACEHOLDER_ENV,
    PLACEHOLDER_SECRET_KEYS,
    get_hashed_secret_key,
)


@pytest.fixture(autouse=True)
def _reset_warning() -> None:
    sk._warn_placeholder_once.cache_clear()


def test_returns_sha256_hex_of_a_real_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECRET_KEY", "a3f1c2d4e5b6")
    assert get_hashed_secret_key() == hashlib.sha256(b"a3f1c2d4e5b6").hexdigest()


@pytest.mark.parametrize("value", [None, "", "   "])
def test_missing_or_empty_key_is_refused(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    if value is None:
        monkeypatch.delenv("SECRET_KEY", raising=False)
    else:
        monkeypatch.setenv("SECRET_KEY", value)
    with pytest.raises(ValueError, match="SECRET_KEY environment variable is required.*openssl rand -hex 32"):
        get_hashed_secret_key()


@pytest.mark.parametrize("value", sorted(PLACEHOLDER_SECRET_KEYS))
def test_placeholder_key_is_refused(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("SECRET_KEY", value)
    monkeypatch.delenv(ALLOW_PLACEHOLDER_ENV, raising=False)
    with pytest.raises(ValueError, match=f"example value.*{ALLOW_PLACEHOLDER_ENV} to true"):
        get_hashed_secret_key()


def test_placeholder_with_surrounding_whitespace_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECRET_KEY", " your_random_encryption_secret_key\n")
    monkeypatch.delenv(ALLOW_PLACEHOLDER_ENV, raising=False)
    with pytest.raises(ValueError, match="example value"):
        get_hashed_secret_key()


@pytest.mark.parametrize("flag", ["", "false", "1", "yes"])
def test_only_true_opts_in(monkeypatch: pytest.MonkeyPatch, flag: str) -> None:
    monkeypatch.setenv("SECRET_KEY", "your_random_encryption_secret_key")
    monkeypatch.setenv(ALLOW_PLACEHOLDER_ENV, flag)
    with pytest.raises(ValueError):
        get_hashed_secret_key()


def test_opt_in_keeps_the_existing_key_and_warns_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("SECRET_KEY", "your_random_encryption_secret_key")
    monkeypatch.setenv(ALLOW_PLACEHOLDER_ENV, "TRUE")
    with caplog.at_level(logging.WARNING, logger="app.config.secret_key"):
        first = get_hashed_secret_key()
        second = get_hashed_secret_key()
    # Same key as before the check existed, so data already stored stays readable.
    assert first == second == hashlib.sha256(b"your_random_encryption_secret_key").hexdigest()
    assert [r.levelno for r in caplog.records].count(logging.WARNING) == 1
    assert "your_random_encryption_secret_key" not in caplog.text


def test_configuration_service_refuses_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config.configuration_service import ConfigurationService

    monkeypatch.setenv("SECRET_KEY", "your_random_encryption_secret_key")
    monkeypatch.delenv(ALLOW_PLACEHOLDER_ENV, raising=False)
    with patch.object(ConfigurationService, "_start_watch"), pytest.raises(ValueError, match="example value"):
        ConfigurationService(logger=logging.getLogger("test"), key_value_store=AsyncMock())


def test_encrypted_store_refuses_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.config.providers.encrypted_store import EncryptedKeyValueStore

    monkeypatch.setenv("SECRET_KEY", "your_random_encryption_secret_key")
    monkeypatch.delenv(ALLOW_PLACEHOLDER_ENV, raising=False)
    with patch.object(EncryptedKeyValueStore, "_create_store") as create, pytest.raises(ValueError, match="example value"):
        EncryptedKeyValueStore(logger=logging.getLogger("test"))
    create.assert_not_called()


def test_sync_point_refuses_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    from app.connectors.core.base.sync_point.sync_point import (
        SyncDataPointType,
        SyncPoint,
    )

    monkeypatch.setenv("SECRET_KEY", "your_random_encryption_secret_key")
    monkeypatch.delenv(ALLOW_PLACEHOLDER_ENV, raising=False)
    with pytest.raises(ValueError, match="example value"):
        SyncPoint("c1", "o1", next(iter(SyncDataPointType)), AsyncMock())
