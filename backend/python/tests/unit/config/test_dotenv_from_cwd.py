"""Services read the .env of the directory they are started from."""

from __future__ import annotations

import importlib
import os

import pytest


@pytest.fixture
def env_in_cwd(tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("PIPESHUB_DOTENV_PROBE=from-cwd\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PIPESHUB_DOTENV_PROBE", raising=False)
    yield
    os.environ.pop("PIPESHUB_DOTENV_PROBE", None)


def test_configuration_service_loads_the_working_directory_env(env_in_cwd) -> None:
    importlib.reload(importlib.import_module("app.config.configuration_service"))

    assert os.environ.get("PIPESHUB_DOTENV_PROBE") == "from-cwd"


def test_a_service_container_loads_the_working_directory_env(env_in_cwd) -> None:
    importlib.reload(importlib.import_module("app.containers.parsing"))

    assert os.environ.get("PIPESHUB_DOTENV_PROBE") == "from-cwd"
