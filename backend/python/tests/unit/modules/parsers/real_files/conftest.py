"""Small real sample files, built in memory, for the parser tests in this folder.

The root conftest swaps any package it cannot import for a MagicMock. A parser
test run against a mocked python-docx or pdfplumber would pass while checking
nothing, so every test here fails loudly unless the real libraries are loaded.
"""

from __future__ import annotations

import importlib
import logging
import os
import stat
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

REAL_LIBRARIES = (
    "docx",
    "openpyxl",
    "pptx",
    "pdfplumber",
    "reportlab.pdfgen.canvas",
    "PIL.Image",
    "bs4",
    "selectolax.lexbor",
    "markdown_it",
    "docling.document_converter",
    "docling_core",
)


@pytest.fixture(autouse=True, scope="session")
def _parsing_libraries_are_real() -> None:
    mocked = [
        name for name in REAL_LIBRARIES
        if isinstance(importlib.import_module(name), MagicMock)
    ]
    if mocked:
        pytest.fail(
            "These parsing libraries are MagicMock stand-ins, so these tests would "
            f"check nothing: {mocked}. Run them with the Python 3.12 test venv."
        )


@pytest.fixture(autouse=True)
def _real_docling_converter() -> Iterator[None]:
    # DoclingProcessor caches one DocumentConverter per process. Other test
    # files build it while DocumentConverter is patched, which would leave a
    # MagicMock cached for these tests.
    from app.modules.parsers.pdf import docling_processor

    docling_processor._get_converter.cache_clear()
    yield
    docling_processor._get_converter.cache_clear()


@pytest.fixture
def logger() -> logging.Logger:
    return logging.getLogger("parser-real-file-tests")


@pytest.fixture
def fake_libreoffice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[..., None]:
    """Put a stand-in ``libreoffice`` on PATH so conversion error handling runs
    through a real subprocess even where LibreOffice is not installed.

    By default it fails the way the real one does on a file it cannot open.
    ``body`` replaces the whole script; every call's arguments are appended,
    one per line, to ``tmp_path / "libreoffice-args.log"``."""

    def install(
        stderr: str = "Error: source file could not be loaded",
        exit_code: int = 1,
        body: str | None = None,
    ) -> None:
        bin_dir = tmp_path / "fake-bin"
        bin_dir.mkdir(exist_ok=True)
        script = bin_dir / "libreoffice"
        log = tmp_path / "libreoffice-args.log"
        record = f'printf "%s\\n" "$@" >> "{log}"\necho "---" >> "{log}"\n'
        script.write_text("#!/bin/sh\n" + record + (body or f"echo '{stderr}' >&2\nexit {exit_code}\n"))
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")

    return install
