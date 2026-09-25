"""Async LibreOffice document conversion.

DocParser, PPTParser and XLSParser each convert legacy Office formats
(.doc/.ppt/.xls) to their OOXML equivalents via headless LibreOffice before
handing off to the OOXML-capable parser. The synchronous versions of that
conversion (``subprocess.run``) live on each of those classes for backward
compatibility with callers outside the standalone parsing service (e.g.
``app/events/processor.py``, ``app/agents/actions/util/parse_file.py``).

This module provides the async equivalent — ``asyncio.create_subprocess_exec``
instead of ``subprocess.run`` — for use inside the parsing service, where
LibreOffice's ~seconds-long runtime must not block the event loop nor
occupy a slot in the bounded parsing thread pool.
"""
from __future__ import annotations

import asyncio
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from app.exceptions.indexing_exceptions import DocumentProcessingError
from app.services.parsing.interface import ParseError, ParseErrorCode

if TYPE_CHECKING:
    from collections.abc import Iterator

LIBREOFFICE_CONVERT_TIMEOUT_SECONDS = 60
# Printed by LibreOffice (desktop/source/app/dispatchwatcher.cxx) when it cannot
# open the input file.
_SOURCE_NOT_LOADED = "source file could not be loaded"


class LibreOfficeCouldNotReadFileError(DocumentProcessingError):
    """LibreOffice ran but could not open or convert this particular file."""


@contextmanager
def unreadable_file_as_parse_error(input_ext: str) -> Iterator[None]:
    """Report a file LibreOffice cannot read as a parse failure of that file.

    Left as a plain exception it reaches the parsing service as a 500, which
    the indexer retries and counts against its circuit breaker as an outage.
    Every other LibreOffice failure (not installed, timed out, killed, profile
    or disk errors) propagates unchanged and is retried: those are about the
    deployment or the load, not the file.
    """
    try:
        yield
    except LibreOfficeCouldNotReadFileError as exc:
        raise ParseError(
            ParseErrorCode.PARSE_FAILED,
            f"LibreOffice could not read this .{input_ext} file. It may be damaged "
            f"or not really a .{input_ext} file. Open it in the app that made it, "
            "save a fresh copy, and upload it again.",
            details=exc.details,
        ) from exc


async def _run_subprocess(*args: str) -> tuple[int, bytes]:
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    return proc.returncode or 0, stderr


async def convert_with_libreoffice(binary: bytes, input_ext: str, output_ext: str) -> bytes:
    """Convert *binary* from *input_ext* to *output_ext* via headless LibreOffice.

    Runs the LibreOffice subprocess without blocking the calling event loop.
    Temp-file I/O runs on the default executor so it doesn't block the loop
    either.

    Raises:
        LibreOfficeCouldNotReadFileError: LibreOffice reports it could not load
            the file, or exits cleanly without writing the output.
        DocumentProcessingError: LibreOffice is missing, times out, is killed,
            or fails for any other reason.
    """
    which_code, which_stderr = await _run_subprocess("which", "libreoffice")
    if which_code != 0:
        raise DocumentProcessingError(
            "LibreOffice is not installed. Please install it using: sudo apt-get install libreoffice",
            details={"stderr": which_stderr.decode("utf-8", errors="replace")},
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        input_path = os.path.join(temp_dir, f"input.{input_ext}")
        output_path = os.path.join(temp_dir, f"input.{output_ext}")

        await asyncio.to_thread(Path(input_path).write_bytes, binary)

        # A private profile per conversion: runs that share the default profile
        # lock each other out, and the loser fails or exits having written nothing.
        profile_url = Path(temp_dir, "lo-profile").as_uri()
        convert_proc = await asyncio.create_subprocess_exec(
            "libreoffice",
            f"-env:UserInstallation={profile_url}",
            "--headless",
            "--convert-to",
            output_ext,
            "--outdir",
            temp_dir,
            input_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, convert_stderr = await asyncio.wait_for(
                convert_proc.communicate(), timeout=LIBREOFFICE_CONVERT_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError as e:
            convert_proc.kill()
            await convert_proc.wait()
            raise DocumentProcessingError(
                f"LibreOffice conversion timed out after {LIBREOFFICE_CONVERT_TIMEOUT_SECONDS} seconds",
                details={"timeout": f"{LIBREOFFICE_CONVERT_TIMEOUT_SECONDS}s"},
            ) from e

        returncode = convert_proc.returncode
        stderr_text = convert_stderr.decode("utf-8", errors="replace")
        details = {"exit_code": returncode, "stderr": stderr_text}
        if returncode is not None and returncode < 0:
            raise DocumentProcessingError(
                f"LibreOffice was stopped by signal {-returncode} while converting to .{output_ext}",
                details=details,
            )
        if returncode != 0:
            # Only LibreOffice's own "could not load" message means the file is
            # at fault; any other failure (profile, disk, crash) is retryable.
            error_type = (
                LibreOfficeCouldNotReadFileError
                if _SOURCE_NOT_LOADED in stderr_text
                else DocumentProcessingError
            )
            raise error_type(
                f"LibreOffice conversion to .{output_ext} failed (exit code {returncode})",
                details=details,
            )

        if not os.path.exists(output_path):
            raise LibreOfficeCouldNotReadFileError(
                f"{output_ext.upper()} conversion failed - output file not found",
                details={"expected_path": output_path},
            )

        return await asyncio.to_thread(Path(output_path).read_bytes)
