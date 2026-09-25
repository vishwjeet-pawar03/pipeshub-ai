"""Decode uploaded text files (plain text, Markdown, MDX, HTML, JSON) into str.

Files arrive in whatever encoding the tool that saved them used: UTF-8, UTF-8
or UTF-16 with a byte-order mark (Windows Notepad, Excel "Unicode Text",
PowerShell), or Windows-1252. A strict UTF-8 decode turns every one of those
into either an exception or a stray BOM glued to the first word.
"""

from __future__ import annotations

import codecs

from bs4.dammit import EncodingDetector

# UTF-32 LE must be checked before UTF-16 LE: its BOM starts with the same two bytes.
_BOMS = (
    (codecs.BOM_UTF32_LE, "utf-32-le"),
    (codecs.BOM_UTF32_BE, "utf-32-be"),
    (codecs.BOM_UTF8, "utf-8"),
    (codecs.BOM_UTF16_LE, "utf-16-le"),
    (codecs.BOM_UTF16_BE, "utf-16-be"),
)


def decode_text(content: bytes | str, *, html: bool = False) -> str:
    """Return *content* as text, never raising on an unexpected encoding.

    Order: byte-order mark, then strict UTF-8, then (for HTML) the charset the
    page declares, then Windows-1252 with undefined bytes replaced.
    """
    if isinstance(content, str):
        return content.removeprefix("\ufeff")

    for bom, encoding in _BOMS:
        if content.startswith(bom):
            return content[len(bom):].decode(encoding, errors="replace")

    # UTF-8 goes before the declared charset: pages re-saved as UTF-8 often keep
    # a stale <meta charset="iso-8859-1">, and single-byte codecs accept any
    # bytes, so trusting the declaration first would garble every accent.
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        pass

    if html:
        declared = EncodingDetector.find_declared_encoding(content, is_html=True)
        if declared:
            try:
                return content.decode(declared)
            except (LookupError, UnicodeDecodeError):
                pass

    # A UTF-8 file with a few damaged bytes still has far more valid multi-byte
    # characters than broken ones; reading it as Windows-1252 would garble them all.
    lenient = content.decode("utf-8", errors="replace")
    broken = lenient.count("\ufffd")
    valid_non_ascii = sum(1 for ch in lenient if ord(ch) > 0x7F) - broken
    if valid_non_ascii > broken:
        return lenient

    return content.decode("cp1252", errors="replace")
