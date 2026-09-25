"""Poison-message classification: inputs that fail the same way on every
delivery must be TERMINAL, so a consumer commits them instead of retrying."""
from __future__ import annotations

import json

from app.services.messaging.error_classifier import (
    MessageErrorClassifier,
    MessageErrorType,
)


def _undecodable() -> UnicodeDecodeError:
    try:
        b"\xff\xfe".decode("utf-8")
    except UnicodeDecodeError as e:
        return e
    raise AssertionError("expected a decode error")


class TestUndecodableBytes:
    def test_bytes_that_are_not_utf8_are_terminal(self) -> None:
        assert MessageErrorClassifier.classify_by_exception(_undecodable()) == MessageErrorType.TERMINAL

    def test_a_decode_error_wrapped_by_a_handler_is_still_terminal(self) -> None:
        try:
            try:
                raise _undecodable()
            except UnicodeDecodeError as inner:
                raise RuntimeError("could not read message body") from inner
        except RuntimeError as wrapped:
            assert MessageErrorClassifier.classify_by_exception(wrapped) == MessageErrorType.TERMINAL

    def test_malformed_json_stays_terminal(self) -> None:
        try:
            json.loads("{not json")
        except json.JSONDecodeError as e:
            assert MessageErrorClassifier.classify_by_exception(e) == MessageErrorType.TERMINAL
