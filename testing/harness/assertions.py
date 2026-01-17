"""Lightweight assertion helpers for the test harness."""

from __future__ import annotations

from typing import Any


def fail(message: str) -> None:
    raise AssertionError(message)


def assert_true(condition: bool, message: str | None = None) -> None:
    if not condition:
        raise AssertionError(message or "Assertion failed")


def assert_eq(actual: Any, expected: Any, message: str | None = None) -> None:
    if actual != expected:
        default_message = f"Expected {expected!r}, got {actual!r}"
        raise AssertionError(message or default_message)
