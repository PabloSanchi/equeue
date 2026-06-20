"""Shared pytest fixtures for EQueue tests."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator

import pytest

from equeue import Queue


@pytest.fixture
def tmp() -> Iterator[str]:
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def quiet_queue(tmp: str) -> Iterator[Queue]:
    """Queue with background threads disabled for deterministic tests."""
    with Queue(tmp, do_recover=False, do_vacuum=False) as q:
        yield q
