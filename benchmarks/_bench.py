"""Shared helpers for the EQueue benchmarks."""

from __future__ import annotations

import contextlib
import shutil
import statistics
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager

from equeue import Queue

PAYLOAD = "benchmark-payload"


@contextlib.contextmanager
def temp_dir() -> Iterator[str]:
    """Yield a throwaway directory that is removed on exit."""
    path = tempfile.mkdtemp(prefix="equeue-bench-")
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


@contextlib.contextmanager
def temp_queue(**kwargs: object) -> Iterator[Queue]:
    """Yield an EQueue in a throwaway directory, closed and removed on exit."""
    with temp_dir() as path:
        q = Queue(path, **kwargs)
        try:
            yield q
        finally:
            q.close()


def time_per_op_us(op: Callable[[], object], *, ops: int, warmup: int) -> float:
    """Return mean microseconds per call of ``op`` over ``ops`` iterations.

    ``warmup`` untimed calls run first to prime caches and let the working set
    settle, so the timed window reflects steady-state cost.
    """
    for _ in range(warmup):
        op()
    start = time.perf_counter()
    for _ in range(ops):
        op()
    return (time.perf_counter() - start) / ops * 1e6


class Stat:
    """Summary of repeated per-operation timing samples (in microseconds)."""

    def __init__(self, samples_us: list[float]) -> None:
        self.samples = samples_us
        self.median = statistics.median(samples_us)
        self.mean = statistics.fmean(samples_us)
        self.stdev = statistics.stdev(samples_us) if len(samples_us) > 1 else 0.0
        self.min = min(samples_us)

    @property
    def ops_per_sec(self) -> float:
        return 1e6 / self.median if self.median else float("inf")


def repeat_trials(
    trial: Callable[[], AbstractContextManager[Callable[[], object]]],
    *,
    ops: int,
    warmup: int,
    trials: int,
) -> Stat:
    """Run ``trials`` independent measurements and summarise them."""
    samples: list[float] = []
    for _ in range(trials):
        with trial() as op:
            samples.append(time_per_op_us(op, ops=ops, warmup=warmup))
    return Stat(samples)


def format_table(headers: list[str], rows: list[list[str]]) -> str:
    """Render a left-aligned fixed-width text table."""
    widths = [
        max(len(headers[col]), *(len(row[col]) for row in rows)) if rows else len(headers[col])
        for col in range(len(headers))
    ]

    def line(cells: list[str]) -> str:
        return "  ".join(cell.ljust(widths[col]) for col, cell in enumerate(cells))

    sep = "  ".join("-" * w for w in widths)
    return "\n".join([line(headers), sep, *(line(row) for row in rows)])
