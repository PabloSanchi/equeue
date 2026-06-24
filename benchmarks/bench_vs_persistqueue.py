"""EQueue (LMDB) vs persist-queue (SQLite ack queue)."""

from __future__ import annotations

import contextlib
import functools
import sqlite3
from collections.abc import Callable, Iterator

import persistqueue
from _bench import PAYLOAD, Stat, format_table, repeat_trials, temp_dir

from equeue import Queue

WARMUP = 200
OPS = 2000
TRIALS = 7

EQUEUE_KWARGS = {"do_recover": False, "do_vacuum": False}


def _set_sqlite_synchronous(q: persistqueue.SQLiteAckQueue, mode: str) -> None:
    """Apply ``PRAGMA synchronous`` to every live connection of the queue."""
    seen: set[int] = set()
    for attr in ("_conn", "_putter", "_getter"):
        conn = getattr(q, attr, None)
        if isinstance(conn, sqlite3.Connection) and id(conn) not in seen:
            conn.execute(f"PRAGMA synchronous={mode};")
            seen.add(id(conn))


@contextlib.contextmanager
def _equeue_put(*, sync: bool) -> Iterator[Callable[[], object]]:
    with temp_dir() as path:
        q = Queue(path, sync=sync, **EQUEUE_KWARGS)
        try:
            yield lambda: q.put(PAYLOAD)
        finally:
            q.close()


@contextlib.contextmanager
def _equeue_round_trip(*, sync: bool) -> Iterator[Callable[[], object]]:
    with temp_dir() as path:
        q = Queue(path, sync=sync, **EQUEUE_KWARGS)

        def op() -> None:
            q.put(PAYLOAD)
            q.get().ack()

        try:
            yield op
        finally:
            q.close()


@contextlib.contextmanager
def _pq_queue(*, synchronous: str) -> Iterator[persistqueue.SQLiteAckQueue]:
    with temp_dir() as path:
        q = persistqueue.SQLiteAckQueue(path, auto_commit=True, multithreading=False)
        _set_sqlite_synchronous(q, synchronous)
        try:
            yield q
        finally:
            with contextlib.suppress(Exception):
                q.close()


@contextlib.contextmanager
def _pq_put(*, synchronous: str) -> Iterator[Callable[[], object]]:
    with _pq_queue(synchronous=synchronous) as q:
        yield lambda: q.put(PAYLOAD)


@contextlib.contextmanager
def _pq_round_trip(*, synchronous: str) -> Iterator[Callable[[], object]]:
    with _pq_queue(synchronous=synchronous) as q:

        def op() -> None:
            q.put(PAYLOAD)
            q.ack(q.get())

        yield op

REGIMES = {
    "durable (fsync)": (True, "FULL"),
    "fast (no fsync)": (False, "OFF"),
}


def _measure(trial: Callable[[], object]) -> Stat:
    return repeat_trials(trial, ops=OPS, warmup=WARMUP, trials=TRIALS)


def main() -> None:
    print(
        "EQueue (LMDB) vs persist-queue (SQLite), matched durability\n"
        f"payload {len(PAYLOAD)} chars | {WARMUP} warmup + {OPS} ops/trial | "
        f"median of {TRIALS} trials\n"
    )

    rows: list[list[str]] = []
    for regime, (eq_sync, pq_sync) in REGIMES.items():
        scenarios = (
            (
                "put",
                functools.partial(_equeue_put, sync=eq_sync),
                functools.partial(_pq_put, synchronous=pq_sync),
            ),
            (
                "put+get+ack",
                functools.partial(_equeue_round_trip, sync=eq_sync),
                functools.partial(_pq_round_trip, synchronous=pq_sync),
            ),
        )
        for scenario, eq_trial, pq_trial in scenarios:
            eq = _measure(eq_trial)
            pq = _measure(pq_trial)
            rows.append(
                [
                    regime,
                    scenario,
                    f"{eq.median:.1f} +/- {eq.stdev:.1f}",
                    f"{pq.median:.1f} +/- {pq.stdev:.1f}",
                    f"{pq.median / eq.median:.1f}x",
                ]
            )

    print(
        format_table(
            ["regime", "scenario", "equeue (us)", "persist-queue (us)", "speedup"],
            rows,
        )
    )


if __name__ == "__main__":
    main()
