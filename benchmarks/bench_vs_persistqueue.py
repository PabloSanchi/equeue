"""Comparison benchmark: EQueue (LMDB) vs persist-queue (SQLite)."""

import shutil
import tempfile
import time

import persistqueue

from equeue import Queue

PAYLOAD = "benchmark-payload"
WARMUP = 200
LOOPS = 2000


def measure(fn, loops):
    for _ in range(WARMUP):
        fn()
    t0 = time.perf_counter()
    for _ in range(loops):
        fn()
    return (time.perf_counter() - t0) / loops * 1e6


def bench_equeue_put():
    tmp = tempfile.mkdtemp()
    q = Queue(tmp, do_recover=False, do_vacuum=False, sync=False)
    try:
        return measure(lambda: q.put(PAYLOAD), LOOPS)
    finally:
        q.close()
        shutil.rmtree(tmp, ignore_errors=True)


def bench_equeue_roundtrip():
    tmp = tempfile.mkdtemp()
    q = Queue(tmp, do_recover=False, do_vacuum=False, sync=False)
    try:

        def op():
            q.put(PAYLOAD)
            q.get().ack()

        return measure(op, LOOPS)
    finally:
        q.close()
        shutil.rmtree(tmp, ignore_errors=True)


def bench_pq_put():
    tmp = tempfile.mkdtemp()
    q = persistqueue.SQLiteAckQueue(tmp, auto_commit=True, multithreading=False)
    try:
        return measure(lambda: q.put(PAYLOAD), LOOPS)
    finally:
        q.close()
        shutil.rmtree(tmp, ignore_errors=True)


def bench_pq_roundtrip():
    tmp = tempfile.mkdtemp()
    q = persistqueue.SQLiteAckQueue(tmp, auto_commit=True, multithreading=False)
    try:

        def op():
            q.put(PAYLOAD)
            item = q.get()
            q.ack(item)

        return measure(op, LOOPS)
    finally:
        q.close()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    print(
        "EQueue (LMDB) vs persist-queue (SQLite)\n",
        f"Payload: {len(PAYLOAD)} chars, {WARMUP} warmup, {LOOPS} measured loops",
        end="\n\n",
    )

    eq_put = bench_equeue_put()
    pq_put = bench_pq_put()
    eq_rt = bench_equeue_roundtrip()
    pq_rt = bench_pq_roundtrip()

    put_speedup = pq_put / eq_put
    rt_speedup = pq_rt / eq_rt

    print(
        "scenario\tequeue (us)\tpersist-queue (us)\tspeedup\n",
        f"put()\t{eq_put:.1f}\t{pq_put:.1f}\t{put_speedup:.0f}x faster\n",
        f"put+get+ack\t{eq_rt:.1f}\t{pq_rt:.1f}\t{rt_speedup:.0f}x faster\n\n",
        f"EQueue is {put_speedup:.0f}x faster on put() and {rt_speedup:.0f}x"
        "faster on a round-trip.\nBoth queues run without fsync.",
        "EQueue uses LMDB memory-mapped I/O; persist-queue uses SQLite with a journal write",
        "and B-tree update per operation.",
    )
