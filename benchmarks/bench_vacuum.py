"""Benchmark: _vacuum() time for N done jobs."""

import shutil
import tempfile
import time

from equeue import Queue

SIZES = [100, 500, 1000, 5000]
REPS = 5
PAYLOAD = "benchmark-payload"


def run_once(n):
    """Execute n jobs with ack, let them be vacuumed later"""
    tmp = tempfile.mkdtemp()
    try:
        q = Queue(tmp, do_recover=False, do_vacuum=False, sync=False)
        for _ in range(n):
            q.put(PAYLOAD)
            q.get().ack()

        start = time.perf_counter()
        removed = q._vacuum()
        elapsed = time.perf_counter() - start

        assert removed == n, f"Expected {n} removed, got {removed}"
        q.close()
        return elapsed
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    print(f"Vacuum benchmark ({REPS} reps each)\nn_jobs\tmean (ms)\tmin (ms)\tmax (ms)")
    for n in SIZES:
        times = [run_once(n) for _ in range(REPS)]
        mean_ms = sum(times) / len(times) * 1000
        min_ms = min(times) * 1000
        max_ms = max(times) * 1000
        print(f"{n}\t{mean_ms:.2f}\t{min_ms:.2f}\t{max_ms:.2f}")
