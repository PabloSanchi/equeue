"""Benchmark: _recover() time for N expired running jobs."""

import shutil
import tempfile
import time

from equeue import Queue

SIZES = [100, 500, 1000, 5000]
REPS = 5
PAYLOAD = "benchmark-payload"


def run_once(n):
    """Run jobs and let them expire, so then they can be recovered"""
    tmp = tempfile.mkdtemp()
    try:
        q = Queue(tmp, lease_time=0.001, do_recover=False, do_vacuum=False, sync=False)
        for _ in range(n):
            q.put(PAYLOAD)
            q.get()

        time.sleep(0.05)

        start = time.perf_counter()
        recovered = q._recover()
        elapsed = time.perf_counter() - start

        assert recovered == n, f"Expected {n} recovered, got {recovered}"
        q.close()
        return elapsed
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    print(f"Recovery benchmark ({REPS} reps each)\nn_jobs\tmean (ms)\tmin (ms)\tmax (ms)")
    for n in SIZES:
        times = [run_once(n) for _ in range(REPS)]
        mean_ms = sum(times) / len(times) * 1000
        min_ms = min(times) * 1000
        max_ms = max(times) * 1000
        print(f"{n}\t{mean_ms:.2f}\t{min_ms:.2f}\t{max_ms:.2f}")
