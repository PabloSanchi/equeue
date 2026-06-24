"""_vacuum() cost vs. number of DONE jobs to reclaim.

Measures the time to delete N completed jobs (all four key families per job) in
a single write transaction, the work the background vacuum thread performs.
"""

from __future__ import annotations

import statistics
import time

from _bench import PAYLOAD, format_table, temp_queue

SIZES = [100, 500, 1000, 5000]
TRIALS = 7


def _vacuum_seconds(n_jobs: int) -> float:
    with temp_queue(do_recover=False, do_vacuum=False, sync=False) as q:
        for _ in range(n_jobs):
            q.put(PAYLOAD)
            q.get().ack()

        start = time.perf_counter()
        removed = q._vacuum()
        elapsed = time.perf_counter() - start

        if removed != n_jobs:
            raise AssertionError(f"expected {n_jobs} removed, got {removed}")
    return elapsed


def main() -> None:
    print(f"Vacuum cost, median of {TRIALS} trials\n")
    rows: list[list[str]] = []
    for n_jobs in SIZES:
        median_s = statistics.median(_vacuum_seconds(n_jobs) for _ in range(TRIALS))
        rows.append([str(n_jobs), f"{median_s * 1e3:.2f}", f"{median_s / n_jobs * 1e6:.2f}"])
    print(format_table(["jobs", "median (ms)", "per job (us)"], rows))


if __name__ == "__main__":
    main()
