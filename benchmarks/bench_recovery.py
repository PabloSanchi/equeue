"""_recover() cost vs. number of expired in-flight jobs (startup-after-crash)."""

from __future__ import annotations

import statistics
import time

from _bench import PAYLOAD, format_table, temp_queue

SIZES = [100, 500, 1000, 5000]
TRIALS = 7


def _recover_seconds(n_jobs: int) -> float:
    with temp_queue(lease_time=0.001, do_recover=False, do_vacuum=False, sync=False) as q:
        for _ in range(n_jobs):
            q.put(PAYLOAD)
            q.get()
        time.sleep(0.05)

        start = time.perf_counter()
        recovered = q._recover()
        elapsed = time.perf_counter() - start

        if recovered != n_jobs:
            raise AssertionError(f"expected {n_jobs} recovered, got {recovered}")
    return elapsed


def main() -> None:
    print(f"Recovery cost, median of {TRIALS} trials\n")
    rows: list[list[str]] = []
    for n_jobs in SIZES:
        median_s = statistics.median(_recover_seconds(n_jobs) for _ in range(TRIALS))
        rows.append([str(n_jobs), f"{median_s * 1e3:.2f}", f"{median_s / n_jobs * 1e6:.2f}"])
    print(format_table(["jobs", "median (ms)", "per job (us)"], rows))


if __name__ == "__main__":
    main()
