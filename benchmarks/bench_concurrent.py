"""Concurrent claim throughput with multiple worker threads."""

from __future__ import annotations

import statistics
import threading
import time

from _bench import PAYLOAD, temp_queue

from equeue import Queue, QueueEmpty

THREAD_COUNTS = [1, 2, 4, 8]
N_JOBS = 2000
TRIALS = 5
_DRAIN_POLL = 0.02


class Drain:
    """Counts claims and records when the final job is taken."""

    def __init__(self, total: int) -> None:
        self._total = total
        self._lock = threading.Lock()
        self.claimed = 0
        self.finished_at = 0.0

    def record_claim(self) -> bool:
        """Register one claim; return True once all jobs have been claimed."""
        with self._lock:
            self.claimed += 1
            if self.claimed == self._total:
                self.finished_at = time.perf_counter()
                return True
            return False


def _worker(q: Queue, drain: Drain, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            q.get(timeout=_DRAIN_POLL).ack()
        except QueueEmpty:
            return
        if drain.record_claim():
            stop.set()
            return


def _run_once(n_threads: int) -> float:
    """Return jobs/sec for one drain of ``N_JOBS`` by ``n_threads`` workers."""
    with temp_queue(do_recover=False, do_vacuum=False, sync=False) as q:
        for _ in range(N_JOBS):
            q.put(PAYLOAD)

        drain = Drain(N_JOBS)
        stop = threading.Event()
        threads = [
            threading.Thread(target=_worker, args=(q, drain, stop)) for _ in range(n_threads)
        ]

        start = time.perf_counter()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    elapsed = drain.finished_at - start
    if drain.claimed != N_JOBS or elapsed <= 0:
        raise AssertionError(f"drained {drain.claimed}/{N_JOBS} jobs in {elapsed:.6f}s")
    return N_JOBS / elapsed


def main() -> None:
    print(
        f"Concurrent claim: {N_JOBS} jobs, median of {TRIALS} trials\n"
        f"{'threads':>8}  {'jobs/sec':>12}"
    )
    for n_threads in THREAD_COUNTS:
        rates = [_run_once(n_threads) for _ in range(TRIALS)]
        print(f"{n_threads:>8}  {statistics.median(rates):>12,.0f}")


if __name__ == "__main__":
    main()
