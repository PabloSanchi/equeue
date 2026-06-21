"""Benchmark: concurrent claim throughput with multiple threads."""

import shutil
import tempfile
import threading
import time

from equeue import Queue, QueueEmpty

THREAD_COUNTS = [1, 2, 4, 8]
N_JOBS = 200
PAYLOAD = "benchmark-payload"


def worker(q, results, idx, done):
    claimed = 0
    while not done.is_set():
        try:
            q.get(timeout=0.05).ack()
            claimed += 1
        except QueueEmpty:
            break
    results[idx] = claimed


def run_once(n_threads):
    tmp = tempfile.mkdtemp()
    try:
        q = Queue(tmp, do_recover=False, do_vacuum=False, sync=False)

        for _ in range(N_JOBS):
            q.put(PAYLOAD)

        results = [0] * n_threads
        done = threading.Event()
        threads = [
            threading.Thread(target=worker, args=(q, results, i, done)) for i in range(n_threads)
        ]

        start = time.perf_counter()

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        elapsed = time.perf_counter() - start
        done.set()

        q.close()
        assert sum(results) == N_JOBS, f"Lost jobs: got {sum(results)}, expected {N_JOBS}"
        return elapsed
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    print(f"Concurrent claim benchmark: {N_JOBS} jobs\nthreads\ttime (ms)\tjobs/sec")
    for t in THREAD_COUNTS:
        elapsed = run_once(t)
        jobs_per_sec = N_JOBS / elapsed
        print(f"{t}\t{elapsed * 1000:.1f}\t{jobs_per_sec:.0f}")
