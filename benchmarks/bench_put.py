"""put() throughput (single thread, no fsync), measured with pyperf."""

from __future__ import annotations

import pyperf
from _bench import PAYLOAD, temp_dir

from equeue import Queue


def bench_put(loops: int, q: Queue) -> float:
    start = pyperf.perf_counter()
    for _ in range(loops):
        q.put(PAYLOAD)
    return pyperf.perf_counter() - start


def main() -> None:
    with temp_dir() as path:
        q = Queue(path, do_recover=False, do_vacuum=False, sync=False)
        try:
            runner = pyperf.Runner()
            runner.argparser.set_defaults(values=20)
            runner.bench_time_func("put", bench_put, q)
        finally:
            q.close()


if __name__ == "__main__":
    main()
