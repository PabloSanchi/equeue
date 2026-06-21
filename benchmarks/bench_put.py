"""Benchmark: put() throughput."""

import shutil
import tempfile

import pyperf

from equeue import Queue

PAYLOAD = "benchmark-payload"


def bench_put(loops, q):
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        q.put(PAYLOAD)
    return pyperf.perf_counter() - t0


tmp = tempfile.mkdtemp()
q = Queue(tmp, do_recover=False, do_vacuum=False, sync=False)

runner = pyperf.Runner()
runner.argparser.set_defaults(values=20)
try:
    runner.bench_time_func("put", bench_put, q)
finally:
    q.close()
    shutil.rmtree(tmp, ignore_errors=True)
