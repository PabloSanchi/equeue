# Benchmarks

Numbers collected on the machine below. `sync=False` on all runs (writes land in
the OS page cache; no fsync). The pyperf runs used `--rigorous` mode.

| Item        | Value                              |
|-------------|------------------------------------|
| CPU         | AMD Ryzen 7 7700 (8-core, 3800 MHz) |
| GPU         | NVIDIA GeForce RTX 5070 OC 12 GB   |
| RAM         | 32 GB                              |
| OS          | Windows 11 Home (Build 26200)      |
| Python      | 3.13 (CPython)                     |
| LMDB binding | lmdb 2.2.0                         |
| Storage     | NVMe SSD, NTFS                     |

| Scenario | Result |
| --- | --- |
| put() | 5.4 us (185,000/sec) |
| put+get+ack round-trip | 17.7 us (56,500/sec) |
| Concurrent claim, 8 threads | ~3,075 jobs/sec |
| Recovery per job | ~1.9 us |
| Vacuum per job | ~1.2 us |
| Python heap per Record | ~494 bytes |
| vs persist-queue put() | 128x faster |
| vs persist-queue round-trip | 112x faster |

---

## put()

`bench_put.py` calls `put()` in a tight loop. Each call opens one LMDB write
transaction and writes three keys (job, state, queued index).

```
put: Mean +- std dev: 5.42 us +- 0.21 us
```

---

## put+get+ack round-trip

`bench_put_get_ack.py` does one `put`, one `get`, and one `ack` per iteration.
The queue stays at most one item deep throughout.

```
put_get_ack: Mean +- std dev: 17.7 us +- 0.3 us
```

The 12 us on top of `put()` covers the claim transaction and the ack transaction.

---

## Concurrent workers

`bench_concurrent.py` pre-fills 200 jobs, then starts N threads that each claim
and ack until the queue is empty. LMDB allows one writer at a time, so threads
queue up on the write lock during the claim phase.

```
Threads    Time (ms)      Jobs/sec
1          53.5           3736
2          56.7           3526
4          59.6           3357
8          65.0           3075
```

Throughput drops slightly as thread count rises. In practice workers spend time
on real work between claims, so this would rarely be a bottleneck.

---

## Recovery

`bench_recovery.py` measures `_recover()` for N expired jobs (startup cost after
a crash).

```
N jobs     Mean (ms)      Min (ms)       Max (ms)
100        0.25           0.21           0.29
500        1.22           0.90           1.43
1000       2.30           1.98           2.60
5000       9.79           9.32           10.71
```

About 1.9 us per job. A crash with 1,000 in-flight jobs adds ~2 ms to startup.

---

## Vacuum

`bench_vacuum.py` measures `_vacuum()` for N completed jobs.

```
N jobs     Mean (ms)      Min (ms)       Max (ms)
100        0.13           0.12           0.14
500        0.61           0.59           0.68
1000       1.19           1.17           1.21
5000       6.20           6.00           6.40
```

About 1.2 us per job.

---

## Python heap per Record

`mem_trace.py` uses `tracemalloc`. LMDB memory-mapped pages are not tracked.

```
Baseline (empty queue open):       0.0 KB
After 1000 puts:                   9.9 KB   (+9.9 KB)
After 1000 gets (records held):  492.6 KB   (+492.6 KB)
Approx bytes per Record object:    494 bytes
After 1000 acks (records freed):  72.9 KB   (+72.9 KB)
```

Each in-flight `Record` costs about 494 bytes: ~72 B for the dataclass, ~255 B
for the `partial` closure, ~58 B for the decoded payload string (18 chars in this
test), ~49 B for the claim token.

---

## VS persist-queue

`bench_vs_persistqueue.py` compares against
[persist-queue](https://pypi.org/project/persist-queue/) (SQLite-backed,
`SQLiteAckQueue`). Both run without fsync.

```
Scenario            EQueue (us)     persist-queue (us)     Ratio
put()               4.7             599.7                  128x faster
put+get+ack         16.4            1836.8                 112x faster
```

LMDB uses memory-mapped I/O with no per-write syscall. SQLite writes a journal
entry and updates a B-tree on each operation.

Result: Equeue is significantly faster than persits-queue (check scenarios `put()` and `put+get+ack`)

---

## How to run

Benchmark dependencies (`pyperf`, `persist-queue`) are in a separate group and
are not installed by `uv sync`. Install them once with:

```bash
uv sync --group benchmarks
```

Then run:

```bash
uv run --group benchmarks python benchmarks/bench_put.py --rigorous
uv run --group benchmarks python benchmarks/bench_put_get_ack.py --rigorous
uv run python benchmarks/bench_concurrent.py
uv run python benchmarks/bench_recovery.py
uv run python benchmarks/bench_vacuum.py
uv run python benchmarks/mem_trace.py
uv run --group benchmarks python benchmarks/bench_vs_persistqueue.py
```

`bench_concurrent.py`, `bench_recovery.py`, `bench_vacuum.py`, and `mem_trace.py`
only use the standard library and `equeue`, so they work without the extra group.
