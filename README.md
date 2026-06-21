# EQueue


[![Build](https://github.com/PabloSanchi/equeue/actions/workflows/build.yml/badge.svg)](https://github.com/PabloSanchi/equeue/actions/workflows/build.yml)
[![Tests](https://github.com/PabloSanchi/equeue/actions/workflows/tests.yml/badge.svg)](https://github.com/PabloSanchi/equeue/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
<!-- [![PyPI](https://img.shields.io/pypi/v/equeue)](https://pypi.org/project/equeue/)
[![Python](https://img.shields.io/pypi/pyversions/equeue)](https://pypi.org/project/equeue/) -->

**Persistent, crash-safe job queue for Python. No broker. No daemon. Just disk.**

Jobs survive process restarts. Workers run as threads or separate processes on the same machine. Powered by [LMDB](https://lmdb.readthedocs.io/), a memory-mapped database that writes atomically and recovers instantly after a crash.

---

## Install

Not published yet.

---

## Usage

### Basic

```python
from equeue import Queue

with Queue("./myqueue") as q:
    q.put({"task": "send_email", "to": "user@example.com"})

    record = q.get(timeout=5.0)
    try:
        send(record.payload)
        record.ack()       # job done
    except Exception:
        record.nack()      # re-queue, or mark FAILED after too many retries
```

### Context manager (auto ack / nack)

```python
with Queue("./myqueue") as q:
    with q.processing(timeout=5.0) as record:
        process(record.payload)
    # ack() on success, nack() on any exception
```

### Async

```python
from equeue import AsyncQueue

async with AsyncQueue("./myqueue") as q:
    await q.put({"task": "resize_image"})

    async with q.processing() as record:
        await handle(record.payload)
```

---

## The Record

`get()` returns a `Record`. Completion always goes through the record, never through a bare job ID. This prevents one worker from accidentally finishing a job held by another.

| Field | Meaning |
| --- | --- |
| `payload` | Job data passed to `put()` |
| `job_id` | Stable integer ID, useful for logs |
| `retries` | How many times this job was nacked |
| `enqueued_at` | Unix timestamp from `put()` |

---

## Configuration

```python
q = Queue(
    "./myqueue",
    lease_time=30.0,        # seconds before an idle job can be re-queued
    max_retries=3,          # nacks before FAILED (0 = one attempt only)
    map_size=2**30,         # LMDB virtual size in bytes (default 1 GiB)
    sync=False,             # True = safer, slower (fsync every write)
    do_recover=True,        # background thread re-queues expired jobs
    recover_interval=15.0,
    do_vacuum=True,         # background thread removes old DONE jobs
    vacuum_interval=300.0,
)
```

---

## Statistics

```python
q.stats()
# {
#     "pending":   4,   # waiting for a worker
#     "running":   1,   # claimed by a worker
#     "done":    120,   # finished successfully
#     "failed":    2,   # exceeded retry limit
#     "total":   127,   # ever enqueued; never decreases
#     "recovered": 3,   # moved back to PENDING by recovery thread
#     "vacuumed":  0,   # DONE jobs removed from disk
# }
```

---

## Job lifecycle

```
PENDING  -->  RUNNING  -->  DONE  -->  (vacuumed)
                  |
                  +--> PENDING   nack() with retries left, or lease expired
                  |
                  +--> FAILED    nack() with no retries left (kept on disk)
```

---

## Exceptions

| Exception | When |
| --- | --- |
| `QueueEmpty` | `get(timeout=...)` found no job in time |
| `QueueClosed` | Operation on a closed queue |
| `QueueCorrupted` | Wrong claim token, double ack, or broken disk state |

---

## Notes

- **At-least-once delivery.** A job can run more than once if a worker crashes or its lease expires. Make handlers idempotent.
- **Payload types.** Values must be msgpack-serializable: dicts, lists, strings, numbers, bytes, `None`.
- **`map_size` is not disk usage.** It is virtual address space. Disk grows as data is written.
- **Not a distributed broker.** EQueue is for one machine. It does not replace Redis or Kafka.

---

## Docs

- [User guide](docs/index.md)
- [RFC: architecture and behaviour contracts](docs/rfc.md)

```bash
uv run pytest              # all tests
uv run pytest -m contract  # RFC contract tests only
```
