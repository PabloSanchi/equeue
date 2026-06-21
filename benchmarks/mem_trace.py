"""Memory profiling: Python heap usage via tracemalloc."""

import shutil
import tempfile
import tracemalloc

from equeue import Queue

N = 1000
PAYLOAD = "benchmark-payload"


def total_bytes(snapshot):
    return sum(s.size for s in snapshot.statistics("lineno"))


def fmt(n_bytes):
    return f"{n_bytes / 1024:.1f} KB"


def main():
    tmp = tempfile.mkdtemp()
    q = Queue(tmp, do_recover=False, do_vacuum=False, sync=False)

    tracemalloc.start()

    snap0 = tracemalloc.take_snapshot()

    for _ in range(N):
        q.put(PAYLOAD)
    snap_puts = tracemalloc.take_snapshot()

    records = [q.get() for _ in range(N)]
    snap_gets = tracemalloc.take_snapshot()

    for r in records:
        r.ack()
    records.clear()
    snap_acks = tracemalloc.take_snapshot()

    tracemalloc.stop()
    q.close()
    shutil.rmtree(tmp, ignore_errors=True)

    b0 = total_bytes(snap0)
    b_put = total_bytes(snap_puts)
    b_get = total_bytes(snap_gets)
    b_ack = total_bytes(snap_acks)
    per_record = (b_get - b_put) / N
    diff = snap_gets.compare_to(snap0, "lineno")

    print(
        f"Python heap (tracemalloc) - N={N} jobs\n"
        f"note: LMDB memory-mapped pages are not tracked here\n"
        f"\n"
        f"baseline\t{fmt(b0)}\n"
        f"after {N} puts\t{fmt(b_put)}\t(+{fmt(b_put - b0)})\n"
        f"after {N} gets\t{fmt(b_get)}\t(+{fmt(b_get - b0)})\n"
        f"per Record\t{per_record:.0f} bytes\n"
        f"after {N} acks\t{fmt(b_ack)}\t(+{fmt(b_ack - b0)})\n"
        f"\n"
        f"top allocations while {N} records held:"
    )
    for stat in diff[:5]:
        print(f"\t{stat}")


main()
