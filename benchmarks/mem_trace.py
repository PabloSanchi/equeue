"""Python heap usage per in-flight Record, via tracemalloc."""

from __future__ import annotations

import tracemalloc

from _bench import PAYLOAD, format_table, temp_queue

N = 1000


def _heap_bytes(snapshot: tracemalloc.Snapshot) -> int:
    return sum(stat.size for stat in snapshot.statistics("lineno"))


def _kb(n_bytes: float) -> str:
    return f"{n_bytes / 1024:.1f} KB"


def main() -> None:
    with temp_queue(do_recover=False, do_vacuum=False, sync=False) as q:
        tracemalloc.start()
        baseline = tracemalloc.take_snapshot()

        for _ in range(N):
            q.put(PAYLOAD)
        after_puts = tracemalloc.take_snapshot()

        records = [q.get() for _ in range(N)]
        after_gets = tracemalloc.take_snapshot()

        for record in records:
            record.ack()
        records.clear()
        after_acks = tracemalloc.take_snapshot()
        tracemalloc.stop()

        base = _heap_bytes(baseline)
        puts = _heap_bytes(after_puts)
        gets = _heap_bytes(after_gets)
        acks = _heap_bytes(after_acks)
        per_record = (gets - puts) / N

        print(f"Python heap (tracemalloc), N={N} jobs\n")
        print(
            format_table(
                ["stage", "heap", "delta vs baseline"],
                [
                    ["baseline", _kb(base), _kb(0)],
                    [f"after {N} puts", _kb(puts), _kb(puts - base)],
                    [f"after {N} gets (held)", _kb(gets), _kb(gets - base)],
                    [f"after {N} acks (freed)", _kb(acks), _kb(acks - base)],
                ],
            )
        )
        print(f"\napprox per held Record: {per_record:.0f} bytes")

        print(f"\ntop allocations while {N} records were held:")
        for stat in after_gets.compare_to(baseline, "lineno")[:5]:
            print(f"  {stat}")


if __name__ == "__main__":
    main()
