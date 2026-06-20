import threading
import time

import pytest
from lmdb_helpers import U32, key_job, key_lease, key_retry, key_state

from equeue import Queue, QueueClosed, QueueEmpty, Record


def test_put_returns_incrementing_job_ids(tmp):
    with Queue(tmp) as q:
        assert q.put("a") == 0
        assert q.put("b") == 1
        assert q.put("c") == 2


def test_get_returns_record(tmp):
    with Queue(tmp) as q:
        q.put("hello")
        record = q.get()

    assert isinstance(record, Record)
    assert record.payload == "hello"
    assert record.job_id == 0


def test_get_returns_correct_job_id_and_payload(tmp):
    with Queue(tmp) as q:
        job_id_put = q.put("a")
        record = q.get()

    assert record.job_id == job_id_put
    assert record.payload == "a"


def test_get_various_payload_types(tmp):
    payloads = [
        42,
        3.14,
        "string",
        {"key": "value", "nested": [1, 2, 3]},
        [1, "two", None],
        True,
    ]

    with Queue(tmp) as q:
        for p in payloads:
            q.put(p)

        for expected in payloads:
            record = q.get()
            assert record.payload == expected


def test_get_blocks_until_item_available(tmp):
    with Queue(tmp, do_recover=False, do_vacuum=False) as q:

        def delayed_put():
            time.sleep(2)
            q.put("delayed")

        producer_thread = threading.Thread(target=delayed_put, daemon=True)

        start = time.monotonic()
        producer_thread.start()

        record = q.get(timeout=3)

        producer_thread.join(timeout=5)

        assert record.payload == "delayed"
        assert time.monotonic() - start < 3
        assert time.monotonic() - start >= 1.5


def test_get_raises_queue_empty_on_timeout(tmp):
    with Queue(tmp, do_recover=False, do_vacuum=False) as q:
        with pytest.raises(QueueEmpty):
            q.get(timeout=0.1)


def test_get_timeout_zero_raises_immediately(tmp):
    with Queue(tmp, do_recover=False, do_vacuum=False) as q:
        with pytest.raises(QueueEmpty):
            q.get(timeout=0)


def test_fifo_order(tmp):
    with Queue(tmp) as q:
        for i in range(5):
            q.put(i)

        results = []

        for _ in range(5):
            record = q.get()
            results.append(record.payload)
            record.ack()

    assert results == list(range(5))


def test_fifo_order_contextmanager(tmp):
    with Queue(tmp) as q:
        for i in range(5):
            q.put(i)

        results = []

        for _ in range(5):
            with q.processing() as record:
                results.append(record.payload)

    assert results == list(range(5))


def test_get_moves_state_to_running(tmp):
    with Queue(tmp) as q:
        q.put("pending->running")

        stats = q.stats()
        assert stats["pending"] == 1
        assert stats["running"] == 0

        _ = q.get()

        stats = q.stats()
        assert stats["pending"] == 0
        assert stats["running"] == 1


def test_ack_moves_state_to_done(tmp):
    with Queue(tmp) as q:
        q.put("to-ack")
        record = q.get()
        record.ack()

        stats = q.stats()
        assert stats["done"] == 1
        assert stats["running"] == 0


def test_ack_keeps_payload_in_lmdb(tmp):
    with Queue(tmp) as q:
        q.put("to-ack")
        record = q.get()
        record.ack()

        with q._env.begin() as txn:
            assert txn.get(key_job(record.job_id)) is not None
            assert txn.get(key_state(record.job_id)) == b"D"
            assert txn.get(key_lease(record.job_id)) is None
            assert txn.get(key_retry(record.job_id)) is None


def test_acked_job_not_recovered_after_reopen(tmp):
    with Queue(tmp, do_recover=False, do_vacuum=False) as q:
        q.put("done-job")
        q.get().ack()

    with Queue(tmp, do_recover=False, do_vacuum=False) as q2:
        s = q2.stats()
        assert s["pending"] == 0
        assert s["running"] == 0
        assert s["done"] == 1


def test_nack_requeues_job(tmp):
    with Queue(tmp, max_retries=3) as q:
        original_id = q.put("failed")
        record = q.get()
        record.nack()

        record2 = q.get()
        assert record2.job_id == original_id
        assert record2.payload == "failed"


def test_nack_increments_retry_counter_in_lmdb(tmp):
    with Queue(tmp, max_retries=3) as q:
        job_id = q.put("retry")

        for retry in range(2):
            record = q.get(timeout=0.01)
            record.nack()

            with q._env.begin() as txn:
                raw = txn.get(key_retry(job_id))
                retries = U32.unpack(raw)[0]

                assert retries == retry + 1

        record = q.get(timeout=0.01)
        record.nack()

        record = q.get(timeout=0.01)
        record.nack()

        with pytest.raises(QueueEmpty):
            q.get(timeout=0.01)

        stats = q.stats()
        assert stats["pending"] == 0
        assert stats["running"] == 0
        assert stats["done"] == 0
        assert stats["failed"] == 1


def test_failed_job_not_replayed_after_recover(tmp):
    with Queue(tmp, max_retries=0, do_recover=False, do_vacuum=False) as q:
        q.put("instant-fail")
        q.get().nack()

    with Queue(tmp, max_retries=0, do_recover=False, do_vacuum=False) as q2:
        assert q2.stats()["failed"] == 1
        assert q2.stats()["pending"] == 0


def test_stats_counts(tmp):
    with Queue(tmp) as q:
        q.put("a")
        q.put("b")
        record = q.get()

        s = q.stats()
        assert s["pending"] == 1
        assert s["running"] == 1
        assert s["done"] == 0
        assert s["failed"] == 0

        record.ack()

        s = q.stats()
        assert s["pending"] == 1
        assert s["running"] == 0
        assert s["done"] == 1


def test_stats_initial_state(tmp):
    with Queue(tmp, do_recover=False, do_vacuum=False) as q:
        s = q.stats()
        assert s["pending"] == 0
        assert s["running"] == 0
        assert s["done"] == 0
        assert s["failed"] == 0
        assert s["total"] == 0
        assert s["recovered"] == 0
        assert s["vacuumed"] == 0


def test_processing_acks_on_success(tmp):
    with Queue(tmp) as q:
        q.put("ok")

        with q.processing() as record:
            assert record.payload == "ok"

        assert q.stats()["done"] == 1
        assert q.stats()["running"] == 0


def test_processing_nacks_on_exception(tmp):
    with Queue(tmp, max_retries=3) as q:
        q.put("boom")

        with pytest.raises(ValueError):
            with q.processing():
                raise ValueError("boom")

        assert q.stats()["pending"] == 1
        assert q.stats()["running"] == 0


def test_processing_reraises_exception(tmp):
    with Queue(tmp, max_retries=2) as q:
        q.put("x")

        with pytest.raises(RuntimeError, match="expected"):
            with q.processing():
                raise RuntimeError("expected")

        assert q.stats()["pending"] == 1


def test_pending_jobs_survive_restart(tmp):
    q = Queue(tmp, do_recover=False, do_vacuum=False)
    q.put("survive-me")
    q._env.close()

    with Queue(tmp, do_recover=False, do_vacuum=False) as q2:
        assert q2.stats()["pending"] == 1
        record = q2.get()
        assert record.payload == "survive-me"


def test_running_job_recovered_after_expired_lease(tmp):
    q = Queue(tmp, lease_time=0.5, do_recover=False, do_vacuum=False)
    job_id = q.put("leaky-job")
    q.get()
    q._env.close()

    time.sleep(1)

    with Queue(tmp, lease_time=0.5, do_recover=False, do_vacuum=False) as q2:
        assert q2.stats()["pending"] == 1
        record = q2.get()
        assert record.job_id == job_id
        assert record.payload == "leaky-job"


def test_partial_completion_replay(tmp):
    q = Queue(tmp, do_recover=False, do_vacuum=False)
    for i in range(5):
        q.put(i)
    for _ in range(2):
        q.get().ack()
    q._env.close()

    with Queue(tmp, do_recover=False, do_vacuum=False) as q2:
        s = q2.stats()
        assert s["pending"] == 3
        assert s["done"] == 2


def test_requeue_expired_recovers_stale_lease(tmp):
    with Queue(tmp, lease_time=0.1, do_recover=False, do_vacuum=False) as q:
        q.put("expire-me")
        q.get()
        assert q.stats()["running"] == 1

        time.sleep(0.5)

        recovered = q._requeue_expired()
        assert recovered == 1
        assert q.stats()["running"] == 0
        assert q.stats()["pending"] == 1


def test_vacuum_removes_done_records(tmp):
    with Queue(tmp, do_recover=False, do_vacuum=False) as q:
        q.put("clean-me")
        record = q.get()
        record.ack()

        q._vacuum()

        with q._env.begin() as txn:
            assert txn.get(key_job(record.job_id)) is None
            assert txn.get(key_state(record.job_id)) is None

        stats = q.stats()
        assert stats["running"] == 0
        assert stats["done"] == 0
        assert stats["vacuumed"] == 1


def test_vacuum_retains_failed_records(tmp):
    """Vacuum purges DONE jobs only; FAILED records are preserved for inspection."""
    with Queue(tmp, max_retries=0, do_recover=False, do_vacuum=False) as q:
        q.put("fail-me")
        q.get().nack()

        q._vacuum()

        with q._env.begin() as txn:
            assert txn.get(key_job(0)) is not None, "FAILED payload must be retained"
            assert txn.get(key_state(0)) is not None, "FAILED state must be retained"

        stats = q.stats()
        assert stats["running"] == 0
        assert stats["pending"] == 0
        assert stats["failed"] == 1
        assert stats["vacuumed"] == 0
        assert stats["done"] == 0


def test_concurrent_workers_no_duplicate_delivery(tmp):
    N = 50
    seen = []
    lock = threading.Lock()

    with Queue(tmp) as q:
        for i in range(N):
            q.put(i)

        def worker():
            while True:
                try:
                    record = q.get(timeout=1.0)
                    with lock:
                        seen.append(record.payload)
                    record.ack()
                except QueueEmpty:
                    break

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert len(seen) == N
    assert sorted(seen) == list(range(N))


def test_closed_queue_raises_on_put(tmp):
    q = Queue(tmp)
    q.close()
    with pytest.raises(QueueClosed):
        q.put("x")


def test_closed_queue_raises_on_get(tmp):
    q = Queue(tmp)
    q.close()
    with pytest.raises(QueueClosed):
        q.get()


def test_record_ack_after_close_raises(tmp):
    q = Queue(tmp, do_recover=False, do_vacuum=False)
    q.put("in-flight")
    record = q.get()
    q.close()
    with pytest.raises(QueueClosed):
        record.ack()


def test_close_is_idempotent(tmp):
    q = Queue(tmp)
    q.close()
    q.close()


def test_context_manager_closes_on_exit(tmp):
    with Queue(tmp) as q:
        q.put("x")
    assert q._closed


if __name__ == "__main__":
    pytest.main([__file__])
