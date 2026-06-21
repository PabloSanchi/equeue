"""
Contract tests for EQueue behaviour.

Each test is tagged with an RFC marker (see docs/rfc.md section 9).
Run all: ``pytest -m contract -v``
Run one:  ``pytest -m rfc_rec_01 -v``
"""

from __future__ import annotations

import multiprocessing
import time

import pytest
from helpers import ManagedProcess, consumer_worker, producer_worker
from lmdb_helpers import (
    bogus_token,
    job_state,
    key_job,
    key_lease,
    key_retry,
    key_state,
    pending_job_ids,
    queued_job_ids,
    retry_count,
    running_jobs_with_expired_leases,
)

from equeue import Queue, QueueClosed, QueueCorrupted, QueueEmpty

pytestmark = pytest.mark.contract


class TestAckNackStateMachine:
    """State machine rules (RFC-SM-*)."""

    @pytest.mark.rfc_sm_01
    def test_ack_requires_running_state(self, quiet_queue: Queue) -> None:
        """RFC-SM-01: cannot complete a PENDING job."""
        job_id = quiet_queue.put("not-yet-claimed")

        with pytest.raises(QueueCorrupted):
            quiet_queue._finish(job_id, bogus_token(), requeue=False)

        assert quiet_queue.stats()["pending"] == 1
        assert quiet_queue.stats()["running"] == 0

    @pytest.mark.rfc_sm_02
    def test_nack_requires_running_state(self, quiet_queue: Queue) -> None:
        """RFC-SM-02: cannot nack a PENDING job."""
        job_id = quiet_queue.put("not-yet-claimed")

        with pytest.raises(QueueCorrupted):
            quiet_queue._finish(job_id, bogus_token(), requeue=True)

        stats = quiet_queue.stats()
        assert stats["pending"] == 1
        assert stats["running"] == 0
        assert stats["failed"] == 0

    @pytest.mark.rfc_sm_03
    def test_nack_on_failed_is_rejected(self, quiet_queue: Queue) -> None:
        """RFC-SM-03: cannot nack a FAILED job."""
        quiet_queue.put("doomed")
        record = quiet_queue.get()

        for _ in range(quiet_queue._max_retries + 1):
            record.nack()
            try:
                record = quiet_queue.get(timeout=0.1)
            except QueueEmpty:
                break

        assert quiet_queue.stats()["failed"] == 1

        with pytest.raises(QueueCorrupted):
            record.nack()


class TestAckNackIdempotency:
    """Completion idempotency rules (RFC-ID-*)."""

    @pytest.mark.rfc_id_01
    def test_double_ack_raises_queue_corrupted(self, quiet_queue: Queue) -> None:
        """RFC-ID-01: second ack on the same record raises QueueCorrupted."""
        quiet_queue.put("done-once")
        record = quiet_queue.get()
        record.ack()

        with pytest.raises(QueueCorrupted):
            record.ack()

        assert quiet_queue.stats()["done"] == 1
        assert quiet_queue.stats()["running"] == 0

    @pytest.mark.rfc_id_02
    def test_ack_after_vacuumed_job_raises(self, quiet_queue: Queue) -> None:
        """RFC-ID-02: ack after vacuum removed the job raises QueueCorrupted."""
        quiet_queue.put("vacuumed")
        record = quiet_queue.get()
        record.ack()
        quiet_queue._vacuum()

        with pytest.raises(QueueCorrupted):
            record.ack()


class TestRecordClaimToken:
    """Record claim-token rules (RFC-REC-*)."""

    @pytest.mark.rfc_rec_01
    def test_stale_record_after_reclaim_is_rejected(self, tmp: str) -> None:
        """RFC-REC-01: stale record cannot ack after another worker re-claimed."""
        q = Queue(
            tmp,
            lease_time=0.2,
            do_recover=True,
            recover_interval=0.15,
            do_vacuum=False,
        )
        job_id = q.put("shared")
        record_a = q.get()
        assert record_a.job_id == job_id

        time.sleep(0.5)

        record_b = q.get(timeout=1.0)
        assert record_b.job_id == job_id

        with pytest.raises(QueueCorrupted):
            record_a.ack()

        with q._env.begin() as txn:
            assert job_state(txn, job_id) == b"R"

        assert q.stats()["done"] == 0
        q.close()

    @pytest.mark.rfc_rec_02
    def test_wrong_token_raises_queue_corrupted(self, quiet_queue: Queue) -> None:
        """RFC-REC-02: wrong claim token raises QueueCorrupted."""
        quiet_queue.put("token-check")
        record = quiet_queue.get()
        with pytest.raises(QueueCorrupted):
            quiet_queue._finish(record.job_id, bogus_token(), requeue=False)


class TestRetrySemantics:
    """Retry policy rules (RFC-RT-*)."""

    @pytest.mark.rfc_rt_01
    @pytest.mark.parametrize("max_retries", [0, 1, 3])
    def test_failed_after_max_retries_plus_one_nacks(self, tmp: str, max_retries: int) -> None:
        """RFC-RT-01: job fails after max_retries + 1 nacks."""
        with Queue(tmp, max_retries=max_retries, do_recover=False, do_vacuum=False) as q:
            job_id = q.put("retry-me")

            for _ in range(max_retries + 1):
                record = q.get(timeout=0.5)
                assert record.job_id == job_id
                record.nack()

            with pytest.raises(QueueEmpty):
                q.get(timeout=0.1)

            assert q.stats()["failed"] == 1
            assert q.stats()["pending"] == 0
            assert q.stats()["running"] == 0

            with q._env.begin() as txn:
                assert job_state(txn, job_id) == b"F"
                assert retry_count(txn, job_id) == max_retries + 1

    @pytest.mark.rfc_rt_02
    def test_requeue_increments_retry_counter(self, quiet_queue: Queue) -> None:
        """RFC-RT-02: each re-queue nack increments retry count."""
        quiet_queue.put("count-retries")
        record = quiet_queue.get()
        record.nack()

        with quiet_queue._env.begin() as txn:
            assert job_state(txn, record.job_id) == b"P"
            assert retry_count(txn, record.job_id) == 1


class TestPersistenceContracts:
    """Persistence and vacuum rules (RFC-PS-*)."""

    @pytest.mark.rfc_ps_01
    def test_ack_keeps_payload_and_state_in_lmdb(self, quiet_queue: Queue) -> None:
        """RFC-PS-01: ack keeps payload and DONE state on disk."""
        quiet_queue.put("keep-me")
        record = quiet_queue.get()
        record.ack()

        with quiet_queue._env.begin() as txn:
            assert txn.get(key_job(record.job_id)) is not None
            assert job_state(txn, record.job_id) == b"D"
            assert txn.get(key_lease(record.job_id)) is None
            assert txn.get(key_retry(record.job_id)) is None

    @pytest.mark.rfc_ps_02
    def test_vacuum_removes_done_records_only(self, quiet_queue: Queue) -> None:
        """RFC-PS-02: vacuum removes DONE jobs only."""
        done_id = quiet_queue.put("done-job")
        failed_id = quiet_queue.put("failed-job")

        quiet_queue.get().ack()

        record = quiet_queue.get()
        assert record.job_id == failed_id
        for _ in range(quiet_queue._max_retries + 1):
            record.nack()
            try:
                record = quiet_queue.get(timeout=0.1)
            except QueueEmpty:
                break

        quiet_queue._vacuum()

        with quiet_queue._env.begin() as txn:
            assert txn.get(key_job(done_id)) is None
            assert txn.get(key_state(done_id)) is None
            assert txn.get(key_job(failed_id)) is not None
            assert job_state(txn, failed_id) == b"F"

    @pytest.mark.rfc_ps_03
    def test_vacuum_decrements_done_counter(self, quiet_queue: Queue) -> None:
        """RFC-PS-03: vacuum lowers the done counter."""
        quiet_queue.put("to-vacuum")
        quiet_queue.get().ack()

        assert quiet_queue.stats()["done"] == 1
        quiet_queue._vacuum()
        assert quiet_queue.stats()["done"] == 0


class TestRecoveryContracts:
    """Recovery rules (RFC-RC-*)."""

    @pytest.mark.rfc_rc_01
    def test_no_expired_running_leases_after_recover(self, tmp: str) -> None:
        """RFC-RC-01: recover clears expired RUNNING leases."""
        q = Queue(tmp, lease_time=0.1, do_recover=False, do_vacuum=False)
        q.put("expire-me")
        q.get()
        time.sleep(0.3)

        q._recover()

        assert running_jobs_with_expired_leases(q) == []
        q.close()

    @pytest.mark.rfc_rc_02
    def test_running_without_lease_recovered_by_daemon(self, tmp: str) -> None:
        """RFC-RC-02: recovery thread fixes jobs with missing lease keys."""
        q = Queue(
            tmp,
            lease_time=30.0,
            do_recover=True,
            recover_interval=0.2,
            do_vacuum=False,
        )
        q.put("orphan-lease")
        record = q.get()

        with q._env.begin(write=True) as txn:
            txn.delete(key_lease(record.job_id))

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if q.stats()["pending"] >= 1 and q.stats()["running"] == 0:
                break
            time.sleep(0.1)

        assert q.stats()["running"] == 0
        assert q.stats()["pending"] >= 1
        q.close()


class TestClaimContracts:
    """Claim safety rules (RFC-CL-*)."""

    @pytest.mark.rfc_cl_01
    def test_claim_on_missing_payload_raises_queue_corrupted(self, quiet_queue: Queue) -> None:
        """RFC-CL-01: missing payload on disk raises QueueCorrupted."""
        job_id = quiet_queue.put("ghost")

        with quiet_queue._env.begin(write=True) as txn:
            txn.delete(key_job(job_id))

        with pytest.raises(QueueCorrupted):
            quiet_queue._claim(job_id)


class TestStatsInvariants:
    """Statistics rules (RFC-ST-*)."""

    REQUIRED_STATS_KEYS = frozenset(
        {"pending", "running", "done", "failed", "total", "recovered", "vacuumed"}
    )

    @pytest.mark.rfc_st_01
    def test_stats_exposes_all_documented_keys(self, quiet_queue: Queue) -> None:
        """RFC-ST-01: stats returns all documented keys."""
        quiet_queue.put("x")
        stats = quiet_queue.stats()
        assert self.REQUIRED_STATS_KEYS <= stats.keys()

    @pytest.mark.rfc_st_02
    def test_counters_never_negative(self, quiet_queue: Queue) -> None:
        """RFC-ST-02: no counter goes negative."""
        job_id = quiet_queue.put("counter-check")

        with pytest.raises(QueueCorrupted):
            quiet_queue._finish(job_id, bogus_token(), requeue=True)

        record = quiet_queue.get()
        record.nack()

        stats = quiet_queue.stats()
        for key, value in stats.items():
            if key in (
                "pending",
                "running",
                "done",
                "failed",
                "total",
                "recovered",
                "vacuumed",
            ):
                assert value >= 0, f"stats[{key!r}] = {value}"

    @pytest.mark.rfc_st_03
    def test_lifecycle_sum_matches_total(self, quiet_queue: Queue) -> None:
        """RFC-ST-03: pending + running + done + failed equals total."""
        for i in range(4):
            quiet_queue.put(i)

        quiet_queue.get().ack()
        quiet_queue.get().nack()

        stats = quiet_queue.stats()
        lifecycle = stats["pending"] + stats["running"] + stats["done"] + stats["failed"]
        assert lifecycle == stats["total"]


class TestCloseContracts:
    """Shutdown rules (RFC-SH-*)."""

    @pytest.mark.rfc_sh_01
    def test_close_is_idempotent(self, tmp: str) -> None:
        """RFC-SH-01: close twice is safe."""
        q = Queue(tmp, do_recover=False, do_vacuum=False)
        q.close()
        q.close()

    @pytest.mark.rfc_sh_02
    def test_operations_after_close_raise_queue_closed(self, tmp: str) -> None:
        """RFC-SH-02: operations after close raise QueueClosed."""
        q = Queue(tmp, do_recover=False, do_vacuum=False)
        q.put("last")
        record = q.get()
        q.close()

        with pytest.raises(QueueClosed):
            q.put("too-late")

        with pytest.raises(QueueClosed):
            q.get()

        with pytest.raises(QueueClosed):
            record.ack()

        with pytest.raises(QueueClosed):
            record.nack()


class TestMultiProcessingContracts:
    """Multiprocessing rules (RFC-MP-*)."""

    @pytest.mark.rfc_mp_01
    def test_cross_instance_put_get_works(self, tmp: str) -> None:
        """RCF-MP-01: Cross instance put and get successfully"""
        flag = multiprocessing.Event()

        producer = ManagedProcess(target=producer_worker, args=(tmp, flag))
        consumer = ManagedProcess(target=consumer_worker, args=(tmp, flag))

        consumer.start()
        producer.start()

        producer.join()
        consumer.join()

    @pytest.mark.rfc_mp_02
    def test_concurrent_claim_delivers_each_job_exactly_once(self, tmp: str) -> None:
        """RFC-MP-02: Concurrent claim delivers exactly once."""
        flag = multiprocessing.Event()

        producer = ManagedProcess(target=producer_worker, args=(tmp, flag))
        consumer = ManagedProcess(target=consumer_worker, args=(tmp, flag))
        consumer2 = ManagedProcess(target=consumer_worker, args=(tmp, flag))

        consumer.start()
        consumer2.start()
        producer.start()

        producer.join()
        consumer_output = consumer.join_and_get()
        consumer2_output = consumer2.join_and_get()

        if consumer_output is not None and consumer2_output is not None:
            raise AssertionError(
                f"Contract Violation: Both consumers failed.\n"
                f"C1: {repr(consumer_output)}\nC2: {repr(consumer2_output)}"
            )

        if consumer_output is None and consumer2_output is None:
            raise AssertionError("Contract Violation: Both consumers successfully claimed the job")

        failed_exception = consumer_output or consumer2_output
        if not isinstance(failed_exception, QueueEmpty):
            raise AssertionError(
                f"Expected losing consumer to fail with QueueEmpty, "
                f"but got: {repr(failed_exception)}"
            )

    @pytest.mark.rfc_mp_03
    def test_queued_index_is_consistent_with_state_after_recovery(self, tmp: str) -> None:
        """
        RFC-MP-03: queued/ index mirrors state/ PENDING entries exactly after recovery.

        Test simulate a crash, Queue A holds RUNNING jobs with short leases and closes
        without completing them.
         - j_expire will be moved FROM PENDING -> RUNNING (never ack/nack)
         - j_done will be moved FROM PENDING -> RUNNING and then from RUNNING -> DONE (acked)
         - j_pending will be created and never retrieved, PENDING

        Queue B simulates a process restart, _recover() runs and reloads (j_expire and j_pending)
        """
        with Queue(tmp, lease_time=0.05, do_recover=False, do_vacuum=False) as q1:
            j_expire = q1.put("will-expire")
            j_done = q1.put("will-be-done")
            j_pending = q1.put("stays-pending")

            _ = q1.get()
            r_done = q1.get()
            r_done.ack()

        time.sleep(0.2)

        with Queue(tmp, do_recover=False, do_vacuum=False) as q2:
            pending = pending_job_ids(q2)
            queued = queued_job_ids(q2)

            assert queued == pending, (
                f"queued/ out of sync with state/: "
                f"extra={queued - pending}, missing={pending - queued}"
            )
            assert j_expire in queued, f"Job {j_expire} was never recovered from the expired lease."
            assert j_pending in queued, (
                f"Job {j_pending} was never retrieved, should remain in PENDING"
            )
            assert j_done not in queued, f"Job {j_done} was finished, should never be re-queued"

    @pytest.mark.rfc_mp_04
    def test_recover_is_idempotent_across_instances(self, tmp: str) -> None:
        """
        RFC-MP-04: calling _recover() twice must not duplicate queued/ entries or double counters.

        First Queue lets both of its running jobs expire.
        Second Queue  should recover both of its expired jobs once, after subsequent recover
        should take no effect.
        """
        with Queue(tmp, lease_time=0.05, do_recover=False, do_vacuum=False) as q1:
            q1.put("job-a")
            q1.put("job-b")
            q1.get()
            q1.get()

        time.sleep(0.2)

        with Queue(tmp, do_recover=False, do_vacuum=False) as q2:
            queued_after_first = queued_job_ids(q2)
            stats_after_first = q2.stats()

            assert stats_after_first["pending"] == 2
            assert stats_after_first["running"] == 0

            q2._recover()

            queued_after_second = queued_job_ids(q2)
            stats_after_second = q2.stats()

            assert queued_after_second == queued_after_first, (
                "Second _recover() changed the queued/ index"
            )
            assert stats_after_second["pending"] == 2, "Second _recover() changed the pending count"
            assert stats_after_second["running"] == 0, "Second _recover() changed the running count"
