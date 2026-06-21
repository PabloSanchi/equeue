"""Test helpers for inspecting LMDB state directly."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from equeue.db import (
    CLAIM_TOKEN_LEN,
    PFX_QUEUED,
    PFX_QUEUED_OFFSET,
    PFX_STATE,
    PFX_STATE_OFFSET,
    U32,
    U64,
    JobState,
    key_job,
    key_lease,
    key_retry,
    key_state,
    lease_expiry,
)

if TYPE_CHECKING:
    from equeue import Queue

__all__ = [
    "U32",
    "bogus_token",
    "job_state",
    "key_job",
    "key_lease",
    "key_retry",
    "key_state",
    "pending_job_ids",
    "queued_job_ids",
    "retry_count",
    "running_jobs_with_expired_leases",
]


def job_state(txn: object, job_id: int) -> bytes | None:
    return txn.get(key_state(job_id))  # type: ignore[attr-defined]


def retry_count(txn: object, job_id: int) -> int:
    raw = txn.get(key_retry(job_id))  # type: ignore[attr-defined]
    return U32.unpack(raw)[0] if raw else 0


def bogus_token() -> bytes:
    return b"\x00" * CLAIM_TOKEN_LEN


def running_jobs_with_expired_leases(q: Queue, *, now: float | None = None) -> list[int]:
    now = now if now is not None else time.time()
    expired: list[int] = []

    with q._env.begin() as txn:
        cursor = txn.cursor()
        if not cursor.set_range(PFX_STATE):
            return expired

        while cursor.key().startswith(PFX_STATE):
            job_id = U64.unpack_from(cursor.key(), len(PFX_STATE))[0]
            if cursor.value() != b"R":
                if not cursor.next():
                    break
                continue

            raw_lease = txn.get(key_lease(job_id))
            if raw_lease is None or now > lease_expiry(raw_lease):
                expired.append(job_id)

            if not cursor.next():
                break

    return expired


def queued_job_ids(q: Queue) -> set[int]:
    """Return the set of job IDs present in the queued/ index."""
    ids: set[int] = set()
    with q._env.begin() as txn:
        cursor = txn.cursor()
        if cursor.set_range(PFX_QUEUED) and cursor.key().startswith(PFX_QUEUED):
            while cursor.key().startswith(PFX_QUEUED):
                ids.add(U64.unpack_from(cursor.key(), PFX_QUEUED_OFFSET)[0])
                if not cursor.next():
                    break
    return ids


def pending_job_ids(q: Queue) -> set[int]:
    """Return the set of job IDs whose state/ entry is PENDING."""
    ids: set[int] = set()
    with q._env.begin() as txn:
        cursor = txn.cursor()
        if cursor.set_range(PFX_STATE) and cursor.key().startswith(PFX_STATE):
            while cursor.key().startswith(PFX_STATE):
                if cursor.value() == JobState.PENDING:
                    ids.add(U64.unpack_from(cursor.key(), PFX_STATE_OFFSET)[0])
                if not cursor.next():
                    break
    return ids
