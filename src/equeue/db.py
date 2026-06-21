"""LMDB schema: key layout, structs, serialization, lease encoding, and transaction helpers."""

from __future__ import annotations

import secrets
import struct
import time
from dataclasses import dataclass
from typing import Any

import msgpack

from .exceptions import QueueCorrupted

_7U64 = struct.Struct("<7Q")


@dataclass(slots=True)
class Stats:
    """
    All queue counters stored as a single LMDB entry.

    Reading or writing stats is one O(1) LMDB operation.
    All fields default to zero and are updated atomically inside write transactions.
    """

    pending: int = 0
    running: int = 0
    done: int = 0
    failed: int = 0
    total: int = 0
    recovered: int = 0
    vacuumed: int = 0

    def to_dict(self) -> dict[str, int]:
        """Return a plain dict suitable for external use."""
        return {
            "pending": self.pending,
            "running": self.running,
            "done": self.done,
            "failed": self.failed,
            "total": self.total,
            "recovered": self.recovered,
            "vacuumed": self.vacuumed,
        }

    def pack(self) -> bytes:
        """Serialize all counters to bytes for storage in LMDB."""
        return _7U64.pack(
            self.pending,
            self.running,
            self.done,
            self.failed,
            self.total,
            self.recovered,
            self.vacuumed,
        )

    @classmethod
    def unpack(cls, data: bytes) -> Stats:
        """Deserialize bytes read from LMDB into a Stats instance."""
        return cls(*_7U64.unpack(data))


class JobState:
    PENDING = b"P"
    RUNNING = b"R"
    DONE = b"D"
    FAILED = b"F"


# Struct formats
U64 = struct.Struct(">Q")
F64 = struct.Struct("<d")
U32 = struct.Struct("<I")

# Key prefixes
PFX_JOB: bytes = b"job/"
PFX_STATE: bytes = b"state/"
PFX_LEASE: bytes = b"lease/"
PFX_RETRY: bytes = b"retry/"
PFX_QUEUED: bytes = b"queued/"

# Meta keys
META_TAIL: bytes = b"meta/tail"
META_STATS: bytes = b"meta/stats"

# Derived offsets
PFX_STATE_OFFSET = len(PFX_STATE)
PFX_LEASE_OFFSET = len(PFX_LEASE)
PFX_QUEUED_OFFSET = len(PFX_QUEUED)
JOB_TIMESTAMP_SIZE = F64.size  # bytes reserved for enqueued_at at the start of job/ values

EMPTY = b""


def key_job(job_id: int) -> bytes:
    return PFX_JOB + U64.pack(job_id)


def key_state(job_id: int) -> bytes:
    return PFX_STATE + U64.pack(job_id)


def key_lease(job_id: int) -> bytes:
    return PFX_LEASE + U64.pack(job_id)


def key_retry(job_id: int) -> bytes:
    return PFX_RETRY + U64.pack(job_id)


def key_queued(job_id: int) -> bytes:
    return PFX_QUEUED + U64.pack(job_id)


# Cursor helpers


def parse_state_cursor(cursor: Any) -> tuple[int, bytes]:
    """Extract (job_id, state) from a cursor positioned at a ``state/`` key."""
    job_id = U64.unpack_from(cursor.key(), offset=PFX_STATE_OFFSET)[0]
    return job_id, cursor.value()


def meta_get(txn: Any, key: bytes) -> int:
    raw = txn.get(key)
    return U64.unpack(raw)[0] if raw else 0


def meta_set(txn: Any, key: bytes, val: int) -> None:
    txn.put(key, U64.pack(val))


def stats_get(txn: Any) -> Stats:
    raw = txn.get(META_STATS)
    if not raw:
        raise QueueCorrupted("Queue stats are not defined")
    return Stats.unpack(raw)


def stats_set(txn: Any, stats: Stats) -> None:
    txn.put(META_STATS, stats.pack())


def txn_set_pending(txn: Any, job_id: int) -> None:
    """Transition job to PENDING and insert it into the queued/ index."""
    txn.put(key_state(job_id), JobState.PENDING)
    txn.put(key_queued(job_id), EMPTY)


def txn_set_running(txn: Any, job_id: int, packed_lease: bytes) -> None:
    """Transition job to RUNNING, write the lease, and remove it from the queued/ index."""
    txn.put(key_state(job_id), JobState.RUNNING)
    txn.put(key_lease(job_id), packed_lease)
    txn.delete(key_queued(job_id))


def txn_set_done(txn: Any, job_id: int) -> None:
    """Transition job to DONE and clear its retry counter."""
    txn.put(key_state(job_id), JobState.DONE)
    txn.delete(key_retry(job_id))


def txn_set_failed(txn: Any, job_id: int) -> None:
    """Transition job to FAILED (payload and retry count are kept for inspection)."""
    txn.put(key_state(job_id), JobState.FAILED)


def encode(payload: Any) -> bytes:
    try:
        return msgpack.packb(payload, use_bin_type=True)
    except Exception as exc:
        raise TypeError(f"payload is not msgpack-serialisable: {type(payload).__name__}") from exc


def decode(data: bytes) -> Any:
    return msgpack.unpackb(data, raw=False)


CLAIM_TOKEN_LEN = 16
_LEASE_HEADER_LEN = F64.size


def new_claim_token() -> bytes:
    return secrets.token_bytes(CLAIM_TOKEN_LEN)


def pack_lease(expiry: float, claim_token: bytes) -> bytes:
    if len(claim_token) != CLAIM_TOKEN_LEN:
        raise ValueError(f"claim token must be {CLAIM_TOKEN_LEN} bytes")
    return F64.pack(expiry) + claim_token


def lease_expiry(raw: bytes) -> float:
    return F64.unpack(raw[:_LEASE_HEADER_LEN])[0]


def lease_claim_token(raw: bytes) -> bytes:
    return raw[_LEASE_HEADER_LEN : _LEASE_HEADER_LEN + CLAIM_TOKEN_LEN]


def new_lease(lease_time: float, *, now: float | None = None) -> tuple[bytes, bytes]:
    """Return ``(packed_lease_bytes, claim_token)`` for a new claim."""
    now = time.time() if now is None else now
    token = new_claim_token()
    return pack_lease(now + lease_time, token), token
