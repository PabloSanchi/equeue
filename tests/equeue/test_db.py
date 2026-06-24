"""Unit tests for the internal ``equeue.db`` layer (pure, LMDB-free helpers)."""

from __future__ import annotations

from typing import Any

import pytest

from equeue.db import (
    CLAIM_TOKEN_LEN,
    U64,
    JobState,
    Stats,
    decode,
    encode,
    key_job,
    key_lease,
    key_queued,
    key_retry,
    key_state,
    lease_claim_token,
    lease_expiry,
    new_lease,
    pack_lease,
)


class TestCodec:
    @pytest.mark.parametrize(
        "value",
        [
            42,
            -7,
            3.14,
            "text",
            b"\x00\x01\x02",
            True,
            None,
            [1, "two", None],
            {"k": [1, 2], "nested": {"x": 1}},
        ],
    )
    def test_round_trip(self, value: Any) -> None:
        assert decode(encode(value)) == value

    def test_unserialisable_payload_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            encode(object())


class TestKeys:
    @pytest.mark.parametrize(
        ("builder", "prefix"),
        [
            (key_job, b"job/"),
            (key_state, b"state/"),
            (key_lease, b"lease/"),
            (key_retry, b"retry/"),
            (key_queued, b"queued/"),
        ],
    )
    def test_key_layout_is_prefix_plus_packed_id(self, builder: Any, prefix: bytes) -> None:
        key = builder(258)
        assert key.startswith(prefix)
        assert key[len(prefix) :] == U64.pack(258)

    def test_keys_sort_in_numeric_job_id_order(self) -> None:
        # big-endian U64 makes lexicographic byte order match numeric order,
        # which is what gives the queued/ index its FIFO scan property.
        ids = [0, 1, 2, 255, 256, 65_535, 1_000_000]
        keys = [key_job(i) for i in ids]
        assert keys == sorted(keys)


class TestLease:
    def test_new_lease_round_trip(self) -> None:
        packed, token = new_lease(30.0, now=1000.0)
        assert len(token) == CLAIM_TOKEN_LEN
        assert lease_claim_token(packed) == token
        assert lease_expiry(packed) == pytest.approx(1030.0)

    def test_tokens_are_unique(self) -> None:
        _, token_a = new_lease(1.0)
        _, token_b = new_lease(1.0)
        assert token_a != token_b

    def test_pack_lease_rejects_wrong_token_length(self) -> None:
        with pytest.raises(ValueError):
            pack_lease(1.0, b"short")


class TestStats:
    def test_pack_unpack_round_trip(self) -> None:
        stats = Stats(pending=1, running=2, done=3, failed=4, total=10)
        assert Stats.unpack(stats.pack()) == stats

    def test_to_dict_exposes_all_counters(self) -> None:
        assert set(Stats().to_dict()) == {"pending", "running", "done", "failed", "total"}


class TestJobState:
    def test_values_are_distinct_single_bytes(self) -> None:
        values = [state.value for state in JobState]
        assert all(isinstance(v, bytes) and len(v) == 1 for v in values)
        assert len(set(values)) == len(values)

    def test_members_compare_equal_to_raw_bytes(self) -> None:
        # LMDB returns plain bytes; the bytes mixin keeps direct comparison valid.
        assert JobState.PENDING == b"P"
        assert b"R" == JobState.RUNNING
