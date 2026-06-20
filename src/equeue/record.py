from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class Record(Generic[T]):
    """
    A claimed job returned by :meth:`Queue.get`.

    The ``Record`` is the only object that
    may complete a job. Completion methods carry an internal **claim token**
    minted at claim time; ``ack()`` / ``nack()`` on this instance succeed
    only while this worker still holds the active lease.

    Args:
        job_id: Monotonic job identifier (for logging and metrics).
        payload: Deserialized job body.
        retries: Times this job has been nacked so far.
        enqueued_at: Unix timestamp from ``put()``.
    """

    job_id: int
    payload: T
    retries: int
    enqueued_at: float
    _finish: Callable[[bool], None] = field(repr=False, compare=False)

    def ack(self) -> None:
        """
        Mark this job as successfully completed.

        Raises:
            QueueClosed: The queue has been closed.
            QueueCorrupted: This record no longer holds the active claim
                (lease expired, re-claimed by another worker, or already completed).
        """
        self._finish(requeue=False)

    def nack(self) -> None:
        """
        Reject this job and apply the retry policy.

        Raises:
            QueueClosed: The queue has been closed.
            QueueCorrupted: This record no longer holds the active claim.
        """
        self._finish(requeue=True)
