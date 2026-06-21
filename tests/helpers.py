"""Multiprocessing test utilities."""

from __future__ import annotations

import multiprocessing
import traceback
from multiprocessing.synchronize import Event

from equeue import Queue

__all__ = ["ManagedProcess", "consumer_worker", "producer_worker"]


def producer_worker(tmp: str, flag: Event) -> None:
    """Producer worker: enqueues one job and signals readiness."""
    with Queue[str](path=tmp, max_retries=0, do_recover=False, do_vacuum=False) as q:
        q.put("rfc_mp_01: producer")
        flag.set()


def consumer_worker(tmp: str, flag: Event) -> None:
    """Consumer worker: waits for the signal, claims and nacks the job."""
    flag.wait()
    with Queue[str](path=tmp, max_retries=0, do_recover=False, do_vacuum=False) as q:
        record = q.get(timeout=1)
        assert record.payload == "rfc_mp_01: producer"
        record.nack()


class ManagedProcess(multiprocessing.Process):
    """
    multiprocessing.Process wrapper that forwards child exceptions to the
    parent pytest process so assertion failures surface in the test output.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._exception_queue: multiprocessing.Queue = multiprocessing.Queue()

    def run(self) -> None:
        try:
            super().run()
            self._exception_queue.put(None)
        except Exception as e:
            self._exception_queue.put((e, traceback.format_exc()))

    def join(self, timeout: float | None = None) -> None:
        """Join and re-raise any child exception in the parent process."""
        super().join(timeout)
        if not self._exception_queue.empty():
            exception_data = self._exception_queue.get_nowait()
            if exception_data:
                exc, tb_str = exception_data
                raise exc from RuntimeError(f"Child process traceback:\n{tb_str}")

    def join_and_get(self, timeout: float | None = None) -> Exception | None:
        """Join and return any child exception instead of re-raising."""
        super().join(timeout)
        if not self._exception_queue.empty():
            exception_data = self._exception_queue.get_nowait()
            if exception_data:
                exc, _tb_str = exception_data
                return exc
        return None
