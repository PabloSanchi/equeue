import multiprocessing
import traceback

__all__ = ["ManagedProcess"]


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
