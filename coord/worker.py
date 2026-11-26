"""Background worker loops: timeout sweep, reconcile, compensation."""

from __future__ import annotations

import logging
import threading

from .coordinator import Coordinator

log = logging.getLogger("coord.worker")


class Worker:
    def __init__(self, coord: Coordinator, interval: float = 0.05):
        self.coord = coord
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def tick(self) -> dict:
        """One synchronous sweep of all queues. Used by tests and the loop."""
        swept = self.coord.sweep_timeouts()
        reconciled = 0
        while True:
            job = self.coord.claim_job()
            if job is None:
                break
            self.coord.run_reconcile_job(job)
            reconciled += 1
        compensated = 0
        while True:
            comp = self.coord.claim_compensation()
            if comp is None:
                break
            self.coord.run_compensation(comp)
            compensated += 1
        return {"swept": swept, "reconciled": reconciled,
                "compensated": compensated}

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("worker tick failed")
            self._stop.wait(self.interval)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="coord-worker")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
