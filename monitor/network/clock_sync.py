"""Public futures REST clock estimate; never changes the operating system clock."""
from dataclasses import dataclass
import math
import threading
import time

import requests


@dataclass(frozen=True)
class ClockEstimate:
    server_ms: float
    midpoint_mono: float
    offset_ms: float  # server minus local wall clock, at calibration
    rtt_ms: float

    def event_lag(self, received_mono, event_ms):
        return self.server_ms + (received_mono - self.midpoint_mono) * 1000 - event_ms


class ServerClock:
    def __init__(self, runtime, diagnostics):
        self.runtime, self.diagnostics = runtime, diagnostics
        self.estimate = None
        self.error = ""
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, name="server-clock", daemon=True)

    def start(self):
        self.thread.start()

    def current(self):
        estimate = self.estimate
        if estimate is None or time.monotonic() - estimate.midpoint_mono > self.runtime.current.clock_sync_max_age_seconds:
            return None
        return estimate

    def synchronize(self, session):
        candidates = []
        for _ in range(3):
            if self.stop.is_set():
                break
            start_mono, start_wall = time.monotonic(), time.time() * 1000
            response = session.get("https://fapi.binance.com/fapi/v1/time", timeout=(2, 2))
            response.raise_for_status()
            server_ms = float(response.json()["serverTime"])
            end_mono, end_wall = time.monotonic(), time.time() * 1000
            rtt = (end_mono - start_mono) * 1000
            if (not math.isfinite(server_ms) or server_ms <= 0
                    or abs((end_wall - start_wall) - rtt) > 100
                    or rtt > self.runtime.current.clock_sync_max_rtt_ms):
                continue
            candidates.append(ClockEstimate(server_ms, (start_mono + end_mono) / 2,
                                            server_ms - (start_wall + end_wall) / 2, rtt))
        if not candidates:
            raise ValueError("无合格对时样本：RTT过大、系统时钟跳变或响应无效")
        self.estimate = min(candidates, key=lambda item: item.rtt_ms)
        self.error = ""
        self.diagnostics.emit("clock_sync", offset_ms=self.estimate.offset_ms,
                              rtt_ms=self.estimate.rtt_ms, samples=len(candidates),
                              server_ms=self.estimate.server_ms)

    def _run(self):
        with requests.Session() as session:
            while not self.stop.is_set():
                try:
                    self.synchronize(session)
                    delay = self.runtime.current.clock_sync_interval_seconds
                except Exception as exc:
                    self.error = f"{type(exc).__name__}: {exc}"
                    self.diagnostics.emit("clock_sync_error", error=self.error)
                    delay = 10
                self.stop.wait(delay)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
