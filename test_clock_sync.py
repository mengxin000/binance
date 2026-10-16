import time
import unittest
from types import SimpleNamespace
from unittest import mock

from monitor.network.clock_sync import ClockEstimate, ServerClock
from monitor.network.core import LatestQuotePipeline, NetworkDiagnostics
from monitor.models import MarketState
from monitor.settings import RuntimeConfig


class ClockTests(unittest.TestCase):
    def setUp(self):
        self.runtime = SimpleNamespace(current=RuntimeConfig(raw_coalesce_interval_ms=20))
        self.diagnostics = NetworkDiagnostics(None, 1000000)

    def test_midpoint_offset_and_lowest_rtt(self):
        clock = ServerClock(self.runtime, self.diagnostics)
        session = mock.Mock()
        session.get.return_value.json.side_effect = [
            {"serverTime": 900200}, {"serverTime": 901050}, {"serverTime": 902100},
        ]
        with mock.patch("monitor.network.clock_sync.time.monotonic",
                        side_effect=[10, 10.4, 11, 11.1, 12, 12.2]), \
             mock.patch("monitor.network.clock_sync.time.time",
                        side_effect=[1000, 1000.4, 1001, 1001.1, 1002, 1002.2]):
            clock.synchronize(session)
        self.assertAlmostEqual(clock.estimate.rtt_ms, 100)
        self.assertAlmostEqual(clock.estimate.offset_ms, -100000)
        self.assertAlmostEqual(clock.estimate.event_lag(11.2, 901100), 100)

    def test_expired_clock_is_not_used(self):
        clock = ServerClock(self.runtime, self.diagnostics)
        clock.estimate = ClockEstimate(1, time.monotonic() - 200, 0, 1)
        self.assertIsNone(clock.current())

    def test_ingress_drops_over_threshold_before_json_or_latest_slot(self):
        now = time.monotonic()
        self.runtime.current = RuntimeConfig(raw_coalesce_interval_ms=1000, max_transport_lag_ms=3000)
        clock = ServerClock(self.runtime, self.diagnostics)
        clock.estimate = ClockEstimate(1000000, now, -10000, 100)
        pipeline = LatestQuotePipeline(self.runtime, self.diagnostics, clock=clock)
        token = object()
        def put(event_time):
            return pipeline.put("futures", "test", token, frozenset({"BTCUSDT"}),
                                ('{"data":{"s":"BTCUSDT","b":"100","a":"101","E":%s}}'
                                 % event_time).encode(), now, 1010000)
        try:
            with mock.patch("monitor.network.core.json.loads") as loads:
                self.assertTrue(put(997000))  # Exactly 3000ms remains allowed.
                stored = pipeline.latest_raw[("futures", "BTCUSDT")]
                self.assertFalse(put(996999))  # RTT does not loosen the threshold.
                self.assertIs(pipeline.latest_raw[("futures", "BTCUSDT")], stored)
                loads.assert_not_called()
            self.assertEqual(pipeline.metrics()["transport_lag"], 1)
            self.assertIsNone(pipeline.pop_fault(token))
        finally:
            pipeline.close()

    def test_large_local_clock_error_does_not_trigger_reconnect(self):
        now = time.monotonic()
        clock = ServerClock(self.runtime, self.diagnostics)
        clock.estimate = ClockEstimate(1000000, now, -10000, 10)
        pipeline = LatestQuotePipeline(self.runtime, self.diagnostics, clock=clock)
        token = object()
        try:
            pipeline.put("futures", "test", token, frozenset({"BTCUSDT"}),
                         b'{"data":{"s":"BTCUSDT","b":"100","a":"101","E":999900}}',
                         now, 1010000)
            deadline = time.monotonic() + 1
            state = MarketState()
            while time.monotonic() < deadline and not state.futures_books:
                time.sleep(.01)
                pipeline.apply_latest(state)
            self.assertAlmostEqual(state.futures_books["BTCUSDT"].transport_lag_ms, 100)
            self.assertIsNone(pipeline.pop_fault(token))
            self.assertEqual(pipeline.metrics()["stage_timings"]["test"]["raw_event_lag_ms"], 10100)
        finally:
            pipeline.close()

    def test_unsynchronized_clock_neither_publishes_nor_reconnects(self):
        clock = ServerClock(self.runtime, self.diagnostics)
        pipeline = LatestQuotePipeline(self.runtime, self.diagnostics, clock=clock)
        token = object()
        try:
            pipeline.put("futures", "test", token, frozenset({"BTCUSDT"}),
                         b'{"data":{"s":"BTCUSDT","b":"100","a":"101","E":1}}',
                         time.monotonic(), time.time() * 1000)
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline and not pipeline.metrics()["clock_unavailable"]:
                time.sleep(.01)
            state = MarketState()
            pipeline.apply_latest(state)
            self.assertFalse(state.futures_books)
            self.assertIsNone(pipeline.pop_fault(token))
            self.assertEqual(pipeline.metrics()["clock_unavailable"], 1)
        finally:
            pipeline.close()
