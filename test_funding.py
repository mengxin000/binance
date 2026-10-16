import time
import unittest
from types import SimpleNamespace
from unittest import mock
from monitor.funding.core import FundingHistoryWorker
from monitor.terminal.pages import funding_snapshot, display_errors

from main import (
    BookQuote,
    FilterConfig,
    FundingConfig,
    FundingPayment,
    FundingQuote,
    MarketState,
    RuntimeConfig,
    StatisticsConfig,
    StatisticsEngine,
    VolumeTicker,
    calculate_funding_statistics,
    parse_funding_history,
    render_funding,
)


class FundingStatisticsTests(unittest.TestCase):
    def test_first_history_load_does_not_wait_for_machine_uptime(self):
        client = mock.Mock()
        client.fetch.return_value = [FundingPayment(1000, 0.0001)]
        with mock.patch("monitor.funding.core.time.monotonic", return_value=10.0):
            worker = FundingHistoryWorker(SimpleNamespace(current=FundingConfig()), client)
            try:
                worker.set_symbols({"BTCUSDT"})
                for _ in range(100):
                    if "BTCUSDT" in worker.snapshot()[0]:
                        break
                    time.sleep(0.01)
                self.assertIn("BTCUSDT", worker.snapshot()[0])
                worker._wake.set()
                time.sleep(0.1)
                client.fetch.assert_called_once_with("BTCUSDT", 30)
            finally:
                worker.close()

    def test_live_rate_visible_without_history_and_retained_during_reconnect(self):
        state = MarketState()
        state.funding_quotes["AAAUSDT"] = FundingQuote(
            0.0002, 123, 100, time.monotonic()
        )
        result = funding_snapshot(state, "AAAUSDT", FundingConfig())
        self.assertEqual(result[0], "+2.00bp")
        self.assertEqual(result[1], "--")
        self.assertIsNone(result[2])
        self.assertIsNone(result[3])
        state.funding_quotes["AAAUSDT"].received_at -= 20
        state.funding_connected = False
        retained = funding_snapshot(state, "AAAUSDT", FundingConfig())
        self.assertEqual(retained[0], "+2.00bp")
        self.assertGreaterEqual(retained[4], 20)
        self.assertEqual(funding_snapshot(state, "MISSING", FundingConfig())[0], "--")

    def test_transport_recovery_notice_hidden_without_erasing_diagnostics(self):
        state = MarketState()
        state.errors.update({
            "futures_book_3": "TransportLagError: 连续10秒没有通过接收时效校验的消息",
            "futures_book_1": "TimeoutError: handshake timed out",
            "funding_stream": "ValueError: invalid response",
        })
        errors = dict(display_errors(state))
        self.assertNotIn("futures_book_3", errors)
        self.assertIn("futures_book_1", errors)
        self.assertIn("funding_stream", errors)
        self.assertIn("futures_book_3", state.errors)

    def test_time_weighted_average_normalizes_mixed_intervals(self):
        hour = 3_600_000
        records = [
            FundingPayment(0, 0.0001),
            FundingPayment(4 * hour, 0.0001),
            FundingPayment(8 * hour, -0.00005),
        ]
        stats = calculate_funding_statistics("AAAUSDT", records, 0.0)
        self.assertIsNotNone(stats)
        assert stats is not None
        self.assertAlmostEqual(stats.average_bps_8h, 1.0)
        self.assertAlmostEqual(stats.positive_average_bps_8h, 2.0)
        self.assertAlmostEqual(stats.negative_average_bps_8h, -1.0)
        self.assertAlmostEqual(stats.positive_time_ratio, 2 / 3)

    def test_current_rate_uses_next_settlement_interval(self):
        hour = 3_600_000
        records = [FundingPayment(0, 0.0001), FundingPayment(4 * hour, 0.0001)]
        stats = calculate_funding_statistics("AAAUSDT", records)
        assert stats is not None
        quote = FundingQuote(0.0001, 8 * hour, 0, time.monotonic())
        self.assertAlmostEqual(stats.current_bps_8h(quote), 2.0)

    def test_percentile_and_censored_direction_streak(self):
        hour = 3_600_000
        records = [
            FundingPayment(0, 0.00005),
            FundingPayment(8 * hour, 0.0001),
            FundingPayment(16 * hour, 0.0002),
        ]
        stats = calculate_funding_statistics("AAAUSDT", records, 0.05)
        assert stats is not None
        self.assertAlmostEqual(stats.percentile(1.0), 2 / 3)
        streak = stats.direction_streak_seconds(1.0, 0.05, 24 * hour)
        self.assertEqual(streak, (1, 24 * 3600.0, True))

    def test_history_parser_ignores_invalid_rows(self):
        rows = parse_funding_history([
            {"fundingTime": 1000, "fundingRate": "0.0001"},
            {"bad": "row"},
        ])
        self.assertEqual(rows, [FundingPayment(1000, 0.0001)])

    def test_funding_page_renders_an_eligible_pair(self):
        now_mono = time.monotonic()
        now_ms = int(time.time() * 1000)
        hour = 3_600_000
        state = MarketState()
        filters = FilterConfig()
        state.set_pairing_config(filters)
        state.spot_volumes["AAAUSDT"] = VolumeTicker(20_000_000)
        state.futures_volumes["AAAUSDT"] = VolumeTicker(30_000_000)
        state.spot_books["AAAUSDT"] = BookQuote(1.0, 1.01, now_mono)
        state.futures_books["AAAUSDT"] = BookQuote(1.01, 1.02, now_mono)
        records = [
            FundingPayment(now_ms - 16 * hour, 0.0001),
            FundingPayment(now_ms - 8 * hour, 0.0001),
            FundingPayment(now_ms, 0.0001),
        ]
        history = calculate_funding_statistics("AAAUSDT", records)
        assert history is not None
        state.funding_statistics["AAAUSDT"] = history
        state.funding_quotes["AAAUSDT"] = FundingQuote(
            0.0001, now_ms + 8 * hour, now_ms, now_mono
        )
        output = render_funding(
            state, StatisticsEngine(), filters, StatisticsConfig(),
            FundingConfig(), RuntimeConfig(), now_mono, [],
        )
        self.assertIn("AAAUSDT", output)
        self.assertIn("+1.00bp", output)
        state.funding_statistics.clear()
        pending = render_funding(
            state, StatisticsEngine(), filters, StatisticsConfig(),
            FundingConfig(), RuntimeConfig(), now_mono, [],
        )
        self.assertIn("AAAUSDT", pending)
        self.assertIn("+1.00bp", pending)


if __name__ == "__main__":
    unittest.main()
