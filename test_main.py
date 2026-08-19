import json
import tempfile
import unittest
from pathlib import Path

from main import (
    ConfigManager,
    FilterConfig,
    PairDirectoryStore,
    MarketState,
    PairSnapshot,
    PairStatistics,
    StatisticsConfig,
    format_quote_volume,
    table_row,
    terminal_width,
)


class PairStatisticsTests(unittest.TestCase):
    def test_bbo_queue_drops_oldest_snapshot_when_full(self):
        state = MarketState(bbo_queue_maxsize=2)
        for market in ("spot", "futures"):
            state.update_book(market, {"s": "BTCUSDT", "b": "100", "a": "102"})
        state.update_book("spot", {"s": "BTCUSDT", "b": "101", "a": "103"})
        state.update_book("futures", {"s": "BTCUSDT", "b": "102", "a": "104"})

        self.assertEqual(state.bbo_updates.qsize(), 2)
        self.assertEqual(state.bbo_dropped, 1)
        snapshots = [state.bbo_updates.get_nowait(), state.bbo_updates.get_nowait()]
        self.assertEqual(snapshots[-1].spot_mid, 102.0)

    def test_online_mean(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(min_samples=2)
        for index, basis in enumerate((2.0, 4.0, 6.0)):
            stats.update(PairSnapshot("AAAUSDT", 100, 100, basis), 1000 + index, 0.2, config)
        self.assertEqual(stats.sample_count, 3)
        self.assertAlmostEqual(stats.mean_basis_bps, 4.0)
        self.assertAlmostEqual(stats.standard_deviation_bps, (8 / 3) ** 0.5)
        self.assertAlmostEqual(stats.observed_seconds, 0.6)

    def test_opportunity_counts_once_until_rearmed(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(
            min_samples=2, min_four_sigma_bps=0.1,
            expansion_threshold_bps=0.1, quantile_sample_size=20,
        )
        for index, basis in enumerate((0, 2, 4, 5, 1, 10)):
            stats.update(PairSnapshot("AAAUSDT", 100, 100, float(basis)), 1000 + index, 0.2, config)
        self.assertEqual(stats.opportunity_count, 2)
        self.assertFalse(stats.armed)

    def test_bbo_quantiles_keep_only_configured_recent_sample_count(self):
        stats = PairStatistics("AAAUSDT")
        for basis in range(10):
            stats.update_bbo_range(float(basis), sample_size=5)
        self.assertEqual(list(stats.quantile_samples), [5.0, 6.0, 7.0, 8.0, 9.0])
        self.assertEqual(stats.runtime_range, (0.0, 9.0))
        low, high = stats.quantile_range() or (None, None)
        self.assertAlmostEqual(low, 5.2)
        self.assertAlmostEqual(high, 8.8)

    def test_expansion_threshold_is_independent_from_four_sigma_screen(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(
            min_samples=2, min_four_sigma_bps=0.1,
            expansion_threshold_bps=100, quantile_sample_size=20,
        )
        for index, basis in enumerate((0, 2, 20)):
            stats.update(PairSnapshot("AAAUSDT", 100, 100, float(basis)), 1000 + index, 0.2, config)
        self.assertGreater(stats.four_sigma_bps, config.min_four_sigma_bps)
        self.assertEqual(stats.opportunity_count, 0)

    def test_bbo_record_rate_limit_is_per_pair(self):
        stats = PairStatistics("AAAUSDT")
        self.assertTrue(stats.allow_bbo_record(1000.0, 1.0))
        self.assertFalse(stats.allow_bbo_record(1000.999, 1.0))
        self.assertTrue(stats.allow_bbo_record(1001.0, 1.0))


class PairDirectoryStoreTests(unittest.TestCase):
    def test_save_state_and_append_only_opportunity(self):
        with tempfile.TemporaryDirectory() as directory:
            store = PairDirectoryStore(Path(directory))
            stats = PairStatistics(
                "AAAUSDT", sample_count=10, observed_seconds=2,
                mean_basis_bps=3.5, basis_m2=90, current_basis_bps=4,
                current_deviation_bps=0.5, opportunity_count=2,
                last_opportunity_at=999, armed=False,
            )
            store.save_states([stats])
            stats.opportunity_count = 3
            store.save_states([stats])
            event = {"timestamp": 1001, "basis_bps": 8, "deviation_bps": 5}
            store.append_opportunity("AAAUSDT", event)
            restored = store.load()["AAAUSDT"]
            self.assertEqual(restored.opportunity_count, 3)
            self.assertAlmostEqual(restored.mean_basis_bps, 3.5)
            self.assertFalse(restored.armed)
            state = json.loads((Path(directory) / "AAAUSDT" / "state.json").read_text())
            self.assertNotIn("record_type", state)
            lines = (Path(directory) / "AAAUSDT" / "opportunities.jsonl").read_text().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0]), event)


class MarketStateTests(unittest.TestCase):
    def test_cross_quote_pairings_are_normalized_with_usdcusdt_mid(self):
        filters = FilterConfig(
            min_spot_volume=0,
            min_futures_volume=0,
            quote_assets=("USDT", "USDC"),
            cross_quote_pairings=(("USDT", "USDC"), ("USDC", "USDT")),
        )
        state = MarketState()
        state.set_pairing_config(filters)
        state.update_tickers("spot", [
            {"s": "BTCUSDT", "q": "20000000"},
            {"s": "BTCUSDC", "q": "20000000"},
            {"s": "USDCUSDT", "q": "20000000"},
        ])
        state.update_tickers("futures", [
            {"s": "BTCUSDT", "q": "20000000"},
            {"s": "BTCUSDC", "q": "20000000"},
        ])
        state.book_symbols("spot", filters)
        state.book_symbols("futures", filters)
        books = [
            ("spot", "BTCUSDT", 100_000),
            ("spot", "BTCUSDC", 100_100),
            ("spot", "USDCUSDT", 0.999),
            ("futures", "BTCUSDT", 100_000),
            ("futures", "BTCUSDC", 100_100),
        ]
        for market, symbol, price in books:
            state.update_book(market, {"s": symbol, "b": str(price), "a": str(price)})
        state.ticker_connected = {"spot": True, "futures": True}
        state.book_connected = {"spot": True, "futures": True}

        rows = {row.symbol: row for row in state.snapshots(filters, 10)}
        usdt_spot = rows["BTCUSDT__BTCUSDC"]
        usdc_spot = rows["BTCUSDC__BTCUSDT"]
        self.assertAlmostEqual(usdt_spot.futures_mid, 100_100 * 0.999)
        self.assertAlmostEqual(usdc_spot.futures_mid, 100_000 / 0.999)
        self.assertEqual(usdt_spot.spot_symbol, "BTCUSDT")
        self.assertEqual(usdt_spot.futures_symbol, "BTCUSDC")

    def test_quote_assets_only_match_same_quote_currency(self):
        state = MarketState()
        state.set_quote_assets(["USDT", "USDC"])
        state.update_tickers("spot", [
            {"s": "BTCUSDT", "q": "20000000"},
            {"s": "BTCUSDC", "q": "20000000"},
        ])
        state.update_tickers("futures", [
            {"s": "BTCUSDT", "q": "20000000"},
            {"s": "ETHUSDC", "q": "20000000"},
        ])
        self.assertEqual(state.common_symbols(), {"BTCUSDT"})

    def test_mid_basis_and_volume_filter(self):
        state = MarketState()
        state.update_tickers("spot", [{"s": "AAAUSDT", "q": "20000000"}])
        state.update_tickers("futures", [{"s": "AAAUSDT", "q": "30000000"}])
        state.update_book("spot", {"s": "AAAUSDT", "b": "99", "a": "101"})
        state.update_book("futures", {"s": "AAAUSDT", "b": "100", "a": "102"})
        state.ticker_connected = {"spot": True, "futures": True}
        state.book_connected = {"spot": True, "futures": True}
        rows = state.snapshots(FilterConfig(), 10)
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0].spot_mid, 100)
        self.assertAlmostEqual(rows[0].futures_mid, 101)
        self.assertAlmostEqual(rows[0].basis_bps, 100)

    def test_stale_pair_quote_is_not_sampled_when_other_stream_data_is_fresh(self):
        state = MarketState()
        state.update_tickers("spot", [{"s": "AAAUSDT", "q": "20000000"}])
        state.update_tickers("futures", [{"s": "AAAUSDT", "q": "20000000"}])
        state.update_book("spot", {"s": "AAAUSDT", "b": "99", "a": "101"})
        state.update_book("futures", {"s": "AAAUSDT", "b": "100", "a": "102"})
        state.ticker_connected = {"spot": True, "futures": True}
        state.book_connected = {"spot": True, "futures": True}
        state.spot_books["AAAUSDT"].received_at -= 20
        self.assertEqual(state.snapshots(FilterConfig(), stale_seconds=10), [])


class ConfigManagerTests(unittest.TestCase):
    def test_invalid_hot_reload_keeps_last_valid_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "filter.json"
            path.write_text(json.dumps({"top": 7}), encoding="utf-8")
            manager = ConfigManager(path, FilterConfig)
            self.assertEqual(manager.current.top, 7)
            path.write_text("{保存到一半", encoding="utf-8")
            self.assertFalse(manager.reload())
            self.assertEqual(manager.current.top, 7)


class TerminalTableTests(unittest.TestCase):
    def test_quote_volume_uses_compact_units(self):
        self.assertEqual(format_quote_volume(1_250_000_000), "1.25B")
        self.assertEqual(format_quote_volume(23_400_000), "23.40M")
        self.assertEqual(format_quote_volume(15_600), "15.60K")

    def test_chinese_and_ascii_cells_have_equal_rendered_width(self):
        widths = [12, 21]
        aligns = ["left", "right"]
        header = table_row(["交易对", "μ±2σ范围"], widths, aligns)
        data = table_row(["BTCUSDT", "-1.20～+2.30bp"], widths, aligns)
        self.assertEqual(terminal_width(header), terminal_width(data))


if __name__ == "__main__":
    unittest.main()
