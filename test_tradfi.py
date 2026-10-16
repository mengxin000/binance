import json
import tempfile
import time
import unittest
from pathlib import Path

from monitor.models import MarketState
from monitor.basis.core import StatisticsEngine
from monitor.settings import (ConfigManager, FilterConfig, TradFiConfig,
                              StatisticsConfig, FundingConfig, RuntimeConfig)
from monitor.terminal.pages import render


def mapping(spot="QQQB", future="QQQ"):
    return {spot: future}


class TradFiTests(unittest.TestCase):
    def setUp(self):
        self.filters = FilterConfig(quote_assets=("USDT", "USDC"),
                                   cross_quote_pairings=(("USDT", "USDC"),))
        self.state = MarketState()
        self.state.set_pairing_config(self.filters)
        self.state.tradfi_config = TradFiConfig(mappings=mapping())
        for market, symbols in (("spot", ["QQQBUSDT", "BTCUSDT", "USDCUSDT"]),
                                ("futures", ["QQQUSDT", "QQQUSDC", "BTCUSDT"])):
            self.state.update_tickers(market, [{"s": s, "q": "20000000"} for s in symbols])
            for symbol in symbols:
                price = 1.01 if symbol == "USDCUSDT" else 100
                self.state.update_book(market, {"s": symbol, "b": price, "a": price})
        self.state.ticker_connected = dict(spot=True, futures=True)
        self.state.book_connected = dict(spot=True, futures=True)

    def test_routes_are_separate_and_subscriptions_are_union(self):
        self.assertEqual(set(self.state.pair_routes(self.filters)), {"BTCUSDT"})
        routes = self.state.tradfi_view().pair_routes(self.filters)
        self.assertEqual(set(routes), {"QQQBUSDT__QQQUSDT", "QQQBUSDT__QQQUSDC"})
        self.assertIn("QQQBUSDT", self.state.book_symbols("spot", self.filters))
        self.assertIn("QQQUSDC", self.state.book_symbols("futures", self.filters))
        self.assertIn("USDCUSDT", self.state.book_symbols("spot", self.filters))

    def test_reuses_fx_and_quality_checks(self):
        view = self.state.tradfi_view()
        snapshots = view.snapshots(self.filters, 1, 0.2)
        cross = next(s for s in snapshots if s.futures_symbol == "QQQUSDC")
        self.assertAlmostEqual(cross.basis_bps, 100)
        self.assertEqual(cross.market_type, "tradfi")
        self.assertFalse(self.state._spot_futures_basis_cache)
        self.state.spot_books["QQQBUSDT"].received_at -= 5
        self.assertEqual(view.snapshots(self.filters, 1, 0.2), [])
        self.assertEqual(view.drain_quote_rejections()[0]["market_type"], "tradfi")

    def test_disable_removes_only_unneeded_subscriptions(self):
        self.state.tradfi_config = TradFiConfig(enabled=False, mappings=mapping())
        self.assertNotIn("QQQBUSDT", self.state.book_symbols("spot", self.filters))
        self.assertIn("BTCUSDT", self.state.book_symbols("spot", self.filters))
        # The perpetual/perpetual page still needs both QQQ contracts.
        self.assertIn("QQQUSDC", self.state.book_symbols("futures", self.filters))

    def test_volume_and_price_filters(self):
        self.state.spot_volumes["QQQBUSDT"].quote_volume = 1
        self.assertFalse(self.state.tradfi_view().liquid_routes(self.filters))
        self.state.spot_volumes["QQQBUSDT"].quote_volume = 20_000_000
        self.state.update_book("spot", {"s": "QQQBUSDT", "b": 0.01, "a": 0.01})
        self.assertFalse(self.state.tradfi_view().eligible_symbols(self.filters))

    def test_hot_reload_and_invalid_config_preserves_last_valid(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "tradfi.json"
            path.write_text(json.dumps({"mappings": mapping()}), encoding="utf-8")
            manager = ConfigManager(path, TradFiConfig)
            self.state.tradfi_config = manager.current
            self.assertTrue(self.state.tradfi_routes(self.filters))
            path.write_text(json.dumps({"mappings": {}}), encoding="utf-8")
            self.assertTrue(manager.reload(force=True))
            self.state.tradfi_config = manager.current
            self.assertFalse(self.state.tradfi_routes(self.filters))
            previous = manager.current
            path.write_text('{"mappings": {"qqqb": "QQQ"}}', encoding="utf-8")
            self.assertFalse(manager.reload(force=True))
            self.assertIs(manager.current, previous)

    def test_render_uses_tradfi_routes_and_statistics(self):
        view = self.state.tradfi_view()
        engine = StatisticsEngine()
        config = StatisticsConfig(min_k_sigma_bps=0.01)
        engine.update(view.snapshots(self.filters, 1), 1, config)
        self.state.update_book("futures", {"s": "QQQUSDT", "b": 100.2, "a": 100.2})
        engine.update(view.snapshots(self.filters, 1), 1, config)
        text = render(view, engine, self.filters, config, FundingConfig(),
                      RuntimeConfig(), time.monotonic(), [])
        self.assertIn("TradFi", text)
        self.assertIn("QQQBUSDT/QQQUSDT", text)
        self.assertNotIn("BTCUSDT", text)

    def test_missing_symbol_is_reported_without_subscribing(self):
        self.state.tradfi_config = TradFiConfig(mappings=mapping("MISSING", "QQQ"))
        view = self.state.tradfi_view()
        self.assertFalse(view.pair_routes(self.filters))
        text = render(view, StatisticsEngine(), self.filters, StatisticsConfig(),
                      FundingConfig(), RuntimeConfig(), time.monotonic(), [])
        self.assertIn("映射待就绪: MISSING/QQQ", text)

    def test_unrelated_disconnect_does_not_freeze_any_page(self):
        self.state.book_connected = dict(spot=False, futures=False)
        self.state.ticker_connected = dict(spot=False, futures=False)
        self.assertTrue(self.state.snapshots(self.filters, 1))
        self.assertTrue(self.state.tradfi_view().snapshots(self.filters, 1))
        self.assertTrue(self.state.futures_futures_snapshots(self.filters, 1))
        for quote in self.state.futures_books.values():
            quote.received_at -= 5
        self.assertFalse(self.state.snapshots(self.filters, 1))
        self.assertFalse(self.state.tradfi_view().snapshots(self.filters, 1))
        self.assertFalse(self.state.futures_futures_snapshots(self.filters, 1))

    def test_display_preserves_last_valid_statistics_sample(self):
        view = self.state.tradfi_view()
        engine = StatisticsEngine()
        config = StatisticsConfig(min_k_sigma_bps=0.01)
        engine.update(view.snapshots(self.filters, 1), 1, config)
        self.state.update_book("futures", {"s": "QQQUSDT", "b": 100.2, "a": 100.2})
        engine.update(view.snapshots(self.filters, 1), 1, config)
        self.state.update_book("futures", {"s": "QQQUSDT", "b": 100.7, "a": 100.7})
        def current_row():
            text = render(view, engine, self.filters, config, FundingConfig(),
                          RuntimeConfig(), time.monotonic(), [])
            return next(line for line in text.splitlines() if "QQQBUSDT/QQQUSDT" in line)
        self.assertEqual(current_row().split()[10], "+20.00bp")
        self.state.futures_books["QQQUSDT"].received_at -= 5
        row = current_row()
        self.assertNotIn("+70.00bp", row)
        self.assertEqual(row.split()[10], "+20.00bp")
        self.assertIn("+20.00bp", row)  # Historical range remains visible.


if __name__ == "__main__":
    unittest.main()
