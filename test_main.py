import asyncio
import json
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path

from main import (
    BackgroundWriter,
    BasisDiagnosticStore,
    BINANCE_WEBSOCKET_OPTIONS,
    ConfigManager,
    FilterConfig,
    PairDirectoryStore,
    MarketState,
    PairSnapshot,
    PairStatistics,
    QuantileWorker,
    StatisticsEngine,
    StatisticsConfig,
    RuntimeConfig,
    format_quote_volume,
    table_row,
    terminal_width,
    consume_book_chunk,
    RawMessage,
    RawMessageBuffer,
    decode_raw_books,
    receive_raw_books,
    LatestQuotePipeline,
    NetworkDiagnostics,
    extract_combined_stream_symbol,
    BBOIngressWorker,
)


class PairStatisticsTests(unittest.TestCase):
    def test_route_version_only_changes_when_pairing_config_changes(self):
        state = MarketState()
        usdt = FilterConfig(quote_assets=("USDT",))
        state.set_pairing_config(usdt)
        version = state.routes_version
        state.set_pairing_config(usdt)
        self.assertEqual(state.routes_version, version)
        state.set_pairing_config(FilterConfig(quote_assets=("USDT", "USDC")))
        self.assertEqual(state.routes_version, version + 1)

    def test_online_mean(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(
            total_window_samples=3, long_window_samples=3,
            short_window_samples=2, open_threshold_bps=100,
            close_threshold_bps=-100,
        )
        for index, basis in enumerate((2.0, 4.0, 6.0)):
            stats.update(PairSnapshot("AAAUSDT", 100, 100, basis), 1000 + index, 0.2, config)
        self.assertEqual(stats.sample_count, 3)
        self.assertAlmostEqual(stats.mean_basis_bps, 4.0)
        self.assertAlmostEqual(stats.standard_deviation_bps, (8 / 3) ** 0.5)
        self.assertAlmostEqual(stats.observed_seconds, 0.6)
        stats.update(PairSnapshot("AAAUSDT", 100, 100, 8.0), 1004, 0.2, config)
        self.assertEqual(list(stats.basis_samples), [4.0, 6.0, 8.0])
        self.assertAlmostEqual(stats.mean_basis_bps, 6.0)

    def test_open_and_close_excursions_are_counted_independently(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(
            total_window_samples=100, long_window_samples=50,
            short_window_samples=20, open_threshold_bps=5,
            close_threshold_bps=-5,
        )
        events = []
        for index, basis in enumerate((0, 8, 0, -8, 0)):
            events.extend(
            stats.update(PairSnapshot("AAAUSDT", 100, 100, float(basis)), 1000 + index, 0.2, config)
            )
        self.assertEqual(stats.open_count, 1)
        self.assertEqual(stats.close_count, 1)
        self.assertEqual([event["event"] for event in events], [
            "open_opportunity", "close_opportunity",
        ])

    def test_quantiles_use_truncated_tail_of_total_window(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(
            total_window_samples=5, long_window_samples=4,
            short_window_samples=3, open_threshold_bps=100,
            close_threshold_bps=-100,
        )
        for basis in range(10):
            stats.update(PairSnapshot("AAAUSDT", 100, 100, float(basis)), 1000 + basis, 0.2, config)
        self.assertEqual(list(stats.basis_samples), [5.0, 6.0, 7.0, 8.0, 9.0])
        self.assertEqual(stats.runtime_range, (5.0, 9.0))
        low, high = stats.quantile_range(3, 0.0, 1.0) or (None, None)
        self.assertEqual((low, high), (7.0, 9.0))

    def test_background_quantiles_are_cached_without_render_time_sorting(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(
            total_window_samples=5, long_window_samples=4,
            short_window_samples=3, quantile_low=0.0, quantile_high=1.0,
        )
        for basis in range(5):
            stats.update(PairSnapshot("AAAUSDT", 100, 100, float(basis)), 1000 + basis, 1, config)
        worker = QuantileWorker()
        async def calculate():
            self.assertTrue(worker.schedule([(stats, config)]))
            self.assertFalse(worker.schedule([(stats, config)]))
            await worker.close()
        asyncio.run(calculate())
        self.assertEqual(stats.cached_quantile_range(3, 0.0, 1.0), (2.0, 4.0))
        self.assertEqual(stats.cached_quantile_range(4, 0.0, 1.0), (1.0, 4.0))

    def test_background_writer_flushes_before_close(self):
        output = []
        writer = BackgroundWriter(100)
        self.assertTrue(writer.submit(output.append, "saved"))
        writer.close()
        self.assertEqual(output, ["saved"])

    def test_full_disk_queue_never_waits_for_space(self):
        started = threading.Event()
        release = threading.Event()
        writer = BackgroundWriter(1)

        def slow_write():
            started.set()
            release.wait(5)

        try:
            writer.submit(slow_write)
            self.assertTrue(started.wait(2))
            writer.submit(lambda: None)
            with mock.patch.object(writer._queue, "put", wraps=writer._queue.put) as put:
                self.assertFalse(writer.submit(lambda: None))
                self.assertEqual(put.call_args.kwargs, {"block": False})
            self.assertEqual(writer.dropped_records, 1)
        finally:
            release.set()
            writer.close()

    def test_bbo_record_rate_limit_is_per_pair(self):
        stats = PairStatistics("AAAUSDT")
        self.assertTrue(stats.allow_bbo_record(1000.0, 1.0))
        self.assertFalse(stats.allow_bbo_record(1000.999, 1.0))
        self.assertTrue(stats.allow_bbo_record(1001.0, 1.0))

    def test_ranking_uses_smaller_open_close_count_first(self):
        config = StatisticsConfig(
            total_window_samples=10, long_window_samples=8,
            short_window_samples=3, min_k_sigma_bps=0.1,
        )
        engine = StatisticsEngine()
        for symbol in ("AAAUSDT", "BBBUSDT"):
            stats = PairStatistics(symbol)
            stats.update(PairSnapshot(symbol, 100, 100, 0), 1000, 0.2, config)
            stats.update(PairSnapshot(symbol, 100, 100, 2), 1001, 0.2, config)
            engine.pairs[symbol] = stats
        engine.pairs["AAAUSDT"].open_count = 100
        engine.pairs["AAAUSDT"].close_count = 2
        engine.pairs["BBBUSDT"].open_count = 20
        engine.pairs["BBBUSDT"].close_count = 18
        rows = engine.selected_rows(False, config)
        self.assertEqual([row.symbol for row in rows], ["BBBUSDT", "AAAUSDT"])


class PairDirectoryStoreTests(unittest.TestCase):
    def test_save_state_and_append_only_opportunity(self):
        with tempfile.TemporaryDirectory() as directory:
            store = PairDirectoryStore(Path(directory))
            stats = PairStatistics("AAAUSDT", open_count=2, close_count=1)
            config = StatisticsConfig(
                total_window_samples=5, long_window_samples=4,
                short_window_samples=3,
            )
            stats.update(PairSnapshot("AAAUSDT", 100, 101, 3.5), 1000, 0.2, config)
            store.save_states([stats])
            stats.open_count = 3
            store.save_states([stats])
            event = {"timestamp": 1001, "basis_bps": 8, "deviation_bps": 5}
            store.append_opportunity("AAAUSDT", event)
            self.assertEqual(store.load(), {})
            state = json.loads((Path(directory) / "AAAUSDT" / "state.json").read_text())
            self.assertNotIn("record_type", state)
            self.assertEqual(state["open_count"], 3)
            self.assertEqual(state["close_count"], 1)
            lines = (Path(directory) / "AAAUSDT" / "opportunities.jsonl").read_text().splitlines()
            self.assertEqual(len(lines), 1)
            self.assertEqual(json.loads(lines[0]), event)

    def test_rejected_quote_is_rechecked_by_next_aligned_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            store = BasisDiagnosticStore(Path(directory))
            now = 1000.0
            rejection = {
                "market_type": "spot_futures",
                "symbol": "AAAUSDT",
                "rejected_at_wall": now - 0.5,
                "rejected_at_mono": time.monotonic() - 0.5,
                "rejected_basis_bps": 12.0,
                "reason": "quote_skew",
                "raw_quote_skew_ms": 450.0,
                "raw_leg_ages_ms": [20.0, 470.0],
                "exchange_event_times_ms": [None, 123],
            }
            snapshot = PairSnapshot(
                "AAAUSDT", 100, 100, 1.0,
                market_type="spot_futures", quote_skew_ms=50.0,
                leg_ages_ms=(40.0, 80.0),
            )
            store.process([rejection], [snapshot], now)
            rows = (Path(directory) / "basis_recheck.jsonl").read_text().splitlines()
            record = json.loads(rows[0])
            self.assertEqual(record["reversion_bps"], 11.0)
            self.assertEqual(record["rejection_reason"], "quote_skew")


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

    def test_usdt_and_usdc_perpetuals_are_paired_by_base_asset(self):
        filters = FilterConfig(
            min_futures_volume=0,
            quote_assets=("USDT", "USDC"),
        )
        state = MarketState()
        state.set_pairing_config(filters)
        state.update_tickers("spot", [
            {"s": "USDCUSDT", "q": "20000000"},
        ])
        state.update_tickers("futures", [
            {"s": "BTCUSDT", "q": "30000000"},
            {"s": "BTCUSDC", "q": "25000000"},
            {"s": "ETHUSDT", "q": "30000000"},
        ])
        self.assertEqual(
            set(state.futures_futures_routes(filters)),
            {"BTCUSDT__BTCUSDC"},
        )
        state.book_symbols("spot", filters)
        state.book_symbols("futures", filters)
        for market, symbol, price in (
            ("spot", "USDCUSDT", 0.999),
            ("futures", "BTCUSDT", 100_000),
            ("futures", "BTCUSDC", 100_100),
        ):
            state.update_book(market, {"s": symbol, "b": str(price), "a": str(price)})
        state.ticker_connected = {"spot": True, "futures": True}
        state.book_connected = {"spot": True, "futures": True}
        rows = state.futures_futures_snapshots(filters, 10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].symbol, "BTCUSDT__BTCUSDC")
        self.assertAlmostEqual(rows[0].spot_mid, 100_000)
        self.assertAlmostEqual(rows[0].futures_mid, 100_100 * 0.999)

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

    def test_quote_asset_hot_reload_removes_disallowed_cached_market_data(self):
        state = MarketState()
        state.set_pairing_config(FilterConfig(quote_assets=("USDT",)))
        state.update_tickers("spot", [{"s": "BTCUSDT", "q": "20000000"}])
        state.update_tickers("futures", [{"s": "BTCUSDT", "q": "30000000"}])
        state.update_book("spot", {"s": "BTCUSDT", "b": "100", "a": "101"})
        state.update_book("futures", {"s": "BTCUSDT", "b": "100", "a": "101"})

        state.set_pairing_config(FilterConfig(quote_assets=("USDC",)))

        self.assertNotIn("BTCUSDT", state.spot_volumes)
        self.assertNotIn("BTCUSDT", state.futures_volumes)
        self.assertNotIn("BTCUSDT", state.spot_books)
        self.assertNotIn("BTCUSDT", state.futures_books)
        self.assertEqual(state.common_symbols(FilterConfig(quote_assets=("USDC",))), set())

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
        self.assertEqual(state.snapshots(FilterConfig(), max_age_seconds=10), [])

    def test_mismatched_latest_quotes_are_skipped_until_both_legs_refresh(self):
        state = MarketState()
        state.update_tickers("spot", [{"s": "AAAUSDT", "q": "20000000"}])
        state.update_tickers("futures", [{"s": "AAAUSDT", "q": "20000000"}])
        state.update_book("spot", {"s": "AAAUSDT", "b": "100", "a": "100"})
        state.update_book("futures", {"s": "AAAUSDT", "b": "100", "a": "100"})
        state.spot_books["AAAUSDT"].received_at -= 0.5
        state.futures_books["AAAUSDT"].received_at -= 0.5
        state.update_book("spot", {"s": "AAAUSDT", "b": "101", "a": "101"})
        state.ticker_connected = {"spot": True, "futures": True}
        state.book_connected = {"spot": True, "futures": True}

        rows = state.snapshots(
            FilterConfig(), max_age_seconds=1, tolerance_seconds=0.2
        )
        self.assertEqual(rows, [])
        rejections = state.drain_quote_rejections()
        self.assertEqual(len(rejections), 1)
        self.assertEqual(rejections[0]["reason"], "quote_skew")

        state.update_book("futures", {"s": "AAAUSDT", "b": "101", "a": "101", "E": 123456})
        rows = state.snapshots(
            FilterConfig(), max_age_seconds=1, tolerance_seconds=0.2
        )
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0].basis_bps, 0.0)
        self.assertEqual(rows[0].exchange_event_times_ms, (None, 123456))

    def test_real_regime_change_is_not_rejected_by_statistical_filter(self):
        stats = PairStatistics("AAAUSDT")
        config = StatisticsConfig(
            total_window_samples=100, long_window_samples=50,
            short_window_samples=20, open_threshold_bps=100,
            close_threshold_bps=-100,
        )
        for index in range(10):
            stats.update(PairSnapshot("AAAUSDT", 100, 100, 0.1 * index), 1000 + index, 1, config)
        stats.update(PairSnapshot("AAAUSDT", 100, 100, 20.0), 1011, 1, config)
        self.assertEqual(stats.sample_count, 11)
        self.assertEqual(stats.current_basis_bps, 20.0)

    def test_transport_backlog_is_rejected_even_when_local_receive_time_is_fresh(self):
        state = MarketState()
        state.update_tickers("spot", [{"s": "AAAUSDT", "q": "20000000"}])
        state.update_tickers("futures", [{"s": "AAAUSDT", "q": "20000000"}])
        state.update_book(
            "spot", {"s": "AAAUSDT", "b": "100", "a": "100"},
            transport_lag_ms=100,
        )
        state.update_book(
            "futures", {"s": "AAAUSDT", "b": "101", "a": "101", "E": 1},
            transport_lag_ms=5000,
        )
        state.ticker_connected = {"spot": True, "futures": True}
        state.book_connected = {"spot": True, "futures": True}

        self.assertEqual(state.snapshots(FilterConfig(), 1, 0.2), [])
        rejection = state.drain_quote_rejections()[0]
        self.assertEqual(rejection["reason"], "transport_lag")
        self.assertEqual(rejection["transport_lags_ms"], [100, 5000])


class ConfigManagerTests(unittest.TestCase):
    def test_binance_uses_server_heartbeat_without_client_ping_timeout(self):
        self.assertIsNone(BINANCE_WEBSOCKET_OPTIONS["ping_interval"])
        self.assertIsNone(BINANCE_WEBSOCKET_OPTIONS["ping_timeout"])

    def test_invalid_hot_reload_keeps_last_valid_config(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "filter.json"
            path.write_text(json.dumps({"min_spot_volume": 7}), encoding="utf-8")
            manager = ConfigManager(path, FilterConfig)
            self.assertEqual(manager.current.min_spot_volume, 7)
            path.write_text("{保存到一半", encoding="utf-8")
            self.assertFalse(manager.reload())
            self.assertEqual(manager.current.min_spot_volume, 7)


class BookConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_book_chunk_reconnects_after_connection_failure(self):
        state = MarketState()
        stop = asyncio.Event()
        attempts = 0

        class FakeSocket:
            def __init__(self):
                self.messages = 0

            async def recv(self):
                self.messages += 1
                if self.messages == 1:
                    return json.dumps({
                        "stream": "btcusdt@ticker",
                        "data": {"s": "BTCUSDT", "E": int(time.time() * 1000)},
                    })
                if self.messages > 2:
                    while "BTCUSDT" not in state.spot_books:
                        await asyncio.sleep(0)
                    stop.set()
                    await asyncio.Future()
                return json.dumps({
                    "stream": "btcusdt@bookTicker",
                    "data": {"s": "BTCUSDT", "b": "100", "a": "101"},
                })

        class FakeConnection:
            async def __aenter__(self):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise OSError("temporary disconnect")
                return FakeSocket()

            async def __aexit__(self, *_args):
                return False

        with mock.patch("main.websockets.connect", side_effect=lambda *_a, **_k: FakeConnection()):
            await consume_book_chunk("wss://example", "spot", "1", state, stop)

        self.assertEqual(attempts, 2)
        self.assertIn("BTCUSDT", state.spot_books)

    async def test_transport_lag_closes_backlogged_socket_and_reconnects(self):
        state = MarketState()
        state.max_transport_lag_ms = 2000
        stop = asyncio.Event()
        attempts = 0

        class FakeSocket:
            def __init__(self, stale: bool):
                self.stale = stale
                self.messages = 0

            async def recv(self):
                self.messages += 1
                event_time = int(time.time() * 1000) - (60_000 if self.stale else 0)
                if self.messages == 1:
                    return json.dumps({
                        "stream": "btcusdt@bookTicker",
                        "data": {"s": "BTCUSDT", "E": event_time, "b": "100", "a": "101"},
                    })
                while "BTCUSDT" not in state.futures_books:
                    await asyncio.sleep(0)
                stop.set()
                await asyncio.Future()

        class FakeConnection:
            def __init__(self, stale: bool):
                self.socket = FakeSocket(stale)

            async def __aenter__(self):
                return self.socket

            async def __aexit__(self, *_args):
                return False

        def connect(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            return FakeConnection(stale=attempts == 1)

        with mock.patch("main.websockets.connect", side_effect=connect):
            await asyncio.wait_for(consume_book_chunk("wss://example", "futures", "1", state, stop), 5)

        self.assertEqual(attempts, 2)
        self.assertIn("BTCUSDT", state.futures_books)


class RawPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_spot_and_futures_bbo_use_distinct_network_threads(self):
        class Manager:
            current = RuntimeConfig()

        diagnostics = NetworkDiagnostics(None, 1_000_000)
        pipeline = LatestQuotePipeline(Manager(), diagnostics)
        spot = BBOIngressWorker("spot", Manager(), pipeline, diagnostics)
        futures = BBOIngressWorker("futures", Manager(), pipeline, diagnostics)
        try:
            spot.start()
            futures.start()
            async with asyncio.timeout(2):
                while spot.thread_id is None or futures.thread_id is None:
                    await asyncio.sleep(0.01)
            self.assertNotEqual(spot.thread_id, futures.thread_id)
        finally:
            await asyncio.gather(
                asyncio.to_thread(spot.close), asyncio.to_thread(futures.close)
            )
            pipeline.close()
            diagnostics.close()

    async def test_latest_symbol_slot_coalesces_before_json_parse(self):
        class Manager:
            current = RuntimeConfig(raw_coalesce_interval_ms=100)

        diagnostics = NetworkDiagnostics(None, 1_000_000)
        pipeline = LatestQuotePipeline(Manager(), diagnostics)
        token = object()
        try:
            for index in range(100):
                payload = json.dumps({
                    "stream": "btcusdt@bookTicker",
                    "data": {"s": "BTCUSDT", "b": str(100 + index),
                             "a": str(101 + index), "E": int(time.time() * 1000)},
                }).encode()
                pipeline.put(
                    "futures", "futures_book_1", token,
                    frozenset({"BTCUSDT"}), payload,
                    time.monotonic(), time.time() * 1000,
                )
            await asyncio.sleep(0.25)
            state = MarketState()
            pipeline.apply_latest(state)
            metrics = pipeline.metrics()
            self.assertEqual(metrics["received"], 100)
            self.assertEqual(metrics["overwritten"], 99)
            self.assertEqual(metrics["parsed"], 1)
            self.assertEqual(state.futures_books["BTCUSDT"].bid, 199)
            self.assertGreater(state.futures_books["BTCUSDT"].version, 0)
        finally:
            pipeline.close()
            diagnostics.close()

    async def test_combined_stream_identity_is_allowlisted(self):
        payload = b'{"stream":"suiusdt@bookTicker","data":{"s":"SUIUSDT"}}'
        self.assertEqual(
            extract_combined_stream_symbol(payload, frozenset({"SUIUSDT"})),
            "SUIUSDT",
        )
        self.assertIsNone(extract_combined_stream_symbol(
            payload, frozenset({"BTCUSDT", "ETHUSDT"})
        ))

    async def test_receiver_drops_old_raw_frames_without_json_parsing(self):
        buffer = RawMessageBuffer(8)
        stop = asyncio.Event()
        delivered = asyncio.Event()

        class Socket:
            count = 0

            async def recv(self):
                if self.count == 1000:
                    delivered.set()
                    await asyncio.Future()
                self.count += 1
                return f"not-json-{self.count}"

        with mock.patch("monitor.network.core.json.loads", side_effect=AssertionError("receiver parsed JSON")):
            task = asyncio.create_task(receive_raw_books(Socket(), buffer, stop))
            try:
                await asyncio.wait_for(delivered.wait(), 5)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(len(buffer.messages), 8)
        self.assertEqual(buffer.dropped, 992)
        self.assertEqual(buffer.messages[0].payload, "not-json-993")
        self.assertEqual(buffer.messages[-1].payload, "not-json-1000")

    async def test_decoder_skips_expired_json_and_preserves_receive_time(self):
        buffer = RawMessageBuffer(8)
        state = MarketState()
        stop = asyncio.Event()
        now = time.monotonic()
        buffer.append(RawMessage(now - 10, time.time() * 1000, "invalid old JSON"))
        buffer.append(RawMessage(now - 0.1, time.time() * 1000, json.dumps({
            "data": {"s": "SUIUSDT", "b": "1", "a": "2"},
        })))
        task = asyncio.create_task(decode_raw_books("spot", state, buffer, stop))
        try:
            async def wait_decoded():
                while buffer.decoded < 1:
                    if task.done():
                        task.result()
                    await asyncio.sleep(0)
            await asyncio.wait_for(wait_decoded(), 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(buffer.expired, 1)
        self.assertEqual(state.spot_books["SUIUSDT"].received_at, now - 0.1)
        self.assertIsNone(state.spot_books["SUIUSDT"].transport_lag_ms)

    async def test_decoder_yields_between_batches_and_demultiplexes(self):
        buffer = RawMessageBuffer(1000)
        state = MarketState()
        stop = asyncio.Event()
        now = time.monotonic()
        for index in range(300):
            buffer.append(RawMessage(now, time.time() * 1000, json.dumps({
                "data": {"s": f"COIN{index % 3}USDT", "b": str(index + 1), "a": str(index + 1)},
            })))
        task = asyncio.create_task(decode_raw_books("spot", state, buffer, stop))
        turns = 0
        try:
            async def wait_decoded():
                nonlocal turns
                while buffer.messages:
                    turns += 1
                    await asyncio.sleep(0)
            await asyncio.wait_for(wait_decoded(), 2)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.assertGreater(turns, 1)
        self.assertEqual(len(state.spot_books), 3)
        self.assertEqual(state.spot_books["COIN2USDT"].mid, 300)


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
