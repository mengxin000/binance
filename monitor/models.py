from __future__ import annotations

import asyncio
import ssl
import time
from dataclasses import dataclass
from typing import Any

from .settings import BINANCE_WEBSOCKET_OPTIONS, FilterConfig

# 币安接口数据类
@dataclass(slots=True)
class VolumeTicker:
    quote_volume: float


@dataclass(slots=True)
class BookQuote:
    bid: float
    ask: float
    received_at: float
    exchange_event_time_ms: int | None = None
    transport_lag_ms: float | None = None
    version: int = 0

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


@dataclass(slots=True)
class FundingQuote:
    rate: float
    next_funding_time_ms: int
    event_time_ms: int
    received_at: float


class TransportLagError(RuntimeError):
    """The socket is delivering exchange events too old to be useful."""


@dataclass(slots=True)
class PairSnapshot:
    symbol: str
    spot_mid: float
    futures_mid: float
    basis_bps: float
    spot_symbol: str = ""
    futures_symbol: str = ""
    conversion_rate: float = 1.0
    market_type: str = "spot_futures"
    quote_skew_ms: float = 0.0
    leg_ages_ms: tuple[float, ...] = ()
    exchange_event_times_ms: tuple[int | None, ...] = ()


@dataclass(slots=True)
class BboSnapshot:
    symbol: str
    spot_symbol: str
    futures_symbol: str
    source_market: str
    timestamp: float
    spot_bid: float
    spot_ask: float
    futures_bid: float
    futures_ask: float
    spot_received_at: float
    futures_received_at: float
    conversion_rate: float = 1.0
    conversion_symbol: str = ""
    conversion_received_at: float = 0.0

    @property
    def spot_mid(self) -> float:
        return (self.spot_bid + self.spot_ask) / 2

    @property
    def futures_mid(self) -> float:
        return (self.futures_bid + self.futures_ask) / 2 * self.conversion_rate

    @property
    def raw_futures_mid(self) -> float:
        return (self.futures_bid + self.futures_ask) / 2

    @property
    def basis_bps(self) -> float:
        return (self.futures_mid / self.spot_mid - 1) * 10_000


@dataclass(frozen=True, slots=True)
class PairRoute:
    key: str
    base_asset: str
    spot_symbol: str
    futures_symbol: str
    spot_quote: str
    futures_quote: str
    conversion_symbol: str = ""
    conversion_inverted: bool = False

    @property
    def label(self) -> str:
        if self.spot_symbol == self.futures_symbol:
            return self.spot_symbol
        return f"{self.spot_symbol}/{self.futures_symbol}"


@dataclass(frozen=True, slots=True)
class FuturesFuturesRoute:
    key: str
    base_asset: str
    usdt_symbol: str
    usdc_symbol: str
    conversion_symbol: str
    conversion_inverted: bool = False

    @property
    def label(self) -> str:
        return f"{self.usdt_symbol}/{self.usdc_symbol}"


# 显示状态栏
class MarketState:
    def __init__(self) -> None:
        self.quote_assets: frozenset[str] = frozenset({"USDT"})
        self.cross_quote_pairings: tuple[tuple[str, str], ...] = ()
        self.spot_volumes: dict[str, VolumeTicker] = {}
        self.futures_volumes: dict[str, VolumeTicker] = {}
        self.spot_books: dict[str, BookQuote] = {}
        self.futures_books: dict[str, BookQuote] = {}
        self.funding_quotes: dict[str, FundingQuote] = {}
        self.funding_statistics: dict[str, Any] = {}
        self.funding_history_errors: dict[str, str] = {}
        self.quote_rejections: list[dict[str, Any]] = []
        self.max_transport_lag_ms: float = 2000.0
        self.raw_buffers: dict[str, RawMessageBuffer] = {}
        self.loop_lag_ms = 0.0
        self.max_loop_lag_ms = 0.0
        self._tls_task: asyncio.Task | None = None
        self._quote_sequence = 0
        self.pipeline_metrics: dict[str, Any] = {}
        self._spot_futures_basis_cache: dict[str, tuple[Any, ...]] = {}
        self._futures_futures_basis_cache: dict[str, tuple[Any, ...]] = {}
        self.active_routes: dict[str, PairRoute] = {}
        # 移除 BBO 队列，改为在主循环定时采样时统一处理
        self.routes_version: int = 0
        self.ticker_connected = {"spot": False, "futures": False}
        self.book_connected = {"spot": False, "futures": False}
        self.book_connected_chunks: dict[str, set[str]] = {
            "spot": set(), "futures": set(),
        }
        self.ticker_last_update = {"spot": 0.0, "futures": 0.0}
        self.book_last_update = {"spot": 0.0, "futures": 0.0}
        self.funding_connected = False
        self.funding_last_update = 0.0
        self.errors = {
            "spot_ticker": "", "futures_ticker": "", "spot_book": "",
            "futures_book": "",
        }

    async def connection_options(self, url: str) -> dict[str, Any]:
        options = dict(BINANCE_WEBSOCKET_OPTIONS)
        if url.startswith("wss://"):
            # asyncio otherwise loads the Windows certificate store synchronously
            # inside each connection attempt, pausing every socket on this loop.
            if self._tls_task is None:
                self._tls_task = asyncio.create_task(asyncio.to_thread(ssl.create_default_context))
            task = self._tls_task
            try:
                options["ssl"] = await asyncio.shield(task)
            except Exception:
                if self._tls_task is task:
                    self._tls_task = None
                raise
        return options

    def update_tickers(self, market: str, rows: list[dict[str, Any]]) -> None:
        destination = self.spot_volumes if market == "spot" else self.futures_volumes
        for row in rows:
            symbol = str(row.get("s", ""))
            if not self.is_allowed_symbol(symbol) or "_" in symbol:
                continue
            try:
                quote_volume = float(row["q"])
            except (KeyError, TypeError, ValueError):
                continue
            destination[symbol] = VolumeTicker(quote_volume)
        self.ticker_last_update[market] = time.monotonic()

    def update_book(
        self, market: str, row: dict[str, Any], received_at: float | None = None,
        transport_lag_ms: float | None = None,
    ) -> None:
        try:
            symbol = str(row["s"])
            bid, ask = float(row["b"]), float(row["a"])
        except (KeyError, TypeError, ValueError):
            return
        if bid <= 0 or ask <= 0 or ask < bid:
            return
        now = time.monotonic() if received_at is None else received_at
        destination = self.spot_books if market == "spot" else self.futures_books
        event_time = row.get("E")
        exchange_event_time_ms = (
            int(event_time)
            if isinstance(event_time, (int, float)) and not isinstance(event_time, bool)
            else None
        )
        self._quote_sequence += 1
        quote = BookQuote(
            bid, ask, now, exchange_event_time_ms, transport_lag_ms,
            self._quote_sequence,
        )
        destination[symbol] = quote
        self.book_last_update[market] = now

    def _split_symbol(self, symbol: str) -> tuple[str, str] | None:
        for quote in sorted(self.quote_assets, key=len, reverse=True):
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return symbol[:-len(quote)], quote
        return None

    def _conversion_for(
        self, futures_quote: str, spot_quote: str
    ) -> tuple[str, bool] | None:
        if futures_quote == spot_quote:
            return "", False
        direct = futures_quote + spot_quote
        if direct in self.spot_volumes:
            return direct, False
        inverse = spot_quote + futures_quote
        if inverse in self.spot_volumes:
            return inverse, True
        return None

    def pair_routes(self, filters: FilterConfig) -> dict[str, PairRoute]:
        spot_by_asset: dict[tuple[str, str], str] = {}
        futures_by_asset: dict[tuple[str, str], str] = {}
        # 使用 list() 避免遍历期间 dict 被并发修改导致 RuntimeError
        for symbol in list(self.spot_volumes):
            parsed = self._split_symbol(symbol)
            if parsed:
                spot_by_asset[parsed] = symbol
        for symbol in list(self.futures_volumes):
            parsed = self._split_symbol(symbol)
            if parsed:
                futures_by_asset[parsed] = symbol

        pairings = [(asset, asset) for asset in filters.quote_assets]
        pairings.extend(tuple(pairing) for pairing in filters.cross_quote_pairings)
        routes: dict[str, PairRoute] = {}
        for spot_quote, futures_quote in pairings:
            conversion = self._conversion_for(futures_quote, spot_quote)
            if conversion is None:
                continue
            conversion_symbol, inverted = conversion
            spot_bases = {
                base for base, quote in spot_by_asset if quote == spot_quote
            }
            futures_bases = {
                base for base, quote in futures_by_asset if quote == futures_quote
            }
            for base in spot_bases & futures_bases:
                spot_symbol = spot_by_asset[(base, spot_quote)]
                futures_symbol = futures_by_asset[(base, futures_quote)]
                key = (
                    spot_symbol if spot_symbol == futures_symbol
                    else f"{spot_symbol}__{futures_symbol}"
                )
                routes[key] = PairRoute(
                    key, base, spot_symbol, futures_symbol,
                    spot_quote, futures_quote, conversion_symbol, inverted,
                )
        return routes

    def futures_futures_routes(
        self, filters: FilterConfig
    ) -> dict[str, FuturesFuturesRoute]:
        if not {"USDT", "USDC"}.issubset(self.quote_assets):
            return {}
        conversion = self._conversion_for("USDC", "USDT")
        if conversion is None:
            return {}
        conversion_symbol, inverted = conversion
        usdt_by_base: dict[str, str] = {}
        usdc_by_base: dict[str, str] = {}
        for symbol in list(self.futures_volumes):
            parsed = self._split_symbol(symbol)
            if not parsed:
                continue
            base, quote = parsed
            if quote == "USDT":
                usdt_by_base[base] = symbol
            elif quote == "USDC":
                usdc_by_base[base] = symbol
        return {
            f"{usdt_by_base[base]}__{usdc_by_base[base]}": FuturesFuturesRoute(
                f"{usdt_by_base[base]}__{usdc_by_base[base]}",
                base,
                usdt_by_base[base],
                usdc_by_base[base],
                conversion_symbol,
                inverted,
            )
            for base in usdt_by_base.keys() & usdc_by_base.keys()
        }

    def common_symbols(self, filters: FilterConfig | None = None) -> set[str]:
        filters = filters or FilterConfig(
            quote_assets=tuple(sorted(self.quote_assets)),
            cross_quote_pairings=self.cross_quote_pairings,
        )
        return set(self.pair_routes(filters))

    def set_pairing_config(self, filters: FilterConfig) -> None:
        cross = tuple(tuple(pairing) for pairing in filters.cross_quote_pairings)
        assets = set(filters.quote_assets)
        for spot_quote, futures_quote in cross:
            assets.update((spot_quote, futures_quote))
        updated_assets = frozenset(assets)
        if (
            updated_assets != self.quote_assets
            or cross != self.cross_quote_pairings
        ):
            assets_changed = updated_assets != self.quote_assets
            self.quote_assets = updated_assets
            self.cross_quote_pairings = cross
            if assets_changed:
                self._remove_disallowed_market_data()
            self.active_routes.clear()
            self._spot_futures_basis_cache.clear()
            self._futures_futures_basis_cache.clear()
            self.routes_version += 1

    def set_quote_assets(self, quote_assets: tuple[str, ...] | list[str]) -> None:
        updated_assets = frozenset(quote_assets)
        if updated_assets != self.quote_assets or self.cross_quote_pairings:
            self.quote_assets = updated_assets
            self.cross_quote_pairings = ()
            self._remove_disallowed_market_data()
            self.active_routes.clear()
            self._spot_futures_basis_cache.clear()
            self._futures_futures_basis_cache.clear()
            self.routes_version += 1

    def _remove_disallowed_market_data(self) -> None:
        """Drop cached rows that no longer belong to the effective quote universe."""
        for destination in (
            self.spot_volumes, self.futures_volumes,
            self.spot_books, self.futures_books,
        ):
            for symbol in list(destination):
                if not self.is_allowed_symbol(symbol):
                    destination.pop(symbol, None)

    def set_book_chunk_connected(
        self, market: str, connection_key: str, connected: bool
    ) -> None:
        chunks = self.book_connected_chunks[market]
        if connected:
            chunks.add(connection_key)
        else:
            chunks.discard(connection_key)
        self.book_connected[market] = bool(chunks)

    def is_allowed_symbol(self, symbol: str) -> bool:
        return any(symbol.endswith(asset) for asset in self.quote_assets)

    def liquid_routes(self, filters: FilterConfig) -> dict[str, PairRoute]:
        return {
            key: route
            for key, route in self.pair_routes(filters).items()
            if self.spot_volumes[route.spot_symbol].quote_volume >= filters.min_spot_volume
            and self.futures_volumes[route.futures_symbol].quote_volume >= filters.min_futures_volume
        }

    def liquid_symbols(self, filters: FilterConfig) -> set[str]:
        return set(self.liquid_routes(filters))

    def liquid_futures_futures_routes(
        self, filters: FilterConfig
    ) -> dict[str, FuturesFuturesRoute]:
        return {
            key: route
            for key, route in self.futures_futures_routes(filters).items()
            if self.futures_volumes[route.usdt_symbol].quote_volume
            >= filters.min_futures_volume
            and self.futures_volumes[route.usdc_symbol].quote_volume
            >= filters.min_futures_volume
        }

    def book_symbols(self, market: str, filters: FilterConfig) -> set[str]:
        routes = self.liquid_routes(filters)
        futures_futures = self.liquid_futures_futures_routes(filters)
        self.active_routes = routes
        if market == "futures":
            symbols = {route.futures_symbol for route in routes.values()}
            symbols.update(route.usdt_symbol for route in futures_futures.values())
            symbols.update(route.usdc_symbol for route in futures_futures.values())
            return symbols
        symbols = {route.spot_symbol for route in routes.values()}
        symbols.update(
            route.conversion_symbol for route in routes.values()
            if route.conversion_symbol
        )
        symbols.update(
            route.conversion_symbol for route in futures_futures.values()
        )
        return symbols

    def _conversion_rate(self, route: PairRoute) -> tuple[float, float] | None:
        if not route.conversion_symbol:
            return 1.0, 0.0
        quote = self.spot_books.get(route.conversion_symbol)
        if quote is None or quote.mid <= 0:
            return None
        rate = 1 / quote.mid if route.conversion_inverted else quote.mid
        return rate, quote.received_at

    def _quote_quality(
        self,
        quotes: list[BookQuote], now: float, max_age_seconds: float,
        tolerance_seconds: float,
    ) -> tuple[bool, str, tuple[float, ...], float]:
        ages = tuple((now - quote.received_at) * 1000 for quote in quotes)
        received_at = [quote.received_at for quote in quotes]
        skew_ms = (max(received_at) - min(received_at)) * 1000
        if max(ages) > max_age_seconds * 1000:
            return False, "quote_age", ages, skew_ms
        transport_lags = [
            quote.transport_lag_ms
            for quote in quotes
            if quote.transport_lag_ms is not None
        ]
        if transport_lags and max(transport_lags) > self.max_transport_lag_ms:
            return False, "transport_lag", ages, skew_ms
        if skew_ms > tolerance_seconds * 1000:
            return False, "quote_skew", ages, skew_ms
        return True, "", ages, skew_ms

    def _record_quote_rejection(
        self, market_type: str, symbol: str, basis_bps: float,
        reason: str, ages_ms: tuple[float, ...], skew_ms: float,
        quotes: list[BookQuote],
    ) -> None:
        self.quote_rejections.append({
            "market_type": market_type,
            "symbol": symbol,
            "rejected_at_wall": time.time(),
            "rejected_at_mono": time.monotonic(),
            "rejected_basis_bps": basis_bps,
            "reason": reason,
            "raw_quote_skew_ms": skew_ms,
            "raw_leg_ages_ms": list(ages_ms),
            "exchange_event_times_ms": [
                quote.exchange_event_time_ms for quote in quotes
            ],
            "transport_lags_ms": [quote.transport_lag_ms for quote in quotes],
        })

    def drain_quote_rejections(self) -> list[dict[str, Any]]:
        rows, self.quote_rejections = self.quote_rejections, []
        return rows

    def _bbo_snapshot(self, route: PairRoute, source_market: str) -> BboSnapshot | None:
        spot = self.spot_books.get(route.spot_symbol)
        futures = self.futures_books.get(route.futures_symbol)
        conversion = self._conversion_rate(route)
        if spot is None or futures is None or conversion is None:
            return None
        rate, conversion_received_at = conversion
        return BboSnapshot(
            route.key, route.spot_symbol, route.futures_symbol,
            source_market, time.time(), spot.bid, spot.ask,
            futures.bid, futures.ask, spot.received_at, futures.received_at,
            rate, route.conversion_symbol, conversion_received_at,
        )

    def get_bbo_snapshot(
        self, route: PairRoute, max_age_seconds: float,
        tolerance_seconds: float = 0.2,
    ) -> BboSnapshot | None:
        """仅使用当前最新且时间质量达标的 BBO。"""
        spot = self.spot_books.get(route.spot_symbol)
        futures = self.futures_books.get(route.futures_symbol)
        conversion = (
            self.spot_books.get(route.conversion_symbol)
            if route.conversion_symbol else None
        )
        if spot is None or futures is None or (route.conversion_symbol and conversion is None):
            return None
        quotes = [spot, futures] + ([conversion] if conversion is not None else [])
        valid, _, _, _ = self._quote_quality(
            quotes, time.monotonic(), max_age_seconds, tolerance_seconds
        )
        if not valid:
            return None
        rate = 1.0
        conversion_received_at = 0.0
        if conversion is not None:
            rate = (
                1 / conversion.mid
                if route.conversion_inverted else conversion.mid
            )
            conversion_received_at = conversion.received_at
        return BboSnapshot(
            route.key, route.spot_symbol, route.futures_symbol,
            "unified", time.time(),
            spot.bid, spot.ask, futures.bid, futures.ask,
            spot.received_at, futures.received_at,
            rate, route.conversion_symbol, conversion_received_at,
        )

    def eligible_symbols(self, filters: FilterConfig) -> set[str]:
        return {
            key for key, route in self.liquid_routes(filters).items()
            if (spot := self.spot_books.get(route.spot_symbol)) is not None
            and spot.mid > filters.min_spot_price
            and self._conversion_rate(route) is not None
        }

    def streams_healthy(self, stale_seconds: float) -> bool:
        now = time.monotonic()
        return all(self.ticker_connected.values()) and all(self.book_connected.values()) and all(
            now - updated <= stale_seconds
            for updated in (*self.ticker_last_update.values(), *self.book_last_update.values())
        )

    def snapshots(
        self, filters: FilterConfig, max_age_seconds: float,
        tolerance_seconds: float = 0.2,
        stream_stale_seconds: float | None = None,
    ) -> list[PairSnapshot]:
        if not self.streams_healthy(stream_stale_seconds or max_age_seconds):
            return []
        now = time.monotonic()
        result: list[PairSnapshot] = []
        routes = self.liquid_routes(filters)
        for key in self.eligible_symbols(filters):
            route = routes[key]
            spot = self.spot_books.get(route.spot_symbol)
            futures = self.futures_books.get(route.futures_symbol)
            conversion_quote = (
                self.spot_books.get(route.conversion_symbol)
                if route.conversion_symbol else None
            )
            if spot is None or futures is None or (
                route.conversion_symbol and conversion_quote is None
            ):
                continue
            signature = (
                spot.version, futures.version,
                conversion_quote.version if conversion_quote is not None else 0,
            )
            cached = self._spot_futures_basis_cache.get(key)
            if cached is not None and cached[0] == signature:
                _, spot_mid, futures_mid, basis_bps, rate = cached
            else:
                rate = 1.0
                if conversion_quote is not None:
                    rate = (
                        1 / conversion_quote.mid
                        if route.conversion_inverted else conversion_quote.mid
                    )
                spot_mid, futures_mid = spot.mid, futures.mid * rate
                basis_bps = (futures_mid / spot_mid - 1) * 10_000
                self._spot_futures_basis_cache[key] = (
                    signature, spot_mid, futures_mid, basis_bps, rate,
                )
            quotes = [spot, futures] + (
                [conversion_quote] if conversion_quote is not None else []
            )
            valid, reason, ages_ms, skew_ms = self._quote_quality(
                quotes, now, max_age_seconds, tolerance_seconds
            )
            if not valid:
                self._record_quote_rejection(
                    "spot_futures", key, basis_bps, reason,
                    ages_ms, skew_ms, quotes,
                )
                continue
            result.append(PairSnapshot(
                key, spot_mid, futures_mid,
                basis_bps,
                route.spot_symbol, route.futures_symbol, rate,
                "spot_futures", skew_ms, ages_ms,
                tuple(quote.exchange_event_time_ms for quote in quotes),
            ))
        return result

    def futures_futures_snapshots(
        self, filters: FilterConfig, max_age_seconds: float,
        tolerance_seconds: float = 0.2,
        stream_stale_seconds: float | None = None,
    ) -> list[PairSnapshot]:
        if not self.streams_healthy(stream_stale_seconds or max_age_seconds):
            return []
        now = time.monotonic()
        result: list[PairSnapshot] = []
        for route in self.liquid_futures_futures_routes(filters).values():
            usdt = self.futures_books.get(route.usdt_symbol)
            usdc = self.futures_books.get(route.usdc_symbol)
            conversion_book = self.spot_books.get(route.conversion_symbol)
            if usdt is None or usdc is None or conversion_book is None:
                continue
            signature = (usdt.version, usdc.version, conversion_book.version)
            cached = self._futures_futures_basis_cache.get(route.key)
            if cached is not None and cached[0] == signature:
                _, usdt_mid, normalized_usdc_mid, basis_bps, conversion_rate = cached
            else:
                conversion_rate = (
                    1 / conversion_book.mid
                    if route.conversion_inverted else conversion_book.mid
                )
                usdt_mid = usdt.mid
                normalized_usdc_mid = usdc.mid * conversion_rate
                basis_bps = (normalized_usdc_mid / usdt_mid - 1) * 10_000
                self._futures_futures_basis_cache[route.key] = (
                    signature, usdt_mid, normalized_usdc_mid,
                    basis_bps, conversion_rate,
                )
            quotes = [usdt, usdc, conversion_book]
            valid, reason, ages_ms, skew_ms = self._quote_quality(
                quotes, now, max_age_seconds, tolerance_seconds
            )
            if not valid:
                self._record_quote_rejection(
                    "futures_futures", route.key, basis_bps, reason,
                    ages_ms, skew_ms, quotes,
                )
                continue
            result.append(PairSnapshot(
                route.key,
                usdt_mid,
                normalized_usdc_mid,
                basis_bps,
                route.usdt_symbol,
                route.usdc_symbol,
                conversion_rate,
                "futures_futures",
                skew_ms, ages_ms,
                tuple(quote.exchange_event_time_ms for quote in quotes),
            ))
        return result

    def futures_futures_quote_age(
        self, symbol: str, filters: FilterConfig
    ) -> float | None:
        route = self.liquid_futures_futures_routes(filters).get(symbol)
        if route is None:
            return None
        usdt = self.futures_books.get(route.usdt_symbol)
        usdc = self.futures_books.get(route.usdc_symbol)
        conversion = self.spot_books.get(route.conversion_symbol)
        if not usdt or not usdc or not conversion:
            return None
        now = time.monotonic()
        return max(
            now - usdt.received_at,
            now - usdc.received_at,
            now - conversion.received_at,
        )

    def futures_futures_label(self, symbol: str, filters: FilterConfig) -> str:
        route = self.futures_futures_routes(filters).get(symbol)
        return route.label if route else symbol

    def futures_futures_volumes(
        self, symbol: str, filters: FilterConfig
    ) -> tuple[float, float]:
        route = self.futures_futures_routes(filters).get(symbol)
        if route is None:
            return 0.0, 0.0
        return (
            self.futures_volumes[route.usdt_symbol].quote_volume,
            self.futures_volumes[route.usdc_symbol].quote_volume,
        )

    def quote_age(self, symbol: str, filters: FilterConfig | None = None) -> float | None:
        filters = filters or FilterConfig(
            quote_assets=tuple(sorted(self.quote_assets)),
            cross_quote_pairings=self.cross_quote_pairings,
        )
        route = self.pair_routes(filters).get(symbol)
        if route is None:
            return None
        spot = self.spot_books.get(route.spot_symbol)
        futures = self.futures_books.get(route.futures_symbol)
        conversion = self._conversion_rate(route)
        if not spot or not futures or conversion is None:
            return None
        _, conversion_received_at = conversion
        now = time.monotonic()
        ages = [now - spot.received_at, now - futures.received_at]
        if route.conversion_symbol:
            ages.append(now - conversion_received_at)
        return max(ages)

    def route(self, symbol: str, filters: FilterConfig) -> PairRoute | None:
        return self.pair_routes(filters).get(symbol)

    def pair_label(self, symbol: str, filters: FilterConfig) -> str:
        route = self.route(symbol, filters)
        return route.label if route else symbol

    def pair_volumes(self, symbol: str, filters: FilterConfig) -> tuple[float, float]:
        route = self.route(symbol, filters)
        if route is None:
            return 0.0, 0.0
        return (
            self.spot_volumes[route.spot_symbol].quote_volume,
            self.futures_volumes[route.futures_symbol].quote_volume,
        )
