from __future__ import annotations

import asyncio
import copy
import json
import os
import queue
import signal
import threading
import time
import unicodedata
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generic, TypeVar

import websockets
import numpy as np
from functools import partial
from websockets.asyncio.client import ClientConnection
from websockets.frames import Frame, OP_TEXT, OP_BINARY, OP_CONT

BASE_DIR = Path(__file__).resolve().parent
# 24hr ticker
SPOT_TICKER_STREAM = "wss://data-stream.binance.vision/ws/!miniTicker@arr"
FUTURES_TICKER_STREAM = "wss://fstream.binance.com/market/ws/!ticker@arr"
# 流
SPOT_BOOK_BASE = "wss://data-stream.binance.vision/stream?streams="
FUTURES_BOOK_BASE = "wss://fstream.binance.com/public/stream?streams="
# Binance 每20秒主动发送 Ping；websockets 会自动回传同载荷 Pong。
# 禁用客户端额外 Ping，避免本地 ping_timeout 以 1011 主动断开连接。
BINANCE_WEBSOCKET_OPTIONS = {
    "proxy": None,
    "open_timeout": 20,
    "ping_interval": None,
    "ping_timeout": None,
    "close_timeout": 5,
    "max_size": 8_000_000,
}
FILTER_CONFIG_PATH = BASE_DIR / "filter_config.json"
STATISTICS_CONFIG_PATH = BASE_DIR / "statistics_config.json"
FUTURES_FUTURES_STATISTICS_CONFIG_PATH = BASE_DIR / "futures_futures_statistics_config.json"
RUNTIME_CONFIG_PATH = BASE_DIR / "runtime_config.json"


# 配置文件类
@dataclass(frozen=True, slots=True)
class FilterConfig:
    min_spot_volume: float = 10_000_000
    min_futures_volume: float = 10_000_000
    top: int = 10
    positive_basis_only: bool = True
    min_spot_price: float = 0.1
    quote_assets: tuple[str, ...] = ("USDT",)
    cross_quote_pairings: tuple[tuple[str, str], ...] = ()

    def validate(self) -> None:
        if self.min_spot_volume < 0 or self.min_futures_volume < 0:
            raise ValueError("成交额门槛不能小于 0")
        if not isinstance(self.top, int) or isinstance(self.top, bool) or self.top < 1:
            raise ValueError("top 必须是大于 0 的整数")
        if not isinstance(self.positive_basis_only, bool):
            raise ValueError("positive_basis_only 必须是布尔值")
        if self.min_spot_price < 0:
            raise ValueError("min_spot_price 不能小于 0")
        if not isinstance(self.quote_assets, (list, tuple)) or not self.quote_assets:
            raise ValueError("quote_assets 必须是非空数组")
        if any(not isinstance(asset, str) or not asset.isupper() for asset in self.quote_assets):
            raise ValueError("quote_assets 必须是大写报价币种字符串")
        if not isinstance(self.cross_quote_pairings, (list, tuple)):
            raise ValueError("cross_quote_pairings 必须是数组")
        for pairing in self.cross_quote_pairings:
            if (
                not isinstance(pairing, (list, tuple))
                or len(pairing) != 2
                or any(not isinstance(asset, str) or not asset.isupper() for asset in pairing)
                or pairing[0] == pairing[1]
            ):
                raise ValueError(
                    "cross_quote_pairings 每项必须是两个不同的大写报价币种，顺序为[现货, 永续]"
                )


@dataclass(frozen=True, slots=True)
class StatisticsConfig:
    total_window_samples: int = 432_000
    long_window_samples: int = 144_000
    short_window_samples: int = 54_000
    quantile_low: float = 0.05
    quantile_high: float = 0.95
    sigma_multiplier: float = 2.0
    min_k_sigma_bps: float = 3.0
    open_threshold_bps: float = 7.0
    close_threshold_bps: float = -5.0
    bbo_record_interval_ms: int = 1000

    def validate(self) -> None:
        windows = (
            self.short_window_samples,
            self.long_window_samples,
            self.total_window_samples,
        )
        if any(not isinstance(value, int) or value < 2 for value in windows):
            raise ValueError("三个窗口样本数必须是至少为 2 的整数")
        if not (
            self.short_window_samples
            <= self.long_window_samples
            <= self.total_window_samples
        ):
            raise ValueError("必须满足 short_window_samples <= long_window_samples <= total_window_samples")
        if not 0 <= self.quantile_low < self.quantile_high <= 1:
            raise ValueError("分位数必须满足 0 <= quantile_low < quantile_high <= 1")
        if self.sigma_multiplier <= 0 or self.min_k_sigma_bps <= 0:
            raise ValueError("sigma_multiplier 和 min_k_sigma_bps 必须大于 0")
        if self.open_threshold_bps <= 0:
            raise ValueError("open_threshold_bps 必须大于 0")
        if self.close_threshold_bps >= 0:
            raise ValueError("close_threshold_bps 必须小于 0")
        if not isinstance(self.bbo_record_interval_ms, int) or self.bbo_record_interval_ms < 1:
            raise ValueError("bbo_record_interval_ms 必须是至少为 1 的整数")


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    sample_interval_ms: int = 200
    display_refresh_seconds: float = 1.0
    stale_seconds: float = 2.0
    quote_max_age_ms: int = 1000
    quote_match_tolerance_ms: int = 200
    max_transport_lag_ms: int = 2000
    persist_interval_seconds: float = 60.0
    quantile_refresh_seconds: float = 15.0
    background_write_queue_size: int = 10_000
    book_symbols_per_connection: int = 80
    raw_message_capacity: int = 512
    decode_batch_size: int = 32
    data_directory: str = "data"

    def validate(self) -> None:
        if not isinstance(self.sample_interval_ms, int) or self.sample_interval_ms < 50:
            raise ValueError("sample_interval_ms 必须是至少 50 的整数")
        for name in ("raw_message_capacity", "decode_batch_size"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65536:
                raise ValueError(f"{name} 必须是 1 到 65536 的整数")
        if self.display_refresh_seconds <= 0 or self.stale_seconds <= 0:
            raise ValueError("显示刷新和行情过期时间必须大于 0")
        if not isinstance(self.quote_max_age_ms, int) or self.quote_max_age_ms < 1:
            raise ValueError("quote_max_age_ms 必须是大于 0 的整数")
        if (
            not isinstance(self.quote_match_tolerance_ms, int)
            or self.quote_match_tolerance_ms < 0
            or self.quote_match_tolerance_ms > self.quote_max_age_ms
        ):
            raise ValueError("quote_match_tolerance_ms 必须是 0 到 quote_max_age_ms 之间的整数")
        if not isinstance(self.max_transport_lag_ms, int) or self.max_transport_lag_ms < 500:
            raise ValueError("max_transport_lag_ms 必须是至少 500 的整数")
        if self.persist_interval_seconds <= 0 or not self.data_directory.strip():
            raise ValueError("持久化间隔必须大于 0，数据目录不能为空")
        if self.quantile_refresh_seconds <= 0:
            raise ValueError("quantile_refresh_seconds 必须大于 0")
        if (
            not isinstance(self.background_write_queue_size, int)
            or self.background_write_queue_size < 100
        ):
            raise ValueError("background_write_queue_size 必须是至少 100 的整数")
        if (
            not isinstance(self.book_symbols_per_connection, int)
            or not 1 <= self.book_symbols_per_connection <= 200
        ):
            raise ValueError("book_symbols_per_connection 必须是 1 到 200 的整数")


ConfigT = TypeVar("ConfigT")


# 配置文件管理类（读取、解析、验证、热加载 JSON 配置文件）
class ConfigManager(Generic[ConfigT]):
    def __init__(self, path: Path, config_type: type[ConfigT]) -> None:
        self.path = path
        self.config_type = config_type
        self.current = config_type()
        self.error = ""
        self._mtime_ns: int | None = None
        self.reload(force=True)

    def reload(self, force: bool = False) -> bool:
        try:
            mtime_ns = self.path.stat().st_mtime_ns
            if not force and mtime_ns == self._mtime_ns:
                return False
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise ValueError("配置文件顶层必须是 JSON 对象")
            fields = self.config_type.__dataclass_fields__  # type: ignore[attr-defined]
            unknown = set(raw) - set(fields)
            # 如果有未知配置项就抛异常
            if unknown:
                raise ValueError(f"未知配置项: {', '.join(sorted(unknown))}")
            # **raw解包
            updated = self.config_type(**raw)
            updated.validate()  # type: ignore[attr-defined]
        except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
            self.error = f"{type(exc).__name__}: {exc}（继续使用上一份有效配置）"
            return False
        self.current = updated
        self._mtime_ns = mtime_ns
        self.error = ""
        return True

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

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


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
        self.quote_rejections: list[dict[str, Any]] = []
        self.max_transport_lag_ms: float = 2000.0
        self.raw_buffers: dict[str, RawMessageBuffer] = {}
        self.loop_lag_ms = 0.0
        self.max_loop_lag_ms = 0.0
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
        self.errors = {
            "spot_ticker": "", "futures_ticker": "", "spot_book": "",
            "futures_book": "",
        }

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
        quote = BookQuote(
            bid, ask, now, exchange_event_time_ms, transport_lag_ms
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
            self.routes_version += 1

    def set_quote_assets(self, quote_assets: tuple[str, ...] | list[str]) -> None:
        updated_assets = frozenset(quote_assets)
        if updated_assets != self.quote_assets or self.cross_quote_pairings:
            self.quote_assets = updated_assets
            self.cross_quote_pairings = ()
            self._remove_disallowed_market_data()
            self.active_routes.clear()
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
            rate = 1.0
            if conversion_quote is not None:
                rate = (
                    1 / conversion_quote.mid
                    if route.conversion_inverted else conversion_quote.mid
                )
            spot_mid, futures_mid = spot.mid, futures.mid * rate
            basis_bps = (futures_mid / spot_mid - 1) * 10_000
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
            conversion_rate = (
                1 / conversion_book.mid
                if route.conversion_inverted else conversion_book.mid
            )
            normalized_usdc_mid = usdc.mid * conversion_rate
            basis_bps = (normalized_usdc_mid / usdt.mid - 1) * 10_000
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
                usdt.mid,
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


@dataclass(slots=True)
class PairStatistics:
    symbol: str
    sample_count: int = 0
    observed_seconds: float = 0.0
    mean_basis_bps: float = 0.0
    rolling_sum: float = 0.0
    rolling_sum_squares: float = 0.0
    current_spot_mid: float = 0.0
    current_futures_mid: float = 0.0
    current_basis_bps: float = 0.0
    current_deviation_bps: float = 0.0
    open_count: int = 0
    close_count: int = 0
    upper_excursion_active: bool = False
    lower_excursion_active: bool = False
    last_sample_at: float | None = None
    basis_samples: deque[float] = field(default_factory=deque, repr=False)
    last_bbo_record_at: float | None = None
    quantile_cache: dict[tuple[int, float, float], tuple[float, float]] = field(
        default_factory=dict, repr=False
    )
    sample_sequence: int = field(default=0, repr=False)
    minimum_samples: deque[tuple[int, float]] = field(default_factory=deque, repr=False)
    maximum_samples: deque[tuple[int, float]] = field(default_factory=deque, repr=False)

    @property
    def standard_deviation_bps(self) -> float:
        if self.sample_count < 2:
            return 0.0
        variance = (
            self.rolling_sum_squares / self.sample_count
            - self.mean_basis_bps * self.mean_basis_bps
        )
        return max(0.0, variance) ** 0.5

    @property
    def effective_opportunity_count(self) -> int:
        return min(self.open_count, self.close_count)

    @property
    def sigma_position(self) -> float:
        sigma = self.standard_deviation_bps
        return self.current_deviation_bps / sigma if sigma > 0 else 0.0

    def quantile_range(
        self, sample_size: int, low: float, high: float
    ) -> tuple[float, float] | None:
        if not self.basis_samples:
            return None
        count = min(sample_size, self.sample_count)
        values = np.fromiter(islice(reversed(self.basis_samples), count), dtype=np.float64, count=count)
        return _percentile_range(values, low, high)

    def cached_quantile_range(
        self, sample_size: int, low: float, high: float
    ) -> tuple[float, float] | None:
        """Return the last background-computed value without sorting here."""
        return self.quantile_cache.get((sample_size, low, high))

    def allow_bbo_record(self, timestamp: float, interval_seconds: float) -> bool:
        if self.last_bbo_record_at is not None and timestamp - self.last_bbo_record_at < interval_seconds:
            return False
        self.last_bbo_record_at = timestamp
        return True

    @property
    def runtime_range(self) -> tuple[float, float] | None:
        if not self.minimum_samples or not self.maximum_samples:
            return None
        return self.minimum_samples[0][1], self.maximum_samples[0][1]

    def _expire_range_samples(self) -> None:
        if not self.basis_samples:
            self.minimum_samples.clear()
            self.maximum_samples.clear()
            return
        oldest_sequence = self.sample_sequence - len(self.basis_samples) + 1
        while self.minimum_samples and self.minimum_samples[0][0] < oldest_sequence:
            self.minimum_samples.popleft()
        while self.maximum_samples and self.maximum_samples[0][0] < oldest_sequence:
            self.maximum_samples.popleft()

    def resize_window(self, total_window_samples: int) -> None:
        while len(self.basis_samples) > total_window_samples:
            removed = self.basis_samples.popleft()
            self.rolling_sum -= removed
            self.rolling_sum_squares -= removed * removed
        self.sample_count = len(self.basis_samples)
        self.mean_basis_bps = (
            self.rolling_sum / self.sample_count if self.sample_count else 0.0
        )
        self._expire_range_samples()

    def _append_sample(self, basis_bps: float, total_window_samples: int) -> None:
        self.sample_sequence += 1
        self.basis_samples.append(basis_bps)
        while self.minimum_samples and self.minimum_samples[-1][1] >= basis_bps:
            self.minimum_samples.pop()
        self.minimum_samples.append((self.sample_sequence, basis_bps))
        while self.maximum_samples and self.maximum_samples[-1][1] <= basis_bps:
            self.maximum_samples.pop()
        self.maximum_samples.append((self.sample_sequence, basis_bps))
        self.rolling_sum += basis_bps
        self.rolling_sum_squares += basis_bps * basis_bps
        self.resize_window(total_window_samples)

    def update(
        self,
        snapshot: PairSnapshot,
        now_wall: float,
        sample_interval_seconds: float,
        config: StatisticsConfig,
    ) -> list[dict[str, Any]]:
        current_basis_bps = snapshot.basis_bps
        previous_mean = self.mean_basis_bps
        previous_sigma = self.standard_deviation_bps

        events: list[dict[str, Any]] = []

        self.current_spot_mid = snapshot.spot_mid
        self.current_futures_mid = snapshot.futures_mid
        self.current_basis_bps = current_basis_bps
        self.current_deviation_bps = (
            current_basis_bps - previous_mean if self.sample_count else 0.0
        )

        if self.sample_count:
            if not self.upper_excursion_active and self.current_deviation_bps >= config.open_threshold_bps:
                self.upper_excursion_active = True
            elif self.upper_excursion_active and self.current_deviation_bps <= 0:
                self.open_count += 1
                self.upper_excursion_active = False
                events.append({
                    "event": "open_opportunity",
                    "market_type": snapshot.market_type,
                    "time": datetime.fromtimestamp(now_wall, timezone.utc).isoformat(),
                    "leg1_symbol": snapshot.spot_symbol or snapshot.symbol,
                    "leg2_symbol": snapshot.futures_symbol or snapshot.symbol,
                    "spot_symbol": snapshot.spot_symbol or snapshot.symbol,
                    "futures_symbol": snapshot.futures_symbol or snapshot.symbol,
                    "spot_mid": snapshot.spot_mid,
                    "futures_mid": snapshot.futures_mid,
                    "conversion_rate": snapshot.conversion_rate,
                    "mean_basis_bps": previous_mean,
                    "standard_deviation_bps": previous_sigma,
                    "trigger_basis_bps": previous_mean + config.open_threshold_bps,
                    "basis_bps": current_basis_bps,
                    "deviation_bps": self.current_deviation_bps,
                    "opportunity_number": self.open_count,
                })

            if not self.lower_excursion_active and self.current_deviation_bps <= config.close_threshold_bps:
                self.lower_excursion_active = True
            elif self.lower_excursion_active and self.current_deviation_bps >= 0:
                self.close_count += 1
                self.lower_excursion_active = False
                events.append({
                    "event": "close_opportunity",
                    "market_type": snapshot.market_type,
                    "time": datetime.fromtimestamp(now_wall, timezone.utc).isoformat(),
                    "leg1_symbol": snapshot.spot_symbol or snapshot.symbol,
                    "leg2_symbol": snapshot.futures_symbol or snapshot.symbol,
                    "spot_symbol": snapshot.spot_symbol or snapshot.symbol,
                    "futures_symbol": snapshot.futures_symbol or snapshot.symbol,
                    "spot_mid": snapshot.spot_mid,
                    "futures_mid": snapshot.futures_mid,
                    "conversion_rate": snapshot.conversion_rate,
                    "mean_basis_bps": previous_mean,
                    "standard_deviation_bps": previous_sigma,
                    "trigger_basis_bps": previous_mean + config.close_threshold_bps,
                    "basis_bps": current_basis_bps,
                    "deviation_bps": self.current_deviation_bps,
                    "opportunity_number": self.close_count,
                })

        self.observed_seconds += sample_interval_seconds

        self._append_sample(current_basis_bps, config.total_window_samples)
        self.last_sample_at = now_wall
        return events


class PairDirectoryStore:
    """每个交易对一个目录：状态覆盖写，机会事件仅追加写。"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, PairStatistics]:
        # 滑动窗口不从 state.json 恢复；每次启动从空窗口重新统计。
        return {}

    def save_states(self, statistics: list[PairStatistics]) -> None:
        self.save_state_rows(self.snapshot_states(statistics))

    @staticmethod
    def snapshot_states(statistics: list[PairStatistics]) -> list[tuple[str, dict[str, Any]]]:
        """Copy mutable statistics before handing them to the disk thread."""
        rows: list[tuple[str, dict[str, Any]]] = []
        for stats in statistics:
            rows.append((stats.symbol, {
                "sample_count": stats.sample_count,
                "observed_seconds": stats.observed_seconds,
                "mean_basis_bps": stats.mean_basis_bps,
                "standard_deviation_bps": stats.standard_deviation_bps,
                "current_spot_mid": stats.current_spot_mid,
                "current_futures_mid": stats.current_futures_mid,
                "current_basis_bps": stats.current_basis_bps,
                "current_deviation_bps": stats.current_deviation_bps,
                "open_count": stats.open_count,
                "close_count": stats.close_count,
                "upper_excursion_active": stats.upper_excursion_active,
                "lower_excursion_active": stats.lower_excursion_active,
                "last_sample_at": stats.last_sample_at,
            }))
        return rows

    def save_state_rows(self, rows: list[tuple[str, dict[str, Any]]]) -> None:
        for symbol, row in rows:
            pair_dir = self.root / symbol
            pair_dir.mkdir(parents=True, exist_ok=True)
            target = pair_dir / "state.json"
            temporary = pair_dir / "state.json.tmp"
            temporary.write_text(
                # 缩进 2 格，中文保留
                json.dumps(row, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
            )
            temporary.replace(target)

    def append_opportunity(self, symbol: str, opportunity: dict[str, Any]) -> None:
        pair_dir = self.root / symbol
        pair_dir.mkdir(parents=True, exist_ok=True)
        # with自动管理资源释放，open  a追加写入，w覆盖写入，r读取
        with (pair_dir / "opportunities.jsonl").open("a", encoding="utf-8", newline="\n") as handle:
            # separators=(",", ":") 去掉，和：后的空格，无所谓的
            handle.write(json.dumps(opportunity, ensure_ascii=False, separators=(",", ":")) + "\n")
            # 强制刷新写入磁盘
            handle.flush()

    def append_bbo_records(self, records: list[tuple[str, dict[str, Any]]]) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for symbol, record in records:
            grouped.setdefault(symbol, []).append(record)
        for symbol, rows in grouped.items():
            pair_dir = self.root / symbol
            pair_dir.mkdir(parents=True, exist_ok=True)
            with (pair_dir / "bbo_above_threshold.jsonl").open(
                "a", encoding="utf-8", newline="\n"
            ) as handle:
                for row in rows:
                    handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
                handle.flush()


class BasisDiagnosticStore:
    """用下一条合格报价复核因年龄或时差被拒绝的最新报价。"""

    def __init__(self, root: Path, threshold_bps: float = 3.0) -> None:
        self.root = root
        self.threshold_bps = threshold_bps
        self.last_record_at: dict[str, float] = {}
        self.pending: dict[str, dict[str, Any]] = {}

    def process(
        self, rejections: list[dict[str, Any]],
        snapshots: list[PairSnapshot], now_wall: float, write: bool = True,
    ) -> list[dict[str, Any]]:
        for rejection in rejections:
            key = f"{rejection['market_type']}:{rejection['symbol']}"
            self.pending[key] = rejection

        rows: list[dict[str, Any]] = []
        for snapshot in snapshots:
            key = f"{snapshot.market_type}:{snapshot.symbol}"
            rejection = self.pending.pop(key, None)
            if rejection is None:
                continue
            confirmation_delay_ms = (
                time.monotonic() - rejection["rejected_at_mono"]
            ) * 1000
            if confirmation_delay_ms > 2000:
                continue
            difference = rejection["rejected_basis_bps"] - snapshot.basis_bps
            if abs(difference) < self.threshold_bps:
                continue
            if now_wall - self.last_record_at.get(key, 0.0) < 10.0:
                continue
            self.last_record_at[key] = now_wall
            rows.append({
                "rejected_time": datetime.fromtimestamp(
                    rejection["rejected_at_wall"], timezone.utc
                ).isoformat(),
                "confirmed_time": datetime.fromtimestamp(
                    now_wall, timezone.utc
                ).isoformat(),
                "market_type": snapshot.market_type,
                "symbol": snapshot.symbol,
                "rejection_reason": rejection["reason"],
                "rejected_basis_bps": rejection["rejected_basis_bps"],
                "next_aligned_basis_bps": snapshot.basis_bps,
                "reversion_bps": difference,
                "confirmation_delay_ms": confirmation_delay_ms,
                "rejected_quote_skew_ms": rejection["raw_quote_skew_ms"],
                "rejected_leg_ages_ms": rejection["raw_leg_ages_ms"],
                "aligned_quote_skew_ms": snapshot.quote_skew_ms,
                "aligned_leg_ages_ms": list(snapshot.leg_ages_ms),
                "rejected_exchange_event_times_ms": rejection[
                    "exchange_event_times_ms"
                ],
                "rejected_transport_lags_ms": rejection.get(
                    "transport_lags_ms", []
                ),
                "aligned_exchange_event_times_ms": list(
                    snapshot.exchange_event_times_ms
                ),
            })
        if not rows:
            return []
        if write:
            self.append_rows(rows)
        return rows

    def append_rows(self, rows: list[dict[str, Any]]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / "basis_recheck.jsonl").open(
            "a", encoding="utf-8", newline="\n"
        ) as handle:
            for row in rows:
                handle.write(
                    json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
                )
            handle.flush()


class BackgroundWriter:
    """Single bounded disk queue; market-data tasks never perform filesystem I/O."""

    def __init__(self, maxsize: int) -> None:
        self._queue: queue.Queue[tuple[Any, tuple[Any, ...]] | None] = queue.Queue(maxsize)
        self._thread = threading.Thread(target=self._run, name="disk-writer", daemon=True)
        self.dropped_records = 0
        self.last_error = ""
        self._thread.start()

    def submit(self, function: Any, *args: Any, droppable: bool = False) -> bool:
        item = (function, args)
        try:
            self._queue.put_nowait(item)
            return True
        except queue.Full:
            # Never block the event loop, even for opportunity records.
            self.dropped_records += 1
            return False

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is None:
                    return
                function, args = item
                function(*args)
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"
            finally:
                self._queue.task_done()

    def close(self) -> None:
        # Shutdown is bounded even when a filesystem call stalls.
        try:
            self._queue.put(None, timeout=1)
        except queue.Full:
            self.last_error = "写盘队列未排空，退出时放弃等待"
            return
        self._thread.join(timeout=5)


class LatestScreenWriter:
    """Capacity-one terminal slot: a slow console drops old frames, never market data."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._latest: Any = None
        self._closed = False
        self.last_error = ""
        self._thread = threading.Thread(target=self._run, name="terminal-writer", daemon=True)
        self._thread.start()

    def publish(self, screen: Any) -> None:
        with self._condition:
            self._latest = screen
            self._condition.notify()

    def _run(self) -> None:
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._latest is not None or self._closed)
                if self._closed and self._latest is None:
                    return
                screen, self._latest = self._latest, None
            try:
                if callable(screen):
                    screen = screen()
                print("\x1b[2J\x1b[H" + (screen or ""), end="", flush=True)
            except Exception as exc:
                self.last_error = f"{type(exc).__name__}: {exc}"

    def close(self) -> None:
        with self._condition:
            self._closed = True
            self._condition.notify()
        self._thread.join(timeout=5)


def _percentile_range(values: Any, low: float, high: float) -> tuple[float, float]:
    result = np.quantile(values, [low, high], method="linear")
    return float(result[0]), float(result[1])


class QuantileWorker:
    """At most one quantile job may exist; obsolete calculations never queue up."""

    def __init__(self) -> None:
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="quantiles")
        self._task: asyncio.Task[None] | None = None

    def collect(self) -> None:
        if self._task is None or not self._task.done():
            return
        try:
            self._task.result()
        finally:
            self._task = None

    def schedule(self, jobs: list[tuple[PairStatistics, StatisticsConfig]]) -> bool:
        self.collect()
        if self._task is not None:
            return False
        self._task = asyncio.create_task(self._calculate(jobs))
        return True

    async def _calculate(self, jobs: list[tuple[PairStatistics, StatisticsConfig]]) -> None:
        loop = asyncio.get_running_loop()
        for stats, config in jobs:
            # Only copy the needed tail, one pair at a time. Never pass a live deque
            # to a worker thread or hold an iterator over it across an await.
            count = min(config.long_window_samples, len(stats.basis_samples))
            if not count:
                continue
            values = np.fromiter(
                islice(reversed(stats.basis_samples), count), dtype=np.float64, count=count
            )
            await asyncio.sleep(0)
            cache = {}
            for sample_size in (config.short_window_samples, config.long_window_samples):
                count = min(sample_size, len(values))
                key = (sample_size, config.quantile_low, config.quantile_high)
                cache[key] = await loop.run_in_executor(
                    self._executor, _percentile_range,
                    values[:count], config.quantile_low, config.quantile_high,
                )
            stats.quantile_cache = cache

    async def close(self, cancel: bool = False) -> None:
        if self._task is not None:
            if cancel:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            else:
                await self._task
        await asyncio.to_thread(self._executor.shutdown, wait=True, cancel_futures=True)


# 分析
class StatisticsEngine:
    def __init__(self, restored: dict[str, PairStatistics] | None = None) -> None:
        self.pairs = restored or {}

    def update(
        self,
        snapshots: list[PairSnapshot],
        sample_interval_seconds: float,
        config: StatisticsConfig,
        now_wall: float | None = None,
    ) -> list[tuple[str, dict[str, Any]]]:
        now_wall = time.time() if now_wall is None else now_wall
        opportunities: list[tuple[str, dict[str, Any]]] = []
        for snapshot in snapshots:
            stats = self.pairs.setdefault(snapshot.symbol, PairStatistics(snapshot.symbol))
            for opportunity in stats.update(snapshot, now_wall, sample_interval_seconds, config):
                opportunities.append((snapshot.symbol, opportunity))
        return opportunities

    def resize_windows(self, total_window_samples: int) -> None:
        for stats in self.pairs.values():
            stats.resize_window(total_window_samples)

    # 筛选交易对
    def eligible_rows(self, positive_basis_only: bool) -> list[PairStatistics]:
        return [
            stats for stats in self.pairs.values()
            if stats.sample_count >= 2
            and (not positive_basis_only or stats.mean_basis_bps > 0)
        ]
    # 排序
    def selected_rows(
        self,
        positive_basis_only: bool,
        config: StatisticsConfig,
    ) -> list[PairStatistics]:
        return sorted(
            (
                stats for stats in self.eligible_rows(positive_basis_only)
                if config.sigma_multiplier * stats.standard_deviation_bps
                > config.min_k_sigma_bps
            ),
            key=lambda stats: (
                stats.effective_opportunity_count,
                stats.open_count + stats.close_count,
                stats.sigma_position,
                stats.standard_deviation_bps,
            ),
            reverse=True,
        )


async def consume_tickers(url: str, market: str, state: MarketState, stop: asyncio.Event) -> None:
    delay = 1.0
    error_key = f"{market}_ticker"
    while not stop.is_set():
        try:
            async with websockets.connect(
                url, **BINANCE_WEBSOCKET_OPTIONS,
            ) as websocket:
                state.ticker_connected[market] = True
                state.errors[error_key] = ""
                delay = 1.0
                while not stop.is_set():
                    payload = json.loads(await asyncio.wait_for(websocket.recv(), timeout=45))
                    if isinstance(payload, list):
                        state.update_tickers(market, payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.ticker_connected[market] = False
            state.errors[error_key] = f"{type(exc).__name__}: {exc}"
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 30)
    state.ticker_connected[market] = False


@dataclass(frozen=True, slots=True)
class RawMessage:
    received_at: float
    received_wall_ms: float
    payload: str | bytes


class RawMessageBuffer:
    """Event-loop-owned bounded FIFO. Overflow discards the oldest raw message."""

    def __init__(self, capacity: int) -> None:
        self.messages: deque[RawMessage] = deque(maxlen=capacity)
        self.ready = asyncio.Event()
        self.received = self.dropped = self.expired = self.decoded = 0

    def append(self, message: RawMessage) -> None:
        self.received += 1
        if len(self.messages) == self.messages.maxlen:
            self.dropped += 1
        self.messages.append(message)
        self.ready.set()


class LatestBBOConnection(ClientConnection):
    """BBO-only adapter: bound complete messages before the library's FIFO.

    Control frames and protocol validation remain owned by websockets. Never
    drop fragments: reassemble a complete message before applying overflow.
    This process_event integration is tested against websockets 15.x.
    """

    def __init__(self, *args: Any, raw_capacity: int = 512, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.raw_buffer = RawMessageBuffer(raw_capacity)
        self.fragments: list[bytes] = []
        self.last_message_at = time.monotonic()

    def process_event(self, event: Any) -> None:
        if self.response is not None and isinstance(event, Frame) and event.opcode in (
            OP_TEXT, OP_BINARY, OP_CONT,
        ):
            self.fragments.append(bytes(event.data))
            if event.fin:
                payload = b"".join(self.fragments)
                self.fragments.clear()
                self.last_message_at = time.monotonic()
                self.raw_buffer.append(RawMessage(
                    self.last_message_at, time.time() * 1000, payload,
                ))
            return
        super().process_event(event)

    async def watch_connection(self) -> None:
        while not self.connection_lost_waiter.done():
            try:
                await asyncio.wait_for(asyncio.shield(self.connection_lost_waiter), 1)
            except asyncio.TimeoutError:
                if time.monotonic() - self.last_message_at > 45:
                    raise TimeoutError("BBO 45秒未收到完整消息")
        raise self.protocol.close_exc


async def receive_raw_books(websocket: Any, buffer: RawMessageBuffer, stop: asyncio.Event) -> None:
    count = 0
    while not stop.is_set():
        async with asyncio.timeout(45):
            payload = await websocket.recv()
        buffer.append(RawMessage(time.monotonic(), time.time() * 1000, payload))
        count += 1
        # recv may complete immediately while the library has buffered frames.
        if count % 64 == 0:
            await asyncio.sleep(0)


async def decode_raw_books(
    market: str, state: MarketState, buffer: RawMessageBuffer,
    stop: asyncio.Event, runtime_manager: ConfigManager[RuntimeConfig] | None = None,
) -> None:
    while not stop.is_set():
        await buffer.ready.wait()
        runtime = runtime_manager.current if runtime_manager is not None else RuntimeConfig()
        batch_started = time.monotonic()
        for _ in range(runtime.decode_batch_size):
            if not buffer.messages:
                buffer.ready.clear()
                break
            message = buffer.messages.popleft()
            # Discard expired raw messages before spending CPU on JSON parsing.
            if (time.monotonic() - message.received_at) * 1000 > runtime.quote_max_age_ms:
                buffer.expired += 1
            else:
                payload = json.loads(message.payload)
                row = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(row, dict) and "b" in row and "a" in row:
                    lag = None
                    if market == "futures":
                        event_time = row.get("E")
                        if isinstance(event_time, (int, float)) and not isinstance(event_time, bool):
                            lag = max(0.0, message.received_wall_ms - event_time)
                            if lag > state.max_transport_lag_ms:
                                raise TransportLagError(
                                    f"永续事件延迟 {lag:.0f}ms，上限 {state.max_transport_lag_ms:.0f}ms"
                                )
                    state.update_book(market, row, message.received_at, lag)
                    buffer.decoded += 1
            # Bound parsing work per turn as well as per batch.
            if time.monotonic() - batch_started >= 0.002:
                break
        await asyncio.sleep(0)


async def consume_book_chunk(
    url: str,
    market: str,
    connection_key: str,
    state: MarketState,
    stop: asyncio.Event,
    runtime_manager: ConfigManager[RuntimeConfig] | None = None,
) -> None:
    delay = 1.0
    error_key = f"{market}_book_{connection_key}"
    while not stop.is_set():
        try:
            runtime = runtime_manager.current if runtime_manager is not None else RuntimeConfig()
            async with websockets.connect(
                url, **BINANCE_WEBSOCKET_OPTIONS,
                create_connection=partial(LatestBBOConnection, raw_capacity=runtime.raw_message_capacity),
            ) as websocket:
                direct = isinstance(websocket, LatestBBOConnection)
                buffer = websocket.raw_buffer if direct else RawMessageBuffer(runtime.raw_message_capacity)
                state.raw_buffers[error_key] = buffer
                state.set_book_chunk_connected(market, connection_key, True)
                state.errors[error_key] = ""
                connected_at = time.monotonic()
                workers = [
                    asyncio.create_task(websocket.watch_connection() if direct else
                                        receive_raw_books(websocket, buffer, stop)),
                    asyncio.create_task(decode_raw_books(market, state, buffer, stop, runtime_manager)),
                    asyncio.create_task(stop.wait()),
                ]
                try:
                    done, _ = await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)
                    for worker in done:
                        worker.result()
                finally:
                    for worker in workers:
                        worker.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                    state.set_book_chunk_connected(market, connection_key, False)
                    if time.monotonic() - connected_at >= 30:
                        delay = 1.0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.set_book_chunk_connected(market, connection_key, False)
            state.errors[error_key] = f"{type(exc).__name__}: {exc}"
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 30)
        finally:
            state.set_book_chunk_connected(market, connection_key, False)


async def consume_books(
    market: str,
    state: MarketState,
    filter_manager: ConfigManager[FilterConfig],
    runtime_manager: ConfigManager[RuntimeConfig],
    stop: asyncio.Event,
) -> None:
    base_url = SPOT_BOOK_BASE if market == "spot" else FUTURES_BOOK_BASE
    while not stop.is_set():
        symbols = sorted(state.book_symbols(market, filter_manager.current))
        if not symbols:
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
            continue
        routes_version = state.routes_version
        chunk_size = runtime_manager.current.book_symbols_per_connection
        capacity = runtime_manager.current.raw_message_capacity
        chunks = [symbols[index:index + chunk_size] for index in range(0, len(symbols), chunk_size)]
        state.errors[f"{market}_book"] = ""
        for key in list(state.errors):
            if key.startswith(f"{market}_book_"):
                state.errors.pop(key, None)
                state.raw_buffers.pop(key, None)
        tasks = [
            asyncio.create_task(consume_book_chunk(
                base_url + "/".join(
                    [f"{symbol.lower()}@bookTicker" for symbol in chunk]
                ),
                market, str(index), state, stop, runtime_manager,
            ))
            for index, chunk in enumerate(chunks, 1)
        ]
        try:
            while not stop.is_set():
                done, _ = await asyncio.wait(tasks, timeout=2)
                if done:
                    for task in done:
                        exception = task.exception()
                        if exception is not None:
                            raise exception
                    raise RuntimeError("bookTicker 子连接意外停止")
                if (
                    state.routes_version != routes_version
                    or state.book_symbols(market, filter_manager.current) != set(symbols)
                    or runtime_manager.current.book_symbols_per_connection != chunk_size
                    or runtime_manager.current.raw_message_capacity != capacity
                ):
                    break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.errors[f"{market}_book"] = f"{type(exc).__name__}: {exc}"
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}秒"
    if seconds < 3600:
        return f"{seconds / 60:.1f}分"
    return f"{seconds / 3600:.1f}小时"


def format_time_ago(timestamp: float | None, now_wall: float) -> str:
    if timestamp is None:
        return "--"
    elapsed = max(0.0, now_wall - timestamp)
    return "当前" if elapsed < 2 else f"{format_duration(elapsed)}前"


def format_quote_volume(value: float) -> str:
    """Compact a 24-hour quote-asset turnover for terminal display."""
    for divisor, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if abs(value) >= divisor:
            return f"{value / divisor:.2f}{suffix}"
    return f"{value:.2f}"


def terminal_width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1 for char in text)


def pad_terminal(text: str, width: int, align: str = "right") -> str:
    spaces = max(0, width - terminal_width(text))
    if align == "left":
        return text + " " * spaces
    if align == "center":
        left = spaces // 2
        return " " * left + text + " " * (spaces - left)
    return " " * spaces + text


def table_row(values: list[str], widths: list[int], aligns: list[str]) -> str:
    return "  ".join(
        pad_terminal(value, width, align)
        for value, width, align in zip(values, widths, aligns)
    )


def pipeline_status(state: MarketState) -> str:
    buffers = list(state.raw_buffers.values())
    return (
        f"BBO解帧缓存(本轮连接): {sum(len(b.messages) for b in buffers)} / "
        f"{sum(b.messages.maxlen or 0 for b in buffers)} | "
        f"溢出丢弃: {sum(b.dropped for b in buffers)} | "
        f"过期丢弃: {sum(b.expired for b in buffers)} | "
        f"主循环延迟: {state.loop_lag_ms:.1f}ms / 峰值 {state.max_loop_lag_ms:.1f}ms | "
        f"网络循环延迟: {getattr(state, 'network_loop_lag_ms', 0):.1f}ms / "
        f"峰值 {getattr(state, 'network_max_loop_lag_ms', 0):.1f}ms"
    )


async def monitor_loop_lag(state: MarketState, stop: asyncio.Event) -> None:
    while not stop.is_set():
        expected = time.monotonic() + 0.1
        await asyncio.sleep(0.1)
        state.loop_lag_ms = max(0.0, (time.monotonic() - expected) * 1000)
        state.max_loop_lag_ms = max(state.max_loop_lag_ms, state.loop_lag_ms)


# 显示
def render(
    state: MarketState,
    engine: StatisticsEngine,
    filters: FilterConfig,
    stats_config: StatisticsConfig,
    runtime: RuntimeConfig,
    started_mono: float,
    config_errors: list[str],
) -> str:
    rows = engine.selected_rows(filters.positive_basis_only, stats_config)
    rows = [
        stats for stats in rows
        if stats.symbol in state.eligible_symbols(filters)
        and (age := state.quote_age(stats.symbol, filters)) is not None
        and age <= runtime.stale_seconds
    ]
    lines = [
        "Binance 现货－USDT/USDC-M 永续基差波动筛选  [按 K 切换到永续－永续]",
        f"成交额流: 现货 {'在线' if state.ticker_connected['spot'] else '重连中'} / "
        f"永续 {'在线' if state.ticker_connected['futures'] else '重连中'} | "
        f"盘口流: 现货 {'在线' if state.book_connected['spot'] else '重连中'} / "
        f"永续 {'在线' if state.book_connected['futures'] else '重连中'}",
        f"报价币种: {','.join(sorted(state.quote_assets))} | 配对路线: {len(state.common_symbols(filters))} | "
        f"成交额达标: {len(state.liquid_symbols(filters))} | "
        f"价格达标: {len(state.eligible_symbols(filters))} | "
        f"入选统计: {len(rows)} | 正基差筛选: {'开启' if filters.positive_basis_only else '关闭'} | "
        f"采样周期: {runtime.sample_interval_ms}ms | 本次运行: {format_duration(time.monotonic() - started_mono)}",
        f"报价匹配: 最大年龄 {runtime.quote_max_age_ms}ms / 最大时差 {runtime.quote_match_tolerance_ms}ms / "
        f"永续E延迟上限 {runtime.max_transport_lag_ms}ms（现货仅检查接收时间）",
        pipeline_status(state),
        f"样本窗口: 总{stats_config.total_window_samples} / 长{stats_config.long_window_samples} / "
        f"短{stats_config.short_window_samples} | "
        f"初筛: {stats_config.sigma_multiplier:g}σ > {stats_config.min_k_sigma_bps:g}bp",
        f"开仓机会: 上穿 μ+{stats_config.open_threshold_bps:g}bp 后回落至 μ | "
        f"平仓机会: 下穿 μ{stats_config.close_threshold_bps:+g}bp 后回升至 μ",
        f"市场准入: 现货中间价 > {filters.min_spot_price:g} USDT/USDC",
    ]
    for message in config_errors:
        lines.append(f"配置错误: {message}")
    for key, message in state.errors.items():
        if message:
            lines.append(f"{key} 错误: {message}")
    quantile_name = (
        f"P{stats_config.quantile_low * 100:g}～P{stats_config.quantile_high * 100:g}"
    )
    widths = [3, 28, 13, 13, 16, 10, 21, 21, 21, 21, 11, 10, 10, 10, 10]
    aligns = [
        "right", "left", "right", "right", "right", "right",
        "right", "right", "right", "right", "right", "right",
        "right", "right", "right",
    ]
    separator = "-" * (sum(widths) + 2 * (len(widths) - 1))
    lines += [
        "",
        "【基差机会统计】",
        separator,
        table_row(
            ["#", "现货/永续", "现货成交额", "永续成交额", "现货价格", "μ", f"μ±{stats_config.sigma_multiplier:.0f}σ", f"{quantile_name}短窗", f"{quantile_name}长窗", "运行范围", "当前价差", "当前位置", "行情年龄", "开仓机会", "平仓机会"],
            widths, aligns,
        ),
        separator,
    ]
    if rows:
        for index, stats in enumerate(rows[: filters.top], 1):
            sigma = stats.standard_deviation_bps
            upper_sigma = stats.mean_basis_bps + stats_config.sigma_multiplier * sigma
            lower_sigma = stats.mean_basis_bps - stats_config.sigma_multiplier * sigma
            short_quantiles = stats.cached_quantile_range(
                stats_config.short_window_samples,
                stats_config.quantile_low,
                stats_config.quantile_high,
            )
            long_quantiles = stats.cached_quantile_range(
                stats_config.long_window_samples,
                stats_config.quantile_low,
                stats_config.quantile_high,
            )
            runtime_range = stats.runtime_range
            short_text = (
                f"{short_quantiles[0]:+.2f}～{short_quantiles[1]:+.2f}bp"
                if short_quantiles else "--"
            )
            long_text = (
                f"{long_quantiles[0]:+.2f}～{long_quantiles[1]:+.2f}bp"
                if long_quantiles else "--"
            )
            runtime_text = (
                f"{runtime_range[0]:+.2f}～{runtime_range[1]:+.2f}bp" if runtime_range else "--"
            )
            quote_age = state.quote_age(stats.symbol, filters)
            spot_volume, futures_volume = state.pair_volumes(stats.symbol, filters)
            lines.append(table_row([
                str(index), state.pair_label(stats.symbol, filters),
                format_quote_volume(spot_volume),
                format_quote_volume(futures_volume),
                f"{stats.current_spot_mid:.8g}",
                f"{stats.mean_basis_bps:+.2f}bp",
                f"{lower_sigma:+.2f}～{upper_sigma:+.2f}bp", short_text, long_text, runtime_text,
                f"{stats.current_basis_bps:+.2f}bp",
                f"{stats.sigma_position:+.2f}σ", f"{quote_age:.1f}秒",
                f"{stats.open_count}次", f"{stats.close_count}次",
            ], widths, aligns))
    else:
        lines.append(
            f"暂无交易对满足初筛：{stats_config.sigma_multiplier:g}σ > "
            f"{stats_config.min_k_sigma_bps:g}bp。"
        )
    lines += [
        separator,
        "基差 = (折算后永续中间价 / 现货中间价 - 1) × 10000；跨报价币种使用USDCUSDT现货中间价折算。",
    ]
    return "\n".join(lines)


def render_futures_futures(
    state: MarketState,
    engine: StatisticsEngine,
    filters: FilterConfig,
    stats_config: StatisticsConfig,
    runtime: RuntimeConfig,
    started_mono: float,
    config_errors: list[str],
) -> str:
    routes = state.liquid_futures_futures_routes(filters)
    rows = engine.selected_rows(False, stats_config)
    rows = [
        stats for stats in rows
        if stats.symbol in routes
        and (age := state.futures_futures_quote_age(stats.symbol, filters)) is not None
        and age <= runtime.stale_seconds
    ]
    lines = [
        "Binance USDT永续－USDC永续价差波动筛选  [按 K 切换到现货－永续]",
        f"成交额流: 永续 {'在线' if state.ticker_connected['futures'] else '重连中'} | "
        f"盘口流: 永续 {'在线' if state.book_connected['futures'] else '重连中'} / "
        f"USDCUSDT现货 {'在线' if state.book_connected['spot'] else '重连中'}",
        f"共同永续路线: {len(state.futures_futures_routes(filters))} | "
        f"成交额达标: {len(routes)} | 入选统计: {len(rows)} | "
        f"采样周期: {runtime.sample_interval_ms}ms | "
        f"本次运行: {format_duration(time.monotonic() - started_mono)}",
        f"报价匹配: 最大年龄 {runtime.quote_max_age_ms}ms / 最大时差 {runtime.quote_match_tolerance_ms}ms / "
        f"永续E延迟上限 {runtime.max_transport_lag_ms}ms（现货仅检查接收时间）",
        pipeline_status(state),
        f"样本窗口: 总{stats_config.total_window_samples} / 长{stats_config.long_window_samples} / "
        f"短{stats_config.short_window_samples} | "
        f"初筛: {stats_config.sigma_multiplier:g}σ > {stats_config.min_k_sigma_bps:g}bp",
        f"开仓机会: 上穿 μ+{stats_config.open_threshold_bps:g}bp 后回落至 μ | "
        f"平仓机会: 下穿 μ{stats_config.close_threshold_bps:+g}bp 后回升至 μ",
    ]
    for message in config_errors:
        lines.append(f"配置错误: {message}")
    for key, message in state.errors.items():
        if message:
            lines.append(f"{key} 错误: {message}")
    quantile_name = (
        f"P{stats_config.quantile_low * 100:g}～P{stats_config.quantile_high * 100:g}"
    )
    widths = [3, 28, 13, 13, 16, 16, 10, 21, 21, 21, 21, 11, 10, 10, 10, 10]
    aligns = ["right", "left"] + ["right"] * 14
    separator = "-" * (sum(widths) + 2 * (len(widths) - 1))
    lines += [
        "",
        "【USDT永续－USDC永续价差统计】",
        separator,
        table_row(
            ["#", "USDT永续/USDC永续", "USDT成交额", "USDC成交额", "USDT永续价",
             "USDC折算价", "μ", f"μ±{stats_config.sigma_multiplier:.0f}σ", f"{quantile_name}短窗", f"{quantile_name}长窗",
             "运行范围", "当前价差", "当前位置", "行情年龄", "开仓机会", "平仓机会"],
            widths, aligns,
        ),
        separator,
    ]
    if rows:
        for index, stats in enumerate(rows[: filters.top], 1):
            sigma = stats.standard_deviation_bps
            short_quantiles = stats.cached_quantile_range(
                stats_config.short_window_samples,
                stats_config.quantile_low,
                stats_config.quantile_high,
            )
            long_quantiles = stats.cached_quantile_range(
                stats_config.long_window_samples,
                stats_config.quantile_low,
                stats_config.quantile_high,
            )
            runtime_range = stats.runtime_range
            short_text = (
                f"{short_quantiles[0]:+.2f}～{short_quantiles[1]:+.2f}bp"
                if short_quantiles else "--"
            )
            long_text = (
                f"{long_quantiles[0]:+.2f}～{long_quantiles[1]:+.2f}bp"
                if long_quantiles else "--"
            )
            runtime_text = (
                f"{runtime_range[0]:+.2f}～{runtime_range[1]:+.2f}bp"
                if runtime_range else "--"
            )
            usdt_volume, usdc_volume = state.futures_futures_volumes(
                stats.symbol, filters
            )
            quote_age = state.futures_futures_quote_age(stats.symbol, filters)
            lines.append(table_row([
                str(index), state.futures_futures_label(stats.symbol, filters),
                format_quote_volume(usdt_volume), format_quote_volume(usdc_volume),
                f"{stats.current_spot_mid:.8g}", f"{stats.current_futures_mid:.8g}",
                f"{stats.mean_basis_bps:+.2f}bp",
                f"{stats.mean_basis_bps - stats_config.sigma_multiplier * sigma:+.2f}～{stats.mean_basis_bps + stats_config.sigma_multiplier * sigma:+.2f}bp",
                short_text, long_text, runtime_text,
                f"{stats.current_basis_bps:+.2f}bp", f"{stats.sigma_position:+.2f}σ",
                f"{quote_age:.1f}秒", f"{stats.open_count}次", f"{stats.close_count}次",
            ], widths, aligns))
    else:
        lines.append(
            f"暂无永续路线满足初筛：{stats_config.sigma_multiplier:g}σ > "
            f"{stats_config.min_k_sigma_bps:g}bp。"
        )
    lines += [
        separator,
        "价差 = (USDC永续折算中间价 / USDT永续中间价 - 1) × 10000；折算使用USDCUSDT现货中间价。",
    ]
    return "\n".join(lines)


def poll_page_toggle(current_page: str) -> str:
    if os.name != "nt":
        return current_page
    import msvcrt

    while msvcrt.kbhit():
        if msvcrt.getwch().lower() == "k":
            current_page = (
                "futures_futures"
                if current_page == "spot_futures" else "spot_futures"
            )
    return current_page


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else BASE_DIR / path


def reload_configs(managers: tuple[Any, ...]) -> None:
    for manager in managers:
        manager.reload()


async def resize_statistics_windows(engine: StatisticsEngine, size: int) -> None:
    for stats in engine.pairs.values():
        while len(stats.basis_samples) > size:
            stats.resize_window(max(size, len(stats.basis_samples) - 2048))
            await asyncio.sleep(0)


class MarketNetworkWorker:
    """Own all sockets on a dedicated loop; publish latest state, never a backlog.

    Config objects are immutable and replaced atomically by ConfigManager.
    Mutable market dictionaries never cross thread boundaries without copying.
    """

    def __init__(self, filters: Any, runtime: Any) -> None:
        self.filters, self.runtime = filters, runtime
        self._lock = threading.Lock()
        self._snapshot: dict[str, Any] = {}
        self._exit = threading.Event()
        self.error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="market-network", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        try:
            asyncio.run(self._serve())
        except BaseException as exc:
            self.error = exc

    async def _serve(self) -> None:
        state = MarketState()
        stop = asyncio.Event()
        state.set_pairing_config(self.filters.current)
        tasks = [
            asyncio.create_task(monitor_loop_lag(state, stop)),
            asyncio.create_task(consume_tickers(SPOT_TICKER_STREAM, "spot", state, stop)),
            asyncio.create_task(consume_tickers(FUTURES_TICKER_STREAM, "futures", state, stop)),
            asyncio.create_task(consume_books("spot", state, self.filters, self.runtime, stop)),
            asyncio.create_task(consume_books("futures", state, self.filters, self.runtime, stop)),
        ]
        try:
            while not self._exit.is_set():
                for task in tasks:
                    if task.done():
                        task.result()
                        raise RuntimeError("网络任务意外退出")
                state.set_pairing_config(self.filters.current)
                state.max_transport_lag_ms = self.runtime.current.max_transport_lag_ms
                snapshot = {name: getattr(state, name).copy() for name in (
                    "spot_volumes", "futures_volumes", "spot_books", "futures_books",
                    "ticker_connected", "book_connected", "ticker_last_update",
                    "book_last_update", "errors",
                )}
                buffers = {}
                for key, buffer in state.raw_buffers.items():
                    view = copy.copy(buffer)
                    # Only occupancy/counters are exposed to the UI, not raw messages.
                    view.messages = deque([None] * len(buffer.messages), maxlen=buffer.messages.maxlen)
                    buffers[key] = view
                snapshot.update(raw_buffers=buffers, network_loop_lag_ms=state.loop_lag_ms,
                                network_max_loop_lag_ms=state.max_loop_lag_ms)
                with self._lock:
                    self._snapshot = snapshot
                await asyncio.sleep(0.05)
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def apply_latest(self, state: MarketState) -> None:
        if self.error is not None:
            raise RuntimeError("网络线程退出") from self.error
        with self._lock:
            snapshot = self._snapshot
        for name, value in snapshot.items():
            if name == "errors":
                background = state.errors.get("background")
                state.errors = value.copy()
                if background:
                    state.errors["background"] = background
            else:
                setattr(state, name, value.copy() if isinstance(value, dict) else value)

    def close(self) -> None:
        self._exit.set()
        self._thread.join(timeout=8)
        if self._thread.is_alive():
            raise RuntimeError("网络线程未在8秒内退出")


def make_screen_job(
    renderer: Any, state: MarketState, engine: StatisticsEngine,
    filters: FilterConfig, config: StatisticsConfig, runtime: RuntimeConfig,
    started: float, errors: list[str],
) -> Any:
    # Rendering must not iterate over dictionaries/deques being mutated by receivers.
    view = copy.copy(state)
    for key, value in vars(state).items():
        if isinstance(value, dict):
            setattr(view, key, value.copy())
    rows = {}
    for symbol, stats in engine.pairs.items():
        row = copy.copy(stats)
        row.basis_samples = deque()
        row.minimum_samples = deque(islice(stats.minimum_samples, 1))
        row.maximum_samples = deque(islice(stats.maximum_samples, 1))
        rows[symbol] = row
    snapshot_engine = StatisticsEngine(rows)
    return lambda: renderer(view, snapshot_engine, filters, config, runtime, started, errors)


async def run() -> None:
    filter_manager = ConfigManager(FILTER_CONFIG_PATH, FilterConfig)
    statistics_manager = ConfigManager(STATISTICS_CONFIG_PATH, StatisticsConfig)
    futures_futures_statistics_manager = ConfigManager(
        FUTURES_FUTURES_STATISTICS_CONFIG_PATH, StatisticsConfig
    )
    runtime_manager = ConfigManager(RUNTIME_CONFIG_PATH, RuntimeConfig)
    data_root = resolve_path(runtime_manager.current.data_directory)
    store = PairDirectoryStore(data_root / "spot_futures")
    futures_futures_store = PairDirectoryStore(data_root / "futures_futures")
    diagnostic_store = BasisDiagnosticStore(data_root / "diagnostics")
    disk_writer = BackgroundWriter(runtime_manager.current.background_write_queue_size)
    screen_writer = LatestScreenWriter()
    quantile_worker = QuantileWorker()
    engine = StatisticsEngine(store.load())
    futures_futures_engine = StatisticsEngine(futures_futures_store.load())
    state = MarketState()
    stop = asyncio.Event()
    state.set_pairing_config(filter_manager.current)
    # 记录启动时间，time.monotonic()单增计数1，2，3...
    started_mono = time.monotonic()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    network_worker = MarketNetworkWorker(filter_manager, runtime_manager)
    network_worker.start()
    tasks = [
        asyncio.create_task(monitor_loop_lag(state, stop)),
    ]
    last_display = last_persist = last_quantile = 0.0
    next_sample = time.monotonic()
    current_page = "spot_futures"
    try:
        while not stop.is_set():
            for task in tasks:
                if task.done():
                    exception = task.exception()
                    if exception is not None:
                        raise RuntimeError("后台任务异常退出") from exception
                    raise RuntimeError("后台任务意外停止")
            await asyncio.to_thread(reload_configs, (
                filter_manager, statistics_manager,
                futures_futures_statistics_manager, runtime_manager,
            ))
            filters = filter_manager.current
            network_worker.apply_latest(state)
            state.set_pairing_config(filters)
            stats_config = statistics_manager.current
            futures_futures_stats_config = futures_futures_statistics_manager.current
            await resize_statistics_windows(engine, stats_config.total_window_samples)
            await resize_statistics_windows(futures_futures_engine,
                futures_futures_stats_config.total_window_samples
            )
            runtime = runtime_manager.current
            state.max_transport_lag_ms = runtime.max_transport_lag_ms
            sample_interval = runtime.sample_interval_ms / 1000
            quote_max_age = runtime.quote_max_age_ms / 1000
            quote_tolerance = runtime.quote_match_tolerance_ms / 1000
            now_mono = time.monotonic()
            now_wall = time.time()

            # ── 现货-永续 统一采样 ──
            snapshots = state.snapshots(
                filters, quote_max_age, quote_tolerance,
                runtime.stale_seconds,
            )
            diagnostic_rows = diagnostic_store.process(
                state.drain_quote_rejections(), snapshots, now_wall, write=False
            )
            if diagnostic_rows:
                disk_writer.submit(
                    diagnostic_store.append_rows, diagnostic_rows, droppable=True
                )
            opportunities = engine.update(snapshots, sample_interval, stats_config, now_wall)
            for symbol, opportunity in opportunities:
                stats = engine.pairs[symbol]
                if not filters.positive_basis_only or stats.mean_basis_bps > 0:
                    disk_writer.submit(
                        store.append_opportunity, symbol, dict(opportunity)
                    )

            # ── 现货-永续 BBO 记录（统一采样点生成，避免时间错配和队列丢弃偏差）──
            eligible = state.eligible_symbols(filters)
            bbo_records: list[tuple[str, dict[str, Any]]] = []
            for key, route in state.liquid_routes(filters).items():
                if key not in eligible:
                    continue
                bbo = state.get_bbo_snapshot(
                    route, quote_max_age, quote_tolerance
                )
                if bbo is None:
                    continue
                stats = engine.pairs.get(bbo.symbol)
                if stats is None or stats.sample_count < 1:
                    continue
                trigger_basis_bps = stats.mean_basis_bps + stats_config.open_threshold_bps
                if bbo.basis_bps <= trigger_basis_bps:
                    continue
                if not stats.allow_bbo_record(
                    bbo.timestamp, stats_config.bbo_record_interval_ms / 1000
                ):
                    continue
                bbo_records.append((bbo.symbol, {
                    "time": datetime.fromtimestamp(bbo.timestamp, timezone.utc).isoformat(),
                    "source_market": bbo.source_market,
                    "spot_symbol": bbo.spot_symbol,
                    "futures_symbol": bbo.futures_symbol,
                    "conversion_symbol": bbo.conversion_symbol or None,
                    "conversion_rate": bbo.conversion_rate,
                    "spot_bid": bbo.spot_bid,
                    "spot_ask": bbo.spot_ask,
                    "spot_mid": bbo.spot_mid,
                    "futures_bid": bbo.futures_bid,
                    "futures_ask": bbo.futures_ask,
                    "raw_futures_mid": bbo.raw_futures_mid,
                    "futures_mid": bbo.futures_mid,
                    "mean_basis_bps": stats.mean_basis_bps,
                    "standard_deviation_bps": stats.standard_deviation_bps,
                    "trigger_basis_bps": trigger_basis_bps,
                    "basis_bps": bbo.basis_bps,
                    "deviation_bps": bbo.basis_bps - stats.mean_basis_bps,
                }))
            if bbo_records:
                disk_writer.submit(
                    store.append_bbo_records, bbo_records, droppable=True
                )

            # ── 永续-永续 统一采样 ──
            futures_futures_snapshots = state.futures_futures_snapshots(
                filters, quote_max_age, quote_tolerance,
                runtime.stale_seconds,
            )
            diagnostic_rows = diagnostic_store.process(
                state.drain_quote_rejections(),
                futures_futures_snapshots, now_wall, write=False,
            )
            if diagnostic_rows:
                disk_writer.submit(
                    diagnostic_store.append_rows, diagnostic_rows, droppable=True
                )
            futures_futures_opportunities = futures_futures_engine.update(
                futures_futures_snapshots,
                sample_interval,
                futures_futures_stats_config,
                now_wall,
            )
            for symbol, opportunity in futures_futures_opportunities:
                disk_writer.submit(
                    futures_futures_store.append_opportunity,
                    symbol, dict(opportunity),
                )

            # ── 永续-永续 BBO 记录 ──
            ff_routes = state.liquid_futures_futures_routes(filters)
            ff_bbo_records: list[tuple[str, dict[str, Any]]] = []
            for key, route in ff_routes.items():
                usdt = state.futures_books.get(route.usdt_symbol)
                usdc = state.futures_books.get(route.usdc_symbol)
                conversion_book = state.spot_books.get(route.conversion_symbol)
                if not usdt or not usdc or not conversion_book or conversion_book.mid <= 0:
                    continue
                now_mono_chk = time.monotonic()
                valid, _, _, _ = state._quote_quality(
                    [usdt, usdc, conversion_book], now_mono_chk,
                    quote_max_age, quote_tolerance,
                )
                if not valid:
                    continue
                conversion_rate = (
                    1 / conversion_book.mid
                    if route.conversion_inverted else conversion_book.mid
                )
                normalized_usdc_mid = usdc.mid * conversion_rate
                basis_bps = (normalized_usdc_mid / usdt.mid - 1) * 10_000
                stats = futures_futures_engine.pairs.get(key)
                if stats is None or stats.sample_count < 1:
                    continue
                trigger_basis_bps = stats.mean_basis_bps + futures_futures_stats_config.open_threshold_bps
                if basis_bps <= trigger_basis_bps:
                    continue
                if not stats.allow_bbo_record(
                    now_wall, futures_futures_stats_config.bbo_record_interval_ms / 1000
                ):
                    continue
                ff_bbo_records.append((key, {
                    "time": datetime.fromtimestamp(now_wall, timezone.utc).isoformat(),
                    "source_market": "unified",
                    "usdt_symbol": route.usdt_symbol,
                    "usdc_symbol": route.usdc_symbol,
                    "conversion_symbol": route.conversion_symbol or None,
                    "conversion_rate": conversion_rate,
                    "usdt_bid": usdt.bid,
                    "usdt_ask": usdt.ask,
                    "usdt_mid": usdt.mid,
                    "usdc_bid": usdc.bid,
                    "usdc_ask": usdc.ask,
                    "usdc_mid": usdc.mid,
                    "normalized_usdc_mid": normalized_usdc_mid,
                    "mean_basis_bps": stats.mean_basis_bps,
                    "standard_deviation_bps": stats.standard_deviation_bps,
                    "trigger_basis_bps": trigger_basis_bps,
                    "basis_bps": basis_bps,
                    "deviation_bps": basis_bps - stats.mean_basis_bps,
                }))
            if ff_bbo_records:
                disk_writer.submit(
                    futures_futures_store.append_bbo_records,
                    ff_bbo_records, droppable=True,
                )

            if now_mono - last_persist >= runtime.persist_interval_seconds:
                disk_writer.submit(
                    store.save_state_rows,
                    store.snapshot_states(engine.eligible_rows(filters.positive_basis_only)),
                    droppable=True,
                )
                disk_writer.submit(
                    futures_futures_store.save_state_rows,
                    futures_futures_store.snapshot_states(
                        futures_futures_engine.eligible_rows(False)
                    ),
                    droppable=True,
                )
                last_persist = now_mono
            quantile_worker.collect()
            if now_mono - last_quantile >= runtime.quantile_refresh_seconds:
                jobs = [
                    (stats, stats_config) for stats in engine.pairs.values()
                ] + [
                    (stats, futures_futures_stats_config)
                    for stats in futures_futures_engine.pairs.values()
                ]
                if quantile_worker.schedule(jobs):
                    last_quantile = now_mono
            current_page = poll_page_toggle(current_page)
            if now_mono - last_display >= runtime.display_refresh_seconds:
                errors = [
                    manager.error
                    for manager in (
                        filter_manager,
                        statistics_manager,
                        futures_futures_statistics_manager,
                        runtime_manager,
                    )
                    if manager.error
                ]
                state.errors["background"] = " / ".join(filter(None, (
                    disk_writer.last_error, screen_writer.last_error,
                    f"写盘任务丢弃 {disk_writer.dropped_records}" if disk_writer.dropped_records else "",
                )))
                sf_page = current_page == "spot_futures"
                screen_writer.publish(make_screen_job(
                    render if sf_page else render_futures_futures,
                    state, engine if sf_page else futures_futures_engine,
                    filters, stats_config if sf_page else futures_futures_stats_config,
                    runtime, started_mono, errors,
                ))
                last_display = now_mono

            next_sample += sample_interval
            delay = max(0.0, next_sample - time.monotonic())
            if delay == 0:
                next_sample = time.monotonic()
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
    finally:
        stop.set()
        disk_writer.submit(
            store.save_state_rows,
            store.snapshot_states(
                engine.eligible_rows(filter_manager.current.positive_basis_only)
            ),
        )
        disk_writer.submit(
            futures_futures_store.save_state_rows,
            futures_futures_store.snapshot_states(
                futures_futures_engine.eligible_rows(False)
            ),
        )
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await quantile_worker.close(cancel=True)
        await asyncio.gather(
            asyncio.to_thread(disk_writer.close),
            asyncio.to_thread(screen_writer.close),
            asyncio.to_thread(network_worker.close),
        )

# python 程序入口标准写法
if __name__ == "__main__":
    if os.name == "nt":
        os.system("")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
