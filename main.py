from __future__ import annotations

import asyncio
import json
import os
import signal
import time
import unicodedata
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generic, TypeVar

import websockets

BASE_DIR = Path(__file__).resolve().parent
# 24hr ticker
SPOT_TICKER_STREAM = "wss://data-stream.binance.vision/ws/!miniTicker@arr"
FUTURES_TICKER_STREAM = "wss://fstream.binance.com/market/ws/!ticker@arr"
# 流
SPOT_BOOK_BASE = "wss://data-stream.binance.vision/stream?streams="
FUTURES_BOOK_BASE = "wss://fstream.binance.com/public/stream?streams="
FILTER_CONFIG_PATH = BASE_DIR / "filter_config.json"
STATISTICS_CONFIG_PATH = BASE_DIR / "statistics_config.json"
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
    persist_interval_seconds: float = 60.0
    data_directory: str = "data"
    bbo_queue_maxsize: int = 20_000

    def validate(self) -> None:
        if not isinstance(self.sample_interval_ms, int) or self.sample_interval_ms < 50:
            raise ValueError("sample_interval_ms 必须是至少 50 的整数")
        if self.display_refresh_seconds <= 0 or self.stale_seconds <= 0:
            raise ValueError("显示刷新和行情过期时间必须大于 0")
        if self.persist_interval_seconds <= 0 or not self.data_directory.strip():
            raise ValueError("持久化间隔必须大于 0，数据目录不能为空")
        if not isinstance(self.bbo_queue_maxsize, int) or self.bbo_queue_maxsize < 100:
            raise ValueError("bbo_queue_maxsize 必须是至少为 100 的整数")


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

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2


@dataclass(slots=True)
class PairSnapshot:
    symbol: str
    spot_mid: float
    futures_mid: float
    basis_bps: float
    spot_symbol: str = ""
    futures_symbol: str = ""
    conversion_rate: float = 1.0


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


# 显示状态栏
class MarketState:
    def __init__(self, bbo_queue_maxsize: int = 20_000) -> None:
        self.quote_assets: frozenset[str] = frozenset({"USDT"})
        self.cross_quote_pairings: tuple[tuple[str, str], ...] = ()
        self.spot_volumes: dict[str, VolumeTicker] = {}
        self.futures_volumes: dict[str, VolumeTicker] = {}
        self.spot_books: dict[str, BookQuote] = {}
        self.futures_books: dict[str, BookQuote] = {}
        self.active_routes: dict[str, PairRoute] = {}
        self.bbo_updates: asyncio.Queue[BboSnapshot] = asyncio.Queue(
            maxsize=bbo_queue_maxsize
        )
        self.bbo_dropped = 0
        self.ticker_connected = {"spot": False, "futures": False}
        self.book_connected = {"spot": False, "futures": False}
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

    def update_book(self, market: str, row: dict[str, Any]) -> None:
        try:
            symbol = str(row["s"])
            bid, ask = float(row["b"]), float(row["a"])
        except (KeyError, TypeError, ValueError):
            return
        if bid <= 0 or ask <= 0 or ask < bid:
            return
        now = time.monotonic()
        destination = self.spot_books if market == "spot" else self.futures_books
        destination[symbol] = BookQuote(bid, ask, now)
        self.book_last_update[market] = now
        routes = list(self.active_routes.values())
        if not routes:
            # 保留同名配对的直接用法，正式运行时订阅前会建立 active_routes。
            spot = self.spot_books.get(symbol)
            futures = self.futures_books.get(symbol)
            if spot and futures:
                routes = [PairRoute(symbol, symbol, symbol, symbol, "", "")]
        for route in routes:
            relevant = (
                (market == "spot" and symbol in {route.spot_symbol, route.conversion_symbol})
                or (market == "futures" and symbol == route.futures_symbol)
            )
            if not relevant:
                continue
            snapshot = self._bbo_snapshot(route, market)
            if snapshot is not None:
                self._enqueue_bbo(snapshot)

    def _enqueue_bbo(self, snapshot: BboSnapshot) -> None:
        if self.bbo_updates.full():
            try:
                self.bbo_updates.get_nowait()
                self.bbo_dropped += 1
            except asyncio.QueueEmpty:
                pass
        self.bbo_updates.put_nowait(snapshot)

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
        for symbol in self.spot_volumes:
            parsed = self._split_symbol(symbol)
            if parsed:
                spot_by_asset[parsed] = symbol
        for symbol in self.futures_volumes:
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
        self.quote_assets = frozenset(assets)
        self.cross_quote_pairings = cross

    def set_quote_assets(self, quote_assets: tuple[str, ...] | list[str]) -> None:
        self.quote_assets = frozenset(quote_assets)
        self.cross_quote_pairings = ()

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

    def book_symbols(self, market: str, filters: FilterConfig) -> set[str]:
        routes = self.liquid_routes(filters)
        self.active_routes = routes
        if market == "futures":
            return {route.futures_symbol for route in routes.values()}
        symbols = {route.spot_symbol for route in routes.values()}
        symbols.update(
            route.conversion_symbol for route in routes.values()
            if route.conversion_symbol
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

    def snapshots(self, filters: FilterConfig, stale_seconds: float) -> list[PairSnapshot]:
        if not self.streams_healthy(stale_seconds):
            return []
        now = time.monotonic()
        result: list[PairSnapshot] = []
        routes = self.liquid_routes(filters)
        for key in self.eligible_symbols(filters):
            route = routes[key]
            spot = self.spot_books.get(route.spot_symbol)
            futures = self.futures_books.get(route.futures_symbol)
            conversion = self._conversion_rate(route)
            if not spot or not futures or conversion is None:
                continue
            rate, conversion_received_at = conversion
            ages = [now - spot.received_at, now - futures.received_at]
            if route.conversion_symbol:
                ages.append(now - conversion_received_at)
            if max(ages) > stale_seconds:
                continue
            spot_mid, futures_mid = spot.mid, futures.mid * rate
            result.append(PairSnapshot(
                key, spot_mid, futures_mid,
                (futures_mid / spot_mid - 1) * 10_000,
                route.spot_symbol, route.futures_symbol, rate,
            ))
        return result

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
    current_basis_bps: float = 0.0
    current_deviation_bps: float = 0.0
    open_count: int = 0
    close_count: int = 0
    upper_excursion_active: bool = False
    lower_excursion_active: bool = False
    last_sample_at: float | None = None
    basis_samples: deque[float] = field(default_factory=deque, repr=False)
    last_bbo_record_at: float | None = None

    @property
    def standard_deviation_bps(self) -> float:
        if self.sample_count < 2:
            return 0.0
        variance = self.rolling_sum_squares / self.sample_count - self.mean_basis_bps ** 2
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
        values = sorted(list(self.basis_samples)[-count:])

        def percentile(q: float) -> float:
            position = q * (len(values) - 1)
            left = int(position)
            right = min(left + 1, len(values) - 1)
            weight = position - left
            return values[left] * (1 - weight) + values[right] * weight

        return percentile(low), percentile(high)

    def allow_bbo_record(self, timestamp: float, interval_seconds: float) -> bool:
        if self.last_bbo_record_at is not None and timestamp - self.last_bbo_record_at < interval_seconds:
            return False
        self.last_bbo_record_at = timestamp
        return True

    @property
    def runtime_range(self) -> tuple[float, float] | None:
        if not self.basis_samples:
            return None
        return min(self.basis_samples), max(self.basis_samples)

    def resize_window(self, total_window_samples: int) -> None:
        while len(self.basis_samples) > total_window_samples:
            removed = self.basis_samples.popleft()
            self.rolling_sum -= removed
            self.rolling_sum_squares -= removed * removed
        self.sample_count = len(self.basis_samples)
        self.mean_basis_bps = (
            self.rolling_sum / self.sample_count if self.sample_count else 0.0
        )

    def _append_sample(self, basis_bps: float, total_window_samples: int) -> None:
        self.basis_samples.append(basis_bps)
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
        self.current_spot_mid = snapshot.spot_mid
        self.current_basis_bps = current_basis_bps
        self.current_deviation_bps = (
            current_basis_bps - previous_mean if self.sample_count else 0.0
        )
        events: list[dict[str, Any]] = []
        if self.sample_count:
            if not self.upper_excursion_active and self.current_deviation_bps >= config.open_threshold_bps:
                self.upper_excursion_active = True
            elif self.upper_excursion_active and self.current_deviation_bps <= 0:
                self.open_count += 1
                self.upper_excursion_active = False
                events.append({
                    "event": "open_opportunity",
                    "time": datetime.fromtimestamp(now_wall, timezone.utc).isoformat(),
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
                    "time": datetime.fromtimestamp(now_wall, timezone.utc).isoformat(),
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

        self._append_sample(current_basis_bps, config.total_window_samples)
        self.observed_seconds += sample_interval_seconds
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
        for stats in statistics:
            pair_dir = self.root / stats.symbol
            pair_dir.mkdir(parents=True, exist_ok=True)
            row = {
                    "sample_count": stats.sample_count,
                    "observed_seconds": stats.observed_seconds,
                    "mean_basis_bps": stats.mean_basis_bps,
                    "standard_deviation_bps": stats.standard_deviation_bps,
                    "current_spot_mid": stats.current_spot_mid,
                    "current_basis_bps": stats.current_basis_bps,
                    "current_deviation_bps": stats.current_deviation_bps,
                    "open_count": stats.open_count,
                    "close_count": stats.close_count,
                    "upper_excursion_active": stats.upper_excursion_active,
                    "lower_excursion_active": stats.lower_excursion_active,
                    "last_sample_at": stats.last_sample_at,
            }
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
                url, proxy=None, open_timeout=20, ping_interval=20, ping_timeout=20,
                close_timeout=5, max_size=8_000_000,
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


async def consume_books(
    market: str,
    state: MarketState,
    filter_manager: ConfigManager[FilterConfig],
    stop: asyncio.Event,
) -> None:
    base_url = SPOT_BOOK_BASE if market == "spot" else FUTURES_BOOK_BASE
    error_key = f"{market}_book"
    delay = 1.0
    while not stop.is_set():
        symbols = state.book_symbols(market, filter_manager.current)
        if not symbols:
            try:
                await asyncio.wait_for(stop.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass
            continue
        streams = "/".join(f"{symbol.lower()}@bookTicker" for symbol in sorted(symbols))
        try:
            async with websockets.connect(
                base_url + streams, proxy=None, open_timeout=20, ping_interval=20,
                ping_timeout=20, close_timeout=5, max_size=8_000_000,
            ) as websocket:
                state.book_connected[market] = True
                state.errors[error_key] = ""
                delay = 1.0
                last_check = time.monotonic()
                while not stop.is_set():
                    payload = json.loads(await asyncio.wait_for(websocket.recv(), timeout=45))
                    row = payload.get("data") if isinstance(payload, dict) else None
                    if isinstance(row, dict):
                        state.update_book(market, row)
                    if time.monotonic() - last_check >= 10:
                        if state.book_symbols(market, filter_manager.current) != symbols:
                            break
                        last_check = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.errors[error_key] = f"{type(exc).__name__}: {exc}"
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 30)
        finally:
            state.book_connected[market] = False


async def process_bbo_updates(
    state: MarketState,
    engine: StatisticsEngine,
    store: PairDirectoryStore,
    filter_manager: ConfigManager[FilterConfig],
    statistics_manager: ConfigManager[StatisticsConfig],
    runtime_manager: ConfigManager[RuntimeConfig],
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        try:
            first = await asyncio.wait_for(state.bbo_updates.get(), timeout=1)
        except asyncio.TimeoutError:
            continue
        batch = [first]
        while len(batch) < 5000:
            try:
                batch.append(state.bbo_updates.get_nowait())
            except asyncio.QueueEmpty:
                break

        filters = filter_manager.current
        config = statistics_manager.current
        stale_seconds = runtime_manager.current.stale_seconds
        eligible = state.eligible_symbols(filters)
        now_mono = time.monotonic()
        records: list[tuple[str, dict[str, Any]]] = []
        for bbo in batch:
            if bbo.symbol not in eligible:
                continue
            if now_mono - bbo.spot_received_at > stale_seconds or now_mono - bbo.futures_received_at > stale_seconds:
                continue
            if (
                bbo.conversion_symbol
                and now_mono - bbo.conversion_received_at > stale_seconds
            ):
                continue
            stats = engine.pairs.setdefault(bbo.symbol, PairStatistics(bbo.symbol))
            basis_bps = bbo.basis_bps
            if stats.sample_count < 1:
                continue
            trigger_basis_bps = stats.mean_basis_bps + config.open_threshold_bps
            if basis_bps <= trigger_basis_bps:
                continue
            if not stats.allow_bbo_record(
                bbo.timestamp, config.bbo_record_interval_ms / 1000
            ):
                continue
            records.append((bbo.symbol, {
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
                "basis_bps": basis_bps,
                "deviation_bps": basis_bps - stats.mean_basis_bps,
            }))
        if records:
            store.append_bbo_records(records)


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
        "Binance 现货－USDT/USDC-M 永续基差波动筛选",
        f"成交额流: 现货 {'在线' if state.ticker_connected['spot'] else '重连中'} / "
        f"永续 {'在线' if state.ticker_connected['futures'] else '重连中'} | "
        f"盘口流: 现货 {'在线' if state.book_connected['spot'] else '重连中'} / "
        f"永续 {'在线' if state.book_connected['futures'] else '重连中'}",
        f"报价币种: {','.join(sorted(state.quote_assets))} | 配对路线: {len(state.common_symbols(filters))} | "
        f"成交额达标: {len(state.liquid_symbols(filters))} | "
        f"价格达标: {len(state.eligible_symbols(filters))} | "
        f"入选统计: {len(rows)} | 正基差筛选: {'开启' if filters.positive_basis_only else '关闭'} | "
        f"采样周期: {runtime.sample_interval_ms}ms | 本次运行: {format_duration(time.monotonic() - started_mono)}",
        f"样本窗口: 总{stats_config.total_window_samples} / 长{stats_config.long_window_samples} / "
        f"短{stats_config.short_window_samples} | "
        f"初筛: {stats_config.sigma_multiplier:g}σ > {stats_config.min_k_sigma_bps:g}bp",
        f"开仓机会: 上穿 μ+{stats_config.open_threshold_bps:g}bp 后回落至 μ | "
        f"平仓机会: 下穿 μ{stats_config.close_threshold_bps:+g}bp 后回升至 μ",
        f"BBO队列: {state.bbo_updates.qsize()}/{state.bbo_updates.maxsize} | "
        f"已丢弃旧快照: {state.bbo_dropped}",
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
    widths = [3, 21, 13, 13, 16, 10, 12, 21, 21, 21, 11, 10, 10, 10, 10]
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
            ["#", "现货/永续", "现货成交额", "永续成交额", "现货价格", "μ", "μ+kσ", f"{quantile_name}短窗", f"{quantile_name}长窗", "运行范围", "当前价差", "当前位置", "行情年龄", "开仓机会", "平仓机会"],
            widths, aligns,
        ),
        separator,
    ]
    if rows:
        for index, stats in enumerate(rows[: filters.top], 1):
            sigma = stats.standard_deviation_bps
            upper_sigma = stats.mean_basis_bps + stats_config.sigma_multiplier * sigma
            short_quantiles = stats.quantile_range(
                stats_config.short_window_samples,
                stats_config.quantile_low,
                stats_config.quantile_high,
            )
            long_quantiles = stats.quantile_range(
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
                f"{upper_sigma:+.2f}bp", short_text, long_text, runtime_text,
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


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    return path if path.is_absolute() else BASE_DIR / path


async def run() -> None:
    filter_manager = ConfigManager(FILTER_CONFIG_PATH, FilterConfig)
    statistics_manager = ConfigManager(STATISTICS_CONFIG_PATH, StatisticsConfig)
    runtime_manager = ConfigManager(RUNTIME_CONFIG_PATH, RuntimeConfig)
    store = PairDirectoryStore(resolve_path(runtime_manager.current.data_directory))
    engine = StatisticsEngine(store.load())
    state = MarketState(runtime_manager.current.bbo_queue_maxsize)
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
    tasks = [
        asyncio.create_task(consume_tickers(SPOT_TICKER_STREAM, "spot", state, stop)),
        asyncio.create_task(consume_tickers(FUTURES_TICKER_STREAM, "futures", state, stop)),
        asyncio.create_task(consume_books("spot", state, filter_manager, stop)),
        asyncio.create_task(consume_books("futures", state, filter_manager, stop)),
        asyncio.create_task(process_bbo_updates(
            state, engine, store, filter_manager, statistics_manager, runtime_manager, stop,
        )),
    ]
    last_display = last_persist = 0.0
    next_sample = time.monotonic()
    try:
        while not stop.is_set():
            for task in tasks:
                if task.done():
                    exception = task.exception()
                    if exception is not None:
                        raise RuntimeError("后台任务异常退出") from exception
                    raise RuntimeError("后台任务意外停止")
            filter_manager.reload()
            statistics_manager.reload()
            runtime_manager.reload()
            filters = filter_manager.current
            state.set_pairing_config(filters)
            stats_config = statistics_manager.current
            engine.resize_windows(stats_config.total_window_samples)
            runtime = runtime_manager.current
            sample_interval = runtime.sample_interval_ms / 1000
            now_mono = time.monotonic()
            snapshots = state.snapshots(filters, runtime.stale_seconds)
            opportunities = engine.update(snapshots, sample_interval, stats_config)
            for symbol, opportunity in opportunities:
                stats = engine.pairs[symbol]
                if not filters.positive_basis_only or stats.mean_basis_bps > 0:
                    store.append_opportunity(symbol, opportunity)

            if now_mono - last_persist >= runtime.persist_interval_seconds:
                store.save_states(engine.eligible_rows(filters.positive_basis_only))
                last_persist = now_mono
            if now_mono - last_display >= runtime.display_refresh_seconds:
                errors = [
                    manager.error
                    for manager in (filter_manager, statistics_manager, runtime_manager)
                    if manager.error
                ]
                print(
                    "\x1b[2J\x1b[H"
                    + render(state, engine, filters, stats_config, runtime, started_mono, errors),
                    end="",
                    flush=True,
                )
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
        store.save_states(engine.eligible_rows(filter_manager.current.positive_basis_only))
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

# python 程序入口标准写法
if __name__ == "__main__":
    if os.name == "nt":
        os.system("")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass
