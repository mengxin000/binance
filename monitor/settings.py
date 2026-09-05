from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
# 24hr ticker
SPOT_TICKER_STREAM = "wss://data-stream.binance.vision/ws/!miniTicker@arr"
FUTURES_TICKER_STREAM = "wss://fstream.binance.com/market/ws/!ticker@arr"
FUNDING_STREAM = "wss://fstream.binance.com/market/ws/!markPrice@arr"
# 流
SPOT_BOOK_BASE = "wss://data-stream.binance.vision/stream?streams="
FUTURES_BOOK_BASE = "wss://fstream.binance.com/public/stream?streams="
# Binance 每20秒主动发送 Ping；websockets 会自动回传同载荷 Pong。
# 禁用客户端额外 Ping，避免本地 ping_timeout 以 1011 主动断开连接。
BINANCE_WEBSOCKET_OPTIONS = {
    "proxy": None,
    # BBO messages are tiny; avoid synchronous per-message deflate work on weak CPUs.
    "compression": None,
    "open_timeout": 20,
    "ping_interval": None,
    "ping_timeout": None,
    "close_timeout": 5,
    "max_size": 8_000_000,
}
FILTER_CONFIG_PATH = BASE_DIR / "config" / "market.json"
STATISTICS_CONFIG_PATH = BASE_DIR / "config" / "basis_spot_futures.json"
FUTURES_FUTURES_STATISTICS_CONFIG_PATH = BASE_DIR / "config" / "basis_futures_futures.json"
RUNTIME_CONFIG_PATH = BASE_DIR / "config" / "runtime.json"
FUNDING_CONFIG_PATH = CONFIG_DIR / "funding.json"


# 配置文件类
@dataclass(frozen=True, slots=True)
class FilterConfig:
    min_spot_volume: float = 10_000_000
    min_futures_volume: float = 10_000_000
    min_spot_price: float = 0.1
    quote_assets: tuple[str, ...] = ("USDT",)
    cross_quote_pairings: tuple[tuple[str, str], ...] = ()

    def validate(self) -> None:
        if self.min_spot_volume < 0 or self.min_futures_volume < 0:
            raise ValueError("成交额门槛不能小于 0")
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
    top: int = 40
    positive_basis_only: bool = False
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
        if not isinstance(self.top, int) or isinstance(self.top, bool) or self.top < 1:
            raise ValueError("top 必须是大于 0 的整数")
        if not isinstance(self.positive_basis_only, bool):
            raise ValueError("positive_basis_only 必须是布尔值")
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
    book_symbols_per_connection: int = 20
    raw_coalesce_interval_ms: int = 200
    network_log_max_bytes: int = 20_000_000
    data_directory: str = "data"

    def validate(self) -> None:
        if not isinstance(self.sample_interval_ms, int) or self.sample_interval_ms < 50:
            raise ValueError("sample_interval_ms 必须是至少 50 的整数")
        if (
            isinstance(self.raw_coalesce_interval_ms, bool)
            or not isinstance(self.raw_coalesce_interval_ms, int)
            or not 20 <= self.raw_coalesce_interval_ms <= 1000
        ):
            raise ValueError("raw_coalesce_interval_ms 必须是 20 到 1000 的整数")
        if (
            isinstance(self.network_log_max_bytes, bool)
            or not isinstance(self.network_log_max_bytes, int)
            or self.network_log_max_bytes < 1_000_000
        ):
            raise ValueError("network_log_max_bytes 必须至少为 1000000")
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


@dataclass(frozen=True, slots=True)
class FundingConfig:
    history_days: int = 30
    min_abs_avg_funding_bps_8h: float = 0.5
    neutral_band_bps_8h: float = 0.05
    current_max_age_seconds: float = 10.0
    history_refresh_seconds: float = 3600.0
    top: int = 40

    def validate(self) -> None:
        if not isinstance(self.history_days, int) or not 1 <= self.history_days <= 90:
            raise ValueError("history_days 必须是 1 到 90 的整数")
        if self.min_abs_avg_funding_bps_8h < 0:
            raise ValueError("min_abs_avg_funding_bps_8h 不能小于 0")
        if self.neutral_band_bps_8h < 0:
            raise ValueError("neutral_band_bps_8h 不能小于 0")
        if self.current_max_age_seconds <= 0:
            raise ValueError("current_max_age_seconds 必须大于 0")
        if self.history_refresh_seconds < 60:
            raise ValueError("history_refresh_seconds 必须至少为 60")
        if not isinstance(self.top, int) or isinstance(self.top, bool) or self.top < 1:
            raise ValueError("top 必须是大于 0 的整数")


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
