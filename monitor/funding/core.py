from __future__ import annotations

import bisect
import statistics
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable

import requests

from ..models import FundingQuote, MarketState
from ..settings import FundingConfig


FUNDING_HISTORY_URL = "https://fapi.binance.com/fapi/v1/fundingRate"


@dataclass(frozen=True, slots=True)
class FundingPayment:
    funding_time_ms: int
    rate: float


@dataclass(frozen=True, slots=True)
class FundingHistoryStats:
    symbol: str
    sample_count: int
    observed_start_ms: int
    observed_end_ms: int
    average_bps_8h: float
    positive_average_bps_8h: float | None
    negative_average_bps_8h: float | None
    positive_time_ratio: float
    negative_time_ratio: float
    normalized_rates: tuple[float, ...]
    settlement_times_ms: tuple[int, ...]
    interval_hours: tuple[float, ...]

    def current_interval_hours(self, quote: FundingQuote) -> float:
        if self.settlement_times_ms:
            hours = (
                quote.next_funding_time_ms - self.settlement_times_ms[-1]
            ) / 3_600_000
            if 0.5 <= hours <= 24:
                return hours
        return statistics.median(self.interval_hours) if self.interval_hours else 8.0

    def current_bps_8h(self, quote: FundingQuote) -> float:
        return quote.rate * 10_000 * 8 / self.current_interval_hours(quote)

    def percentile(self, current_bps_8h: float) -> float | None:
        if not self.normalized_rates:
            return None
        ordered = sorted(self.normalized_rates)
        return bisect.bisect_right(ordered, current_bps_8h) / len(ordered)

    def direction_ratio(self, current_bps_8h: float, neutral_band: float) -> float:
        if current_bps_8h > neutral_band:
            return self.positive_time_ratio
        if current_bps_8h < -neutral_band:
            return self.negative_time_ratio
        return max(0.0, 1.0 - self.positive_time_ratio - self.negative_time_ratio)

    def direction_streak_seconds(
        self, current_bps_8h: float, neutral_band: float, now_ms: int,
    ) -> tuple[int, float, bool] | None:
        direction = 1 if current_bps_8h > neutral_band else -1 if current_bps_8h < -neutral_band else 0
        if direction == 0 or not self.settlement_times_ms:
            return None
        opposite_time: int | None = None
        for rate, settled_at in zip(
            reversed(self.normalized_rates), reversed(self.settlement_times_ms)
        ):
            if direction > 0 and rate < -neutral_band:
                opposite_time = settled_at
                break
            if direction < 0 and rate > neutral_band:
                opposite_time = settled_at
                break
        if opposite_time is None:
            return direction, max(0.0, (now_ms - self.observed_start_ms) / 1000), True
        return direction, max(0.0, (now_ms - opposite_time) / 1000), False


def _intervals_hours(records: list[FundingPayment]) -> list[float]:
    differences = [
        (right.funding_time_ms - left.funding_time_ms) / 3_600_000
        for left, right in zip(records, records[1:])
        if right.funding_time_ms > left.funding_time_ms
    ]
    valid = [value for value in differences if 0.5 <= value <= 24]
    fallback = statistics.median(valid) if valid else 8.0
    result = [fallback]
    for index in range(1, len(records)):
        value = (records[index].funding_time_ms - records[index - 1].funding_time_ms) / 3_600_000
        result.append(value if 0.5 <= value <= 24 else fallback)
    return result


def calculate_funding_statistics(
    symbol: str, records: Iterable[FundingPayment], neutral_band_bps_8h: float = 0.05,
) -> FundingHistoryStats | None:
    rows = sorted(records, key=lambda row: row.funding_time_ms)
    if not rows:
        return None
    intervals = _intervals_hours(rows)
    normalized = [row.rate * 10_000 * 8 / hours for row, hours in zip(rows, intervals)]
    total_hours = sum(intervals)
    average = sum(row.rate * 10_000 for row in rows) * 8 / total_hours

    positive = [(row, hours) for row, hours, value in zip(rows, intervals, normalized)
                if value > neutral_band_bps_8h]
    negative = [(row, hours) for row, hours, value in zip(rows, intervals, normalized)
                if value < -neutral_band_bps_8h]

    def conditional_average(items: list[tuple[FundingPayment, float]]) -> float | None:
        hours = sum(interval for _, interval in items)
        if not hours:
            return None
        return sum(row.rate * 10_000 for row, _ in items) * 8 / hours

    positive_hours = sum(hours for _, hours in positive)
    negative_hours = sum(hours for _, hours in negative)
    return FundingHistoryStats(
        symbol=symbol,
        sample_count=len(rows),
        observed_start_ms=rows[0].funding_time_ms,
        observed_end_ms=rows[-1].funding_time_ms,
        average_bps_8h=average,
        positive_average_bps_8h=conditional_average(positive),
        negative_average_bps_8h=conditional_average(negative),
        positive_time_ratio=positive_hours / total_hours,
        negative_time_ratio=negative_hours / total_hours,
        normalized_rates=tuple(normalized),
        settlement_times_ms=tuple(row.funding_time_ms for row in rows),
        interval_hours=tuple(intervals),
    )


def parse_funding_history(payload: Any) -> list[FundingPayment]:
    if not isinstance(payload, list):
        raise ValueError("资金费率历史响应不是数组")
    rows: list[FundingPayment] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        try:
            rows.append(FundingPayment(int(item["fundingTime"]), float(item["fundingRate"])))
        except (KeyError, TypeError, ValueError):
            continue
    return rows


class FundingHistoryClient:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

    def fetch(self, symbol: str, history_days: int) -> list[FundingPayment]:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - history_days * 86_400_000
        response = self.session.get(
            FUNDING_HISTORY_URL,
            params={"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return parse_funding_history(response.json())

    def close(self) -> None:
        self.session.close()


class FundingHistoryWorker:
    """Refresh settled funding history outside all asyncio/network hot paths."""

    def __init__(self, config_manager: Any, client: FundingHistoryClient | None = None) -> None:
        self.config_manager = config_manager
        self.client = client or FundingHistoryClient()
        self._lock = threading.Lock()
        self._desired_symbols: tuple[str, ...] = ()
        self._statistics: dict[str, FundingHistoryStats] = {}
        self._last_refresh: dict[str, float] = {}
        self._errors: dict[str, str] = {}
        self._config_signature: tuple[int, float] | None = None
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="funding-history", daemon=True)
        self._thread.start()

    def set_symbols(self, symbols: set[str]) -> None:
        updated = tuple(sorted(symbols))
        with self._lock:
            changed = updated != self._desired_symbols
            self._desired_symbols = updated
        if changed:
            self._wake.set()

    def snapshot(self) -> tuple[dict[str, FundingHistoryStats], dict[str, str]]:
        with self._lock:
            return self._statistics.copy(), self._errors.copy()

    def apply_latest(self, state: MarketState) -> None:
        statistics_rows, errors = self.snapshot()
        state.funding_statistics = statistics_rows
        state.funding_history_errors = errors

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                config: FundingConfig = self.config_manager.current
                now = time.monotonic()
                with self._lock:
                    signature = (config.history_days, config.neutral_band_bps_8h)
                    if signature != self._config_signature:
                        self._last_refresh.clear()
                        self._config_signature = signature
                    symbols = self._desired_symbols
                    last_refresh = self._last_refresh.copy()
                due = [symbol for symbol in symbols
                       if now - last_refresh.get(symbol, 0.0) >= config.history_refresh_seconds]
                for symbol in due:
                    if self._stop.is_set():
                        break
                    try:
                        records = self.client.fetch(symbol, config.history_days)
                        stats = calculate_funding_statistics(
                            symbol, records, config.neutral_band_bps_8h,
                        )
                        with self._lock:
                            if stats is not None:
                                self._statistics[symbol] = stats
                            self._last_refresh[symbol] = time.monotonic()
                            self._errors.pop(symbol, None)
                    except Exception as exc:
                        with self._lock:
                            self._last_refresh[symbol] = (
                                time.monotonic()
                                - max(0.0, config.history_refresh_seconds - 60.0)
                            )
                            self._errors[symbol] = f"{type(exc).__name__}: {exc}"
                    if self._stop.wait(0.05):
                        break
                self._wake.wait(5.0)
                self._wake.clear()
        finally:
            self.client.close()

    def close(self) -> None:
        self._stop.set()
        self._wake.set()
        self._thread.join(timeout=15)
        if self._thread.is_alive():
            raise RuntimeError("资金费率历史线程未在15秒内退出")
