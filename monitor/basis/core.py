from __future__ import annotations

import asyncio
import json
import queue
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any

import numpy as np

from ..models import PairSnapshot
from ..settings import StatisticsConfig

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
    quantile_cache_version: tuple[Any, ...] | None = field(default=None, repr=False)
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
            cache_version = (
                stats.sample_sequence, config.short_window_samples,
                config.long_window_samples, config.quantile_low, config.quantile_high,
            )
            if stats.quantile_cache_version == cache_version:
                continue
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
            stats.quantile_cache_version = cache_version

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
