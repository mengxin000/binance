from __future__ import annotations

import asyncio
import copy
import signal
import time
from collections import deque
from itertools import islice
from pathlib import Path
from typing import Any

from .basis.core import (BackgroundWriter, BasisDiagnosticStore, LatestScreenWriter,
                         QuantileWorker, StatisticsEngine)
from .models import MarketState
from .funding.core import FundingHistoryWorker
from .network.core import MarketNetworkWorker, monitor_loop_lag
from .settings import (BASE_DIR, FILTER_CONFIG_PATH, FUNDING_CONFIG_PATH,
                       FUTURES_FUTURES_STATISTICS_CONFIG_PATH, RUNTIME_CONFIG_PATH,
                       STATISTICS_CONFIG_PATH, ConfigManager, FilterConfig,
                       FundingConfig, RuntimeConfig, StatisticsConfig)
from .terminal.pages import (poll_page_toggle, render, render_funding,
                             render_futures_futures)

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

def make_screen_job(
    renderer: Any, state: MarketState, engine: StatisticsEngine,
    filters: FilterConfig, config: StatisticsConfig, funding: FundingConfig,
    runtime: RuntimeConfig,
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
    return lambda: renderer(
        view, snapshot_engine, filters, config, funding, runtime, started, errors
    )


async def run() -> None:
    filter_manager = ConfigManager(FILTER_CONFIG_PATH, FilterConfig)
    statistics_manager = ConfigManager(STATISTICS_CONFIG_PATH, StatisticsConfig)
    futures_futures_statistics_manager = ConfigManager(
        FUTURES_FUTURES_STATISTICS_CONFIG_PATH, StatisticsConfig
    )
    runtime_manager = ConfigManager(RUNTIME_CONFIG_PATH, RuntimeConfig)
    funding_manager = ConfigManager(FUNDING_CONFIG_PATH, FundingConfig)
    data_root = resolve_path(runtime_manager.current.data_directory)
    diagnostic_store = BasisDiagnosticStore(data_root / "diagnostics")
    disk_writer = BackgroundWriter(runtime_manager.current.background_write_queue_size)
    screen_writer = LatestScreenWriter()
    quantile_worker = QuantileWorker()
    engine = StatisticsEngine()
    futures_futures_engine = StatisticsEngine()
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
    network_worker = MarketNetworkWorker(
        filter_manager, runtime_manager, data_root / "diagnostics"
    )
    funding_history_worker = FundingHistoryWorker(funding_manager)
    network_worker.start()
    tasks = [
        asyncio.create_task(monitor_loop_lag(state, stop)),
    ]
    last_display = last_quantile = 0.0
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
                futures_futures_statistics_manager, runtime_manager, funding_manager,
            ))
            filters = filter_manager.current
            network_worker.apply_latest(state)
            state.set_pairing_config(filters)
            liquid_routes = state.liquid_routes(filters)
            funding_symbols = {
                liquid_routes[key].futures_symbol
                for key in state.eligible_symbols(filters)
                if key in liquid_routes
            }
            for route in state.liquid_futures_futures_routes(filters).values():
                funding_symbols.update((route.usdt_symbol, route.usdc_symbol))
            funding_history_worker.set_symbols(funding_symbols)
            funding_history_worker.apply_latest(state)
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
            engine.update(snapshots, sample_interval, stats_config, now_wall)

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
            futures_futures_engine.update(
                futures_futures_snapshots,
                sample_interval,
                futures_futures_stats_config,
                now_wall,
            )
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
                        runtime_manager, funding_manager,
                    )
                    if manager.error
                ]
                state.errors["background"] = " / ".join(filter(None, (
                    disk_writer.last_error, screen_writer.last_error,
                    f"写盘任务丢弃 {disk_writer.dropped_records}" if disk_writer.dropped_records else "",
                )))
                if current_page == "spot_futures":
                    renderer, page_engine, page_config = render, engine, stats_config
                elif current_page == "futures_futures":
                    renderer, page_engine, page_config = (
                        render_futures_futures,
                        futures_futures_engine,
                        futures_futures_stats_config,
                    )
                else:
                    renderer, page_engine, page_config = render_funding, engine, stats_config
                screen_writer.publish(make_screen_job(
                    renderer, state, page_engine,
                    filters, page_config, funding_manager.current,
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
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await quantile_worker.close(cancel=True)
        await asyncio.gather(
            asyncio.to_thread(disk_writer.close),
            asyncio.to_thread(screen_writer.close),
            asyncio.to_thread(network_worker.close),
            asyncio.to_thread(funding_history_worker.close),
        )
