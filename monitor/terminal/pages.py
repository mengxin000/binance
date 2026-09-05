from __future__ import annotations

import os
import time
import unicodedata
from typing import Any

from ..basis.core import StatisticsEngine
from ..funding.core import FundingHistoryStats
from ..models import MarketState
from ..settings import FilterConfig, FundingConfig, RuntimeConfig, StatisticsConfig

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
    metrics = state.pipeline_metrics
    if metrics:
        pipeline = (
            f"BBO最新槽待解析: {metrics.get('pending_symbols', 0)} | "
            f"接收: {metrics.get('received', 0)} | 覆盖旧值: {metrics.get('overwritten', 0)} | "
            f"已解析: {metrics.get('parsed', 0)} | 过期: {metrics.get('expired', 0)} | "
            f"无效: {metrics.get('invalid', 0)} | E超限: {metrics.get('transport_lag', 0)} | "
        )
    else:
        buffers = list(state.raw_buffers.values())
        pipeline = (
            f"BBO兼容缓存: {sum(len(b.messages) for b in buffers)} / "
            f"{sum(b.messages.maxlen or 0 for b in buffers)} | "
        )
    return (
        pipeline +
        f"主循环延迟: {state.loop_lag_ms:.1f}ms / 峰值 {state.max_loop_lag_ms:.1f}ms | "
        f"现货网络: {getattr(state, 'spot_network_loop_lag_ms', 0):.1f}/"
        f"{getattr(state, 'spot_network_max_loop_lag_ms', 0):.1f}ms "
        f"({getattr(state, 'spot_network_connections', 0)}连接) | "
        f"永续网络: {getattr(state, 'futures_network_loop_lag_ms', 0):.1f}/"
        f"{getattr(state, 'futures_network_max_loop_lag_ms', 0):.1f}ms "
        f"({getattr(state, 'futures_network_connections', 0)}连接) | "
        f"ticker网络: {getattr(state, 'ticker_loop_lag_ms', 0):.1f}/"
        f"{getattr(state, 'ticker_max_loop_lag_ms', 0):.1f}ms"
    )


def funding_snapshot(
    state: MarketState, symbol: str, config: FundingConfig,
) -> tuple[str, str, float | None, float | None, float | None, FundingHistoryStats | None]:
    history = state.funding_statistics.get(symbol)
    quote = state.funding_quotes.get(symbol)
    average_text = f"{history.average_bps_8h:+.2f}bp" if history else "--"
    if quote is None:
        return "--", average_text, None, None, None, history
    age = max(0.0, time.monotonic() - quote.received_at)
    if age > config.current_max_age_seconds:
        return "--", average_text, None, None, age, history
    current_text = f"{quote.rate * 10_000:+.2f}bp"
    if history is None:
        return current_text, average_text, None, None, age, None
    current = history.current_bps_8h(quote)
    percentile = history.percentile(current)
    return current_text, average_text, current, percentile, age, history

def render(
    state: MarketState,
    engine: StatisticsEngine,
    filters: FilterConfig,
    stats_config: StatisticsConfig,
    funding_config: FundingConfig,
    runtime: RuntimeConfig,
    started_mono: float,
    config_errors: list[str],
) -> str:
    rows = engine.selected_rows(stats_config.positive_basis_only, stats_config)
    rows = [
        stats for stats in rows
        if stats.symbol in state.eligible_symbols(filters)
        and (age := state.quote_age(stats.symbol, filters)) is not None
        and age <= runtime.stale_seconds
    ]
    lines = [
        "Binance 现货－USDT/USDC-M 永续基差波动筛选  [按 K 切换页面]",
        f"成交额流: 现货 {'在线' if state.ticker_connected['spot'] else '重连中'} / "
        f"永续 {'在线' if state.ticker_connected['futures'] else '重连中'} | "
        f"盘口流: 现货 {'在线' if state.book_connected['spot'] else '重连中'} / "
        f"永续 {'在线' if state.book_connected['futures'] else '重连中'}",
        f"报价币种: {','.join(sorted(state.quote_assets))} | 配对路线: {len(state.common_symbols(filters))} | "
        f"成交额达标: {len(state.liquid_symbols(filters))} | "
        f"价格达标: {len(state.eligible_symbols(filters))} | "
        f"入选统计: {len(rows)} | 正基差筛选: {'开启' if stats_config.positive_basis_only else '关闭'} | "
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
    widths = [3, 28, 13, 13, 16, 10, 21, 21, 21, 21, 11, 12, 12, 10, 10, 10, 10]
    aligns = [
        "right", "left", "right", "right", "right", "right",
        "right", "right", "right", "right", "right", "right",
        "right", "right", "right", "right", "right",
    ]
    separator = "-" * (sum(widths) + 2 * (len(widths) - 1))
    lines += [
        "",
        "【基差机会统计】",
        separator,
        table_row(
            ["#", "现货/永续", "现货成交额", "永续成交额", "现货价格", "μ", f"μ±{stats_config.sigma_multiplier:.0f}σ", f"{quantile_name}短窗", f"{quantile_name}长窗", "运行范围", "当前价差", "当前资费", "30日均资费", "当前位置", "行情年龄", "开仓机会", "平仓机会"],
            widths, aligns,
        ),
        separator,
    ]
    if rows:
        for index, stats in enumerate(rows[: stats_config.top], 1):
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
            route = state.route(stats.symbol, filters)
            current_funding, average_funding, *_ = funding_snapshot(
                state, route.futures_symbol if route else "", funding_config
            )
            lines.append(table_row([
                str(index), state.pair_label(stats.symbol, filters),
                format_quote_volume(spot_volume),
                format_quote_volume(futures_volume),
                f"{stats.current_spot_mid:.8g}",
                f"{stats.mean_basis_bps:+.2f}bp",
                f"{lower_sigma:+.2f}～{upper_sigma:+.2f}bp", short_text, long_text, runtime_text,
                f"{stats.current_basis_bps:+.2f}bp",
                current_funding, average_funding,
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
    funding_config: FundingConfig,
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
        "Binance USDT永续－USDC永续价差波动筛选  [按 K 切换页面]",
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
    widths = [3, 28, 13, 13, 16, 16, 10, 21, 21, 21, 21, 11, 12, 12, 10, 10, 10, 10]
    aligns = ["right", "left"] + ["right"] * 16
    separator = "-" * (sum(widths) + 2 * (len(widths) - 1))
    lines += [
        "",
        "【USDT永续－USDC永续价差统计】",
        separator,
        table_row(
            ["#", "USDT永续/USDC永续", "USDT成交额", "USDC成交额", "USDT永续价",
             "USDC折算价", "μ", f"μ±{stats_config.sigma_multiplier:.0f}σ", f"{quantile_name}短窗", f"{quantile_name}长窗",
             "运行范围", "当前价差", "当前资费差", "30日均资差", "当前位置", "行情年龄", "开仓机会", "平仓机会"],
            widths, aligns,
        ),
        separator,
    ]
    if rows:
        for index, stats in enumerate(rows[: stats_config.top], 1):
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
            route = routes.get(stats.symbol)
            usdt_funding = funding_snapshot(
                state, route.usdt_symbol if route else "", funding_config
            )
            usdc_funding = funding_snapshot(
                state, route.usdc_symbol if route else "", funding_config
            )
            current_funding_spread = (
                f"{usdc_funding[2] - usdt_funding[2]:+.2f}bp"
                if usdc_funding[2] is not None and usdt_funding[2] is not None else "--"
            )
            average_funding_spread = (
                f"{usdc_funding[5].average_bps_8h - usdt_funding[5].average_bps_8h:+.2f}bp"
                if usdc_funding[5] is not None and usdt_funding[5] is not None else "--"
            )
            lines.append(table_row([
                str(index), state.futures_futures_label(stats.symbol, filters),
                format_quote_volume(usdt_volume), format_quote_volume(usdc_volume),
                f"{stats.current_spot_mid:.8g}", f"{stats.current_futures_mid:.8g}",
                f"{stats.mean_basis_bps:+.2f}bp",
                f"{stats.mean_basis_bps - stats_config.sigma_multiplier * sigma:+.2f}～{stats.mean_basis_bps + stats_config.sigma_multiplier * sigma:+.2f}bp",
                short_text, long_text, runtime_text,
                f"{stats.current_basis_bps:+.2f}bp", current_funding_spread,
                average_funding_spread, f"{stats.sigma_position:+.2f}σ",
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


def render_funding(
    state: MarketState,
    engine: StatisticsEngine,
    filters: FilterConfig,
    stats_config: StatisticsConfig,
    funding_config: FundingConfig,
    runtime: RuntimeConfig,
    started_mono: float,
    config_errors: list[str],
) -> str:
    del engine, stats_config
    routes = state.liquid_routes(filters)
    eligible = state.eligible_symbols(filters)
    rows: list[tuple[Any, FundingHistoryStats | None, str, str, float | None,
                     float | None, float | None]] = []
    for key in eligible:
        route = routes.get(key)
        if route is None:
            continue
        current_text, average_text, current, percentile, age, history = funding_snapshot(
            state, route.futures_symbol, funding_config
        )
        if history is None and age is None:
            continue
        if history is not None and abs(history.average_bps_8h) < funding_config.min_abs_avg_funding_bps_8h:
            continue
        rows.append((route, history, current_text, average_text, current, percentile, age))
    rows.sort(key=lambda row: (row[1] is not None,
                              abs(row[1].average_bps_8h) if row[1] else 0,
                              row[0].label), reverse=True)

    lines = [
        "Binance 现货－永续资金费率套利筛选  [按 K 切换页面]",
        f"成交额流: 现货 {'在线' if state.ticker_connected['spot'] else '重连中'} / "
        f"永续 {'在线' if state.ticker_connected['futures'] else '重连中'} | "
        f"资金费率流: {'在线' if state.funding_connected else '重连中'}",
        f"公共初筛: 成交额达标 {len(state.liquid_symbols(filters))} / "
        f"价格达标 {len(eligible)} | 30日历史就绪: {len(state.funding_statistics)} | "
        f"入选统计: {len(rows)} | 本次运行: {format_duration(time.monotonic() - started_mono)}",
        f"入选: |30日平均资金费率| ≥ {funding_config.min_abs_avg_funding_bps_8h:g}bp/8h | "
        f"历史窗口: {funding_config.history_days}日 | "
        f"中性区: ±{funding_config.neutral_band_bps_8h:g}bp/8h",
    ]
    for message in config_errors:
        lines.append(f"配置错误: {message}")
    for key, message in state.errors.items():
        if message:
            lines.append(f"{key} 错误: {message}")
    if state.funding_history_errors:
        lines.append(f"资金费率历史暂时失败: {len(state.funding_history_errors)}个标的（后台自动重试）")

    widths = [3, 28, 13, 13, 14, 10, 13, 13, 13, 6, 13, 16, 10]
    aligns = ["right", "left"] + ["right"] * 11
    separator = "-" * (sum(widths) + 2 * (len(widths) - 1))
    lines += [
        "",
        "【资金费率统计】",
        separator,
        table_row([
            "#", "现货/永续", "现货成交额", "永续成交额", "当前资费(本期)",
            "30日分位", "30日均值", "正费均值", "负费均值", "方向", "方向一致率",
            "距反向结算", "更新时间",
        ], widths, aligns),
        separator,
    ]
    now_ms = int(time.time() * 1000)
    for index, (route, history, current_text, average_text, current, percentile, age) in enumerate(
        rows[:funding_config.top], 1
    ):
        spot_volume = state.spot_volumes[route.spot_symbol].quote_volume
        futures_volume = state.futures_volumes[route.futures_symbol].quote_volume
        ratio_text = "--"
        streak_text = "--"
        direction = "--"
        if current is not None and history is not None:
            ratio = history.direction_ratio(current, funding_config.neutral_band_bps_8h)
            direction = "正" if current > funding_config.neutral_band_bps_8h else (
                "负" if current < -funding_config.neutral_band_bps_8h else "中性"
            )
            ratio_text = f"{ratio * 100:.1f}%"
            streak = history.direction_streak_seconds(
                current, funding_config.neutral_band_bps_8h, now_ms
            )
            if streak is not None:
                _, seconds, censored = streak
                days, remainder = divmod(int(seconds), 86400)
                duration = (
                    f"{days}天{remainder // 3600}小时"
                    if days else format_duration(seconds)
                )
                streak_text = ("≥" if censored else "") + duration
        lines.append(table_row([
            str(index), route.label, format_quote_volume(spot_volume),
            format_quote_volume(futures_volume), current_text,
            f"{percentile * 100:.1f}%" if percentile is not None else "--",
            average_text,
            f"{history.positive_average_bps_8h:+.2f}bp"
            if history is not None and history.positive_average_bps_8h is not None else "--",
            f"{history.negative_average_bps_8h:+.2f}bp"
            if history is not None and history.negative_average_bps_8h is not None else "--",
            direction, ratio_text, streak_text,
            f"{age:.1f}秒" if age is not None else "--",
        ], widths, aligns))
    if not rows:
        lines.append("暂无标的满足30日平均资金费率门槛；历史数据仍会在后台持续加载。")
    lines += [
        separator,
        "当前资费为本期原始费率；均值为bp/8h，分位数使用8h等效值。历史待加载的路线列于榜尾，尚未通过均值筛选。",
        "方向按8h等效资费及中性区判定；一致率为历史同方向时间占比；距反向结算不代表实时费率连续保持同方向。",
    ]
    return "\n".join(lines)


def poll_page_toggle(current_page: str) -> str:
    if os.name != "nt":
        return current_page
    import msvcrt

    while msvcrt.kbhit():
        if msvcrt.getwch().lower() == "k":
            pages = ("spot_futures", "futures_futures", "funding")
            current_page = pages[(pages.index(current_page) + 1) % len(pages)]
    return current_page
