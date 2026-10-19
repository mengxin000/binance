from __future__ import annotations

import json
from pathlib import Path

from .core import StatisticsEngine
from ..models import MarketState
from ..settings import FilterConfig, StatisticsConfig


def export_snapshot(
    state: MarketState, engine: StatisticsEngine,
    filters: FilterConfig, config: StatisticsConfig,
) -> dict:
    routes = state.pair_routes(filters)
    eligible = state.eligible_symbols(filters)
    pairs = []
    for stats in engine.selected_rows(config.positive_basis_only, config):
        if stats.symbol not in eligible or stats.symbol not in routes:
            continue
        route = routes[stats.symbol]
        row = {
            "spot_symbol": route.spot_symbol,
            "futures_symbol": route.futures_symbol,
            "basis": route.spot_symbol[:-len(route.spot_quote)],
            "mu_bps": stats.mean_basis_bps,
            "sigma_bps": stats.standard_deviation_bps,
        }
        for name, size in (("short", config.short_window_samples),
                           ("long", config.long_window_samples)):
            bounds = stats.cached_quantile_range(
                size, config.quantile_low, config.quantile_high,
            )
            row[name] = {
                "p5_bps": bounds[0] if bounds else None,
                "p95_bps": bounds[1] if bounds else None,
            }
        pairs.append(row)
    return {"pairs": pairs}


def write_snapshot(path: Path, snapshot: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
