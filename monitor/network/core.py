from __future__ import annotations

import asyncio
import copy
import json
import logging
import queue
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.frames import Frame, OP_BINARY, OP_CONT, OP_TEXT

from ..models import BookQuote, FundingQuote, MarketState, TransportLagError
from ..settings import (FUNDING_STREAM, FUTURES_BOOK_BASE, FUTURES_TICKER_STREAM,
                        SPOT_BOOK_BASE, SPOT_TICKER_STREAM)
from ..settings import ConfigManager, RuntimeConfig

async def consume_tickers(
    url: str, market: str, state: MarketState, stop: asyncio.Event,
    diagnostics: NetworkDiagnostics | None = None,
) -> None:
    delay = 1.0
    error_key = f"{market}_ticker"
    while not stop.is_set():
        try:
            connect_started = time.monotonic()
            async with websockets.connect(
                url, **(await state.connection_options(url)),
            ) as websocket:
                if diagnostics is not None:
                    diagnostics.emit(
                        "connection_open", connection=error_key,
                        duration_ms=round((time.monotonic() - connect_started) * 1000, 1),
                    )
                state.ticker_connected[market] = True
                state.errors[error_key] = ""
                delay = 1.0
                while not stop.is_set():
                    raw = await asyncio.wait_for(websocket.recv(), timeout=45)
                    parse_started = time.monotonic()
                    payload = json.loads(raw)
                    if isinstance(payload, list):
                        state.update_tickers(market, payload)
                    elapsed_ms = (time.monotonic() - parse_started) * 1000
                    if diagnostics is not None and elapsed_ms >= 100:
                        diagnostics.emit(
                            "slow_ticker_parse", connection=error_key,
                            duration_ms=round(elapsed_ms, 1),
                            rows=len(payload) if isinstance(payload, list) else 0,
                        )
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


async def consume_funding(
    url: str, state: MarketState, stop: asyncio.Event,
    diagnostics: NetworkDiagnostics | None = None,
) -> None:
    delay = 1.0
    error_key = "funding_stream"
    while not stop.is_set():
        try:
            async with websockets.connect(
                url, **(await state.connection_options(url))
            ) as websocket:
                state.funding_connected = False
                state.errors[error_key] = ""
                delay = 1.0
                while not stop.is_set():
                    raw = await asyncio.wait_for(websocket.recv(), timeout=45)
                    payload = json.loads(raw)
                    if isinstance(payload, dict) and "data" in payload:
                        payload = payload["data"]
                    if not isinstance(payload, list):
                        raise ValueError("资金费率流返回非数组消息: " + str(payload)[:300])
                    received_at = time.monotonic()
                    updated = 0
                    for row in payload:
                        if not isinstance(row, dict):
                            continue
                        symbol = str(row.get("s", ""))
                        if not state.is_allowed_symbol(symbol):
                            continue
                        try:
                            quote = FundingQuote(
                                rate=float(row["r"]),
                                next_funding_time_ms=int(row["T"]),
                                event_time_ms=int(row.get("E", 0)),
                                received_at=received_at,
                            )
                        except (KeyError, TypeError, ValueError):
                            continue
                        state.funding_quotes[symbol] = quote
                        updated += 1
                    if updated:
                        state.funding_connected = True
                        state.funding_last_update = received_at
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.funding_connected = False
            state.errors[error_key] = f"{type(exc).__name__}: {exc}"
            if diagnostics is not None:
                diagnostics.emit("funding_stream_retry", error=state.errors[error_key],
                                 delay_seconds=delay)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 30)
    state.funding_connected = False


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


@dataclass(slots=True)
class PipelineCounter:
    received: int = 0
    overwritten: int = 0
    parsed: int = 0
    expired: int = 0
    invalid: int = 0
    transport_lag: int = 0


@dataclass(frozen=True, slots=True)
class LatestRawQuote:
    market: str
    symbol: str
    connection_key: str
    connection_token: object
    message: RawMessage


class NetworkDiagnostics:
    """Bounded, rotating JSONL logger that never writes on a market thread."""

    def __init__(self, path: Path | None, max_bytes: int) -> None:
        self.path, self.max_bytes = path, max_bytes
        self.queue: queue.Queue[dict[str, Any]] = queue.Queue(2000)
        self.dropped = 0
        self.error = ""
        self.stop = threading.Event()
        self.thread = (
            threading.Thread(target=self._guarded_run, name="network-log", daemon=True)
            if path is not None else None
        )
        if self.thread is not None:
            self.thread.start()

    def _guarded_run(self) -> None:
        try:
            self._run()
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"

    def emit(self, event: str, **fields: Any) -> None:
        if self.thread is None:
            return
        row = {
            "time": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **fields,
        }
        try:
            self.queue.put_nowait(row)
        except queue.Full:
            self.dropped += 1

    def _run(self) -> None:
        assert self.path is not None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger(f"network-pipeline-{id(self)}")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = RotatingFileHandler(
            self.path, maxBytes=self.max_bytes, backupCount=2, encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        try:
            while not self.stop.is_set() or not self.queue.empty():
                try:
                    row = self.queue.get(timeout=0.2)
                except queue.Empty:
                    continue
                logger.info(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                self.queue.task_done()
        finally:
            handler.close()
            logger.removeHandler(handler)

    def close(self) -> None:
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=3)


def extract_combined_stream_symbol(payload: bytes, allowed: frozenset[str]) -> str | None:
    """Extract only the combined-stream identity; leave data JSON untouched."""
    marker = b'"stream"'
    start = payload.find(marker)
    if start >= 0:
        colon = payload.find(b":", start + len(marker))
        quote = payload.find(b'"', colon + 1) if colon >= 0 else -1
        end = payload.find(b'"', quote + 1) if quote >= 0 else -1
        if quote >= 0 and end > quote:
            stream = payload[quote + 1:end]
            at = stream.find(b"@")
            if at > 0:
                try:
                    symbol = stream[:at].decode("ascii").upper()
                except UnicodeDecodeError:
                    return None
                return symbol if symbol in allowed else None
    # Useful for one-symbol raw/local test streams; production uses combined streams.
    return next(iter(allowed)) if len(allowed) == 1 else None


class LatestQuotePipeline:
    """One unparsed latest slot per market+symbol and one coalescing parser thread."""

    def __init__(self, runtime_manager: Any, diagnostics: NetworkDiagnostics) -> None:
        self.runtime_manager, self.diagnostics = runtime_manager, diagnostics
        self.lock = threading.Lock()
        self.latest_raw: dict[tuple[str, str], LatestRawQuote] = {}
        self.dirty: set[tuple[str, str]] = set()
        self.updates: dict[tuple[str, str], BookQuote] = {}
        self.faults: dict[object, str] = {}
        self.counters: dict[str, PipelineCounter] = {}
        self.sequence = 0
        self.error = ""
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._guarded_run, name="bbo-parser", daemon=True)
        self.thread.start()

    def _guarded_run(self) -> None:
        try:
            self._run()
        except Exception as exc:
            self.error = f"{type(exc).__name__}: {exc}"
            self.diagnostics.emit("parser_crash", error=self.error)

    def put(
        self, market: str, connection_key: str, connection_token: object,
        allowed: frozenset[str], payload: bytes, received_at: float,
        received_wall_ms: float,
    ) -> None:
        symbol = extract_combined_stream_symbol(payload, allowed)
        with self.lock:
            counter = self.counters.setdefault(connection_key, PipelineCounter())
            counter.received += 1
            if symbol is None:
                counter.invalid += 1
                return
            key = (market, symbol)
            if key in self.dirty:
                counter.overwritten += 1
            self.latest_raw[key] = LatestRawQuote(
                market, symbol, connection_key, connection_token,
                RawMessage(received_at, received_wall_ms, payload),
            )
            self.dirty.add(key)

    def _take_dirty(self) -> list[LatestRawQuote]:
        with self.lock:
            rows = [self.latest_raw[key] for key in self.dirty]
            self.dirty.clear()
        return rows

    def _increment(self, connection_key: str, field_name: str) -> None:
        with self.lock:
            counter = self.counters.setdefault(connection_key, PipelineCounter())
            setattr(counter, field_name, getattr(counter, field_name) + 1)

    def _run(self) -> None:
        last_summary = time.monotonic()
        while not self.stop.is_set():
            interval = self.runtime_manager.current.raw_coalesce_interval_ms / 1000
            if self.stop.wait(interval):
                break
            cycle_started = time.monotonic()
            rows = self._take_dirty()
            cycle_lags: dict[str, dict[str, Any]] = {}
            for raw in rows:
                age_ms = (time.monotonic() - raw.message.received_at) * 1000
                if age_ms > self.runtime_manager.current.quote_max_age_ms:
                    self._increment(raw.connection_key, "expired")
                    continue
                try:
                    payload = json.loads(raw.message.payload)
                    row = payload.get("data") if isinstance(payload, dict) else None
                    if not isinstance(row, dict):
                        raise ValueError("combined stream缺少data对象")
                    symbol = str(row["s"])
                    bid, ask = float(row["b"]), float(row["a"])
                    if symbol != raw.symbol or bid <= 0 or ask <= 0 or ask < bid:
                        raise ValueError("BBO字段无效或symbol不一致")
                    with self.lock:
                        if self.latest_raw.get((raw.market, symbol)) is not raw:
                            counter = self.counters.setdefault(
                                raw.connection_key, PipelineCounter()
                            )
                            counter.overwritten += 1
                            continue
                    event_time = row.get("E")
                    exchange_ms = (
                        int(event_time)
                        if isinstance(event_time, (int, float)) and not isinstance(event_time, bool)
                        else None
                    )
                    lag = None
                    if raw.market == "futures" and exchange_ms is not None:
                        lag = max(0.0, raw.message.received_wall_ms - exchange_ms)
                        if lag > self.runtime_manager.current.max_transport_lag_ms:
                            self._increment(raw.connection_key, "transport_lag")
                            detail = (
                                f"永续事件延迟 {lag:.0f}ms，上限 "
                                f"{self.runtime_manager.current.max_transport_lag_ms}ms"
                            )
                            with self.lock:
                                self.faults[raw.connection_token] = detail
                            lag_row = cycle_lags.setdefault(raw.connection_key, {
                                "affected_symbols": 0, "max_lag_ms": 0.0,
                                "max_lag_symbol": symbol, "max_raw_age_ms": 0.0,
                            })
                            lag_row["affected_symbols"] += 1
                            lag_row["max_raw_age_ms"] = max(
                                lag_row["max_raw_age_ms"], round(age_ms, 1)
                            )
                            if lag > lag_row["max_lag_ms"]:
                                lag_row["max_lag_ms"] = round(lag, 1)
                                lag_row["max_lag_symbol"] = symbol
                            continue
                    self.sequence += 1
                    quote = BookQuote(
                        bid, ask, raw.message.received_at, exchange_ms, lag, self.sequence
                    )
                    with self.lock:
                        # A newer raw BBO may have arrived while this row was parsed.
                        # Never publish the superseded row even transiently.
                        if self.latest_raw.get((raw.market, symbol)) is not raw:
                            counter = self.counters.setdefault(
                                raw.connection_key, PipelineCounter()
                            )
                            counter.overwritten += 1
                            continue
                        self.updates[(raw.market, symbol)] = quote
                    self._increment(raw.connection_key, "parsed")
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    self._increment(raw.connection_key, "invalid")
                    self.diagnostics.emit(
                        "invalid_bbo", connection=raw.connection_key,
                        symbol=raw.symbol, error=f"{type(exc).__name__}: {exc}",
                    )
            for connection_key, lag_row in cycle_lags.items():
                self.diagnostics.emit(
                    "transport_lag_batch", connection=connection_key, **lag_row
                )
            elapsed_ms = (time.monotonic() - cycle_started) * 1000
            if elapsed_ms > max(100, interval * 1000):
                self.diagnostics.emit(
                    "slow_parse_cycle", duration_ms=round(elapsed_ms, 1), symbols=len(rows)
                )
            if time.monotonic() - last_summary >= 60:
                self.diagnostics.emit("pipeline_summary", **self.metrics())
                last_summary = time.monotonic()

    def apply_latest(self, state: MarketState) -> None:
        with self.lock:
            updates, self.updates = self.updates, {}
        for (market, symbol), quote in updates.items():
            destination = state.spot_books if market == "spot" else state.futures_books
            destination[symbol] = quote
            state.book_last_update[market] = max(
                state.book_last_update[market], quote.received_at
            )

    def pop_fault(self, token: object) -> str | None:
        with self.lock:
            return self.faults.pop(token, None)

    def metrics(self) -> dict[str, Any]:
        with self.lock:
            pending = len(self.dirty)
            counters = [copy.copy(counter) for counter in self.counters.values()]
        return {
            "pending_symbols": pending,
            "received": sum(c.received for c in counters),
            "overwritten": sum(c.overwritten for c in counters),
            "parsed": sum(c.parsed for c in counters),
            "expired": sum(c.expired for c in counters),
            "invalid": sum(c.invalid for c in counters),
            "transport_lag": sum(c.transport_lag for c in counters),
            "log_dropped": self.diagnostics.dropped,
            "parser_error": self.error,
            "log_error": self.diagnostics.error,
        }

    def close(self) -> None:
        self.stop.set()
        self.thread.join(timeout=3)


class LatestBBOConnection(ClientConnection):
    """BBO-only adapter: bound complete messages before the library's FIFO.

    Control frames and protocol validation remain owned by websockets. Never
    drop fragments: reassemble a complete message before applying overflow.
    This process_event integration is tested against websockets 15.x.
    """

    def __init__(
        self, *args: Any, raw_capacity: int = 512,
        pipeline: LatestQuotePipeline | None = None,
        market: str = "", connection_key: str = "",
        allowed_symbols: frozenset[str] = frozenset(), **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.raw_buffer = RawMessageBuffer(raw_capacity)
        self.pipeline = pipeline
        self.market, self.connection_key = market, connection_key
        self.allowed_symbols = allowed_symbols
        self.connection_token = object()
        self.fragments: list[bytes] = []
        self.last_message_at = time.monotonic()
        self.complete_messages = 0

    def data_received(self, data: bytes) -> None:
        started = time.monotonic()
        messages_before = self.complete_messages
        try:
            super().data_received(data)
        finally:
            elapsed_ms = (time.monotonic() - started) * 1000
            if self.pipeline is not None and elapsed_ms >= 100:
                self.pipeline.diagnostics.emit(
                    "slow_ws_callback", connection=self.connection_key,
                    duration_ms=round(elapsed_ms, 1), bytes=len(data),
                    messages=self.complete_messages - messages_before,
                )

    def process_event(self, event: Any) -> None:
        if self.response is not None and isinstance(event, Frame) and event.opcode in (
            OP_TEXT, OP_BINARY, OP_CONT,
        ):
            self.fragments.append(bytes(event.data))
            if event.fin:
                self.complete_messages += 1
                payload = b"".join(self.fragments)
                self.fragments.clear()
                self.last_message_at = time.monotonic()
                received_wall_ms = time.time() * 1000
                if self.pipeline is None:
                    self.raw_buffer.append(RawMessage(
                        self.last_message_at, received_wall_ms, payload,
                    ))
                else:
                    self.pipeline.put(
                        self.market, self.connection_key, self.connection_token,
                        self.allowed_symbols, payload, self.last_message_at,
                        received_wall_ms,
                    )
            return
        super().process_event(event)

    async def watch_connection(self) -> None:
        while not self.connection_lost_waiter.done():
            try:
                await asyncio.wait_for(asyncio.shield(self.connection_lost_waiter), 1)
            except asyncio.TimeoutError:
                if self.pipeline is not None:
                    fault = self.pipeline.pop_fault(self.connection_token)
                    if fault:
                        raise TransportLagError(fault)
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
        # Compatibility path used only by direct unit-test/fallback sockets.
        for _ in range(32):
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
    pipeline: LatestQuotePipeline | None = None,
    allowed_symbols: frozenset[str] = frozenset(),
) -> None:
    delay = 1.0
    error_key = f"{market}_book_{connection_key}"
    while not stop.is_set():
        try:
            runtime = runtime_manager.current if runtime_manager is not None else RuntimeConfig()
            connect_started = time.monotonic()
            async with websockets.connect(
                url, **(await state.connection_options(url)),
                create_connection=partial(
                    LatestBBOConnection, pipeline=pipeline, market=market,
                    connection_key=error_key, allowed_symbols=allowed_symbols,
                ),
            ) as websocket:
                if pipeline is not None:
                    pipeline.diagnostics.emit(
                        "connection_open", connection=error_key,
                        duration_ms=round((time.monotonic() - connect_started) * 1000, 1),
                    )
                direct = isinstance(websocket, LatestBBOConnection)
                buffer = websocket.raw_buffer if direct else RawMessageBuffer(512)
                if pipeline is None:
                    state.raw_buffers[error_key] = buffer
                state.set_book_chunk_connected(market, connection_key, True)
                state.errors[error_key] = ""
                connected_at = time.monotonic()
                workers = [
                    asyncio.create_task(websocket.watch_connection() if direct else
                                        receive_raw_books(websocket, buffer, stop)),
                    asyncio.create_task(stop.wait()),
                ]
                if pipeline is None:
                    workers.append(asyncio.create_task(
                        decode_raw_books(market, state, buffer, stop, runtime_manager)
                    ))
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
            if pipeline is not None:
                pipeline.diagnostics.emit(
                    "connection_retry", connection=error_key,
                    error=f"{type(exc).__name__}: {exc}", delay_seconds=delay,
                )
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
    pipeline: LatestQuotePipeline | None = None,
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
                market, str(index), state, stop, runtime_manager, pipeline,
                frozenset(chunk),
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

async def monitor_loop_lag(
    state: MarketState, stop: asyncio.Event,
    diagnostics: NetworkDiagnostics | None = None,
    component: str = "main",
) -> None:
    while not stop.is_set():
        expected = time.monotonic() + 0.1
        await asyncio.sleep(0.1)
        state.loop_lag_ms = max(0.0, (time.monotonic() - expected) * 1000)
        state.max_loop_lag_ms = max(state.max_loop_lag_ms, state.loop_lag_ms)
        if diagnostics is not None and state.loop_lag_ms >= 500:
            diagnostics.emit(
                "network_loop_stall", delay_ms=round(state.loop_lag_ms, 1),
                peak_ms=round(state.max_loop_lag_ms, 1), component=component,
            )


# 显示

class BBOIngressWorker:
    """One market's BBO sockets on an isolated thread and event loop."""

    def __init__(
        self, market: str, runtime: Any, pipeline: LatestQuotePipeline,
        diagnostics: NetworkDiagnostics,
    ) -> None:
        self.market, self.runtime = market, runtime
        self.pipeline, self.diagnostics = pipeline, diagnostics
        self._desired_lock = threading.Lock()
        self._desired_symbols: tuple[str, ...] = ()
        self._status_lock = threading.Lock()
        self._status: dict[str, Any] = {
            "connected": False, "errors": {}, "loop_lag_ms": 0.0,
            "max_loop_lag_ms": 0.0, "connections": 0,
        }
        self._exit = threading.Event()
        self.error: BaseException | None = None
        self.thread_id: int | None = None
        self._thread = threading.Thread(
            target=self._run, name=f"{market}-bbo-network", daemon=True
        )

    def start(self) -> None:
        self._thread.start()

    def set_symbols(self, symbols: set[str]) -> None:
        updated = tuple(sorted(symbols))
        with self._desired_lock:
            self._desired_symbols = updated

    def _get_symbols(self) -> tuple[str, ...]:
        with self._desired_lock:
            return self._desired_symbols

    def _run(self) -> None:
        self.thread_id = threading.get_ident()
        try:
            asyncio.run(self._serve())
        except BaseException as exc:
            self.error = exc

    async def _serve(self) -> None:
        state, stop = MarketState(), asyncio.Event()
        lag_task = asyncio.create_task(monitor_loop_lag(
            state, stop, self.diagnostics, self.market,
        ))
        connection_tasks: list[asyncio.Task[Any]] = []
        active_symbols: tuple[str, ...] = ()
        active_chunk_size = 0
        try:
            while not self._exit.is_set():
                desired = self._get_symbols()
                chunk_size = self.runtime.current.book_symbols_per_connection
                if desired != active_symbols or chunk_size != active_chunk_size:
                    for task in connection_tasks:
                        task.cancel()
                    await asyncio.gather(*connection_tasks, return_exceptions=True)
                    connection_tasks = []
                    state.book_connected_chunks[self.market].clear()
                    state.book_connected[self.market] = False
                    for key in list(state.errors):
                        if key.startswith(f"{self.market}_book_"):
                            state.errors.pop(key, None)
                    chunks = [
                        desired[index:index + chunk_size]
                        for index in range(0, len(desired), chunk_size)
                    ]
                    base_url = SPOT_BOOK_BASE if self.market == "spot" else FUTURES_BOOK_BASE
                    connection_tasks = [
                        asyncio.create_task(consume_book_chunk(
                            base_url + "/".join(
                                f"{symbol.lower()}@bookTicker" for symbol in chunk
                            ),
                            self.market, str(index), state, stop, self.runtime,
                            self.pipeline, frozenset(chunk),
                        ))
                        for index, chunk in enumerate(chunks, 1)
                    ]
                    active_symbols, active_chunk_size = desired, chunk_size
                    self.diagnostics.emit(
                        "subscription_rebuild", component=self.market,
                        symbols=len(desired), connections=len(chunks),
                        symbols_per_connection=chunk_size,
                    )
                for task in connection_tasks:
                    if task.done():
                        task.result()
                        raise RuntimeError(f"{self.market} BBO连接任务意外停止")
                expected = len(connection_tasks)
                connected = (
                    expected > 0
                    and len(state.book_connected_chunks[self.market]) == expected
                )
                errors = {
                    key: value for key, value in state.errors.items()
                    if key.startswith(f"{self.market}_book") and value
                }
                with self._status_lock:
                    self._status = {
                        "connected": connected, "errors": errors,
                        "loop_lag_ms": state.loop_lag_ms,
                        "max_loop_lag_ms": state.max_loop_lag_ms,
                        "connections": expected,
                    }
                await asyncio.sleep(0.05)
        finally:
            stop.set()
            for task in connection_tasks:
                task.cancel()
            lag_task.cancel()
            await asyncio.gather(*connection_tasks, lag_task, return_exceptions=True)

    def status(self) -> dict[str, Any]:
        if self.error is not None:
            raise RuntimeError(f"{self.market} BBO网络线程退出") from self.error
        with self._status_lock:
            return {
                key: value.copy() if isinstance(value, dict) else value
                for key, value in self._status.items()
            }

    def close(self) -> None:
        self._exit.set()
        self._thread.join(timeout=8)
        if self._thread.is_alive():
            raise RuntimeError(f"{self.market} BBO网络线程未在8秒内退出")


class MarketNetworkWorker:
    """Own all sockets on a dedicated loop; publish latest state, never a backlog.

    Config objects are immutable and replaced atomically by ConfigManager.
    Mutable market dictionaries never cross thread boundaries without copying.
    """

    def __init__(self, filters: Any, runtime: Any, diagnostics_dir: Path | None = None) -> None:
        self.filters, self.runtime = filters, runtime
        self.diagnostics_dir = diagnostics_dir
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
        diagnostics = NetworkDiagnostics(
            self.diagnostics_dir / "network_pipeline.jsonl" if self.diagnostics_dir else None,
            self.runtime.current.network_log_max_bytes,
        )
        pipeline = LatestQuotePipeline(self.runtime, diagnostics)
        spot_ingress = BBOIngressWorker("spot", self.runtime, pipeline, diagnostics)
        futures_ingress = BBOIngressWorker("futures", self.runtime, pipeline, diagnostics)
        spot_ingress.start()
        futures_ingress.start()
        diagnostics.emit(
            "pipeline_start",
            coalesce_interval_ms=self.runtime.current.raw_coalesce_interval_ms,
            book_symbols_per_connection=self.runtime.current.book_symbols_per_connection,
        )
        state.set_pairing_config(self.filters.current)
        tasks = [
            asyncio.create_task(monitor_loop_lag(state, stop, diagnostics, "ticker")),
            asyncio.create_task(consume_tickers(
                SPOT_TICKER_STREAM, "spot", state, stop, diagnostics
            )),
            asyncio.create_task(consume_tickers(
                FUTURES_TICKER_STREAM, "futures", state, stop, diagnostics
            )),
            asyncio.create_task(consume_funding(
                FUNDING_STREAM, state, stop, diagnostics
            )),
        ]
        spot_status = futures_status = {
            "connected": False, "errors": {}, "loop_lag_ms": 0.0,
            "max_loop_lag_ms": 0.0, "connections": 0,
        }
        try:
            while not self._exit.is_set():
                for task in tasks:
                    if task.done():
                        task.result()
                        raise RuntimeError("网络任务意外退出")
                if pipeline.error:
                    raise RuntimeError(f"BBO解析线程退出: {pipeline.error}")
                state.set_pairing_config(self.filters.current)
                state.max_transport_lag_ms = self.runtime.current.max_transport_lag_ms
                spot_ingress.set_symbols(
                    state.book_symbols("spot", self.filters.current)
                )
                futures_ingress.set_symbols(
                    state.book_symbols("futures", self.filters.current)
                )
                spot_status = spot_ingress.status()
                futures_status = futures_ingress.status()
                state.book_connected["spot"] = spot_status["connected"]
                state.book_connected["futures"] = futures_status["connected"]
                for key in list(state.errors):
                    if key.startswith("spot_book_") or key.startswith("futures_book_"):
                        state.errors.pop(key, None)
                state.errors.update(spot_status["errors"])
                state.errors.update(futures_status["errors"])
                pipeline.apply_latest(state)
                if diagnostics.error:
                    state.errors["network_log"] = diagnostics.error
                snapshot = {name: getattr(state, name).copy() for name in (
                    "spot_volumes", "futures_volumes", "spot_books", "futures_books",
                    "funding_quotes",
                    "ticker_connected", "book_connected", "ticker_last_update",
                    "book_last_update", "errors",
                )}
                snapshot.update(funding_connected=state.funding_connected,
                                funding_last_update=state.funding_last_update,
                                pipeline_metrics=pipeline.metrics(),
                                ticker_loop_lag_ms=state.loop_lag_ms,
                                ticker_max_loop_lag_ms=state.max_loop_lag_ms,
                                spot_network_loop_lag_ms=spot_status["loop_lag_ms"],
                                spot_network_max_loop_lag_ms=spot_status["max_loop_lag_ms"],
                                futures_network_loop_lag_ms=futures_status["loop_lag_ms"],
                                futures_network_max_loop_lag_ms=futures_status["max_loop_lag_ms"],
                                spot_network_connections=spot_status["connections"],
                                futures_network_connections=futures_status["connections"])
                with self._lock:
                    self._snapshot = snapshot
                await asyncio.sleep(0.05)
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(
                asyncio.to_thread(spot_ingress.close),
                asyncio.to_thread(futures_ingress.close),
            )
            await asyncio.to_thread(pipeline.close)
            diagnostics.emit(
                "pipeline_stop", **pipeline.metrics(),
                ticker_loop_peak_ms=round(state.max_loop_lag_ms, 1),
                spot_loop_peak_ms=round(spot_status.get("max_loop_lag_ms", 0), 1),
                futures_loop_peak_ms=round(futures_status.get("max_loop_lag_ms", 0), 1),
            )
            await asyncio.to_thread(diagnostics.close)

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
