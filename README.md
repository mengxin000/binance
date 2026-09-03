# Binance 现货－永续基差波动筛选器

从 Binance 现货与 USDT-M 永续全市场中筛选流动性达标且基差波动空间足够的交易对。可选正基差条件；程序只统计，不需要 API Key、不下单，也不估算收益。

直接在 Python IDE 中运行 `main.py`。需要长期运行时启动 `guardian.py`；主程序异常退出后，守护程序会等待3秒并自动重启，按 Ctrl+C 可同时停止。运行期间按 `K` 在“现货－永续”和“USDT永续－USDC永续”两个独立页面之间切换。

## 数据口径

24小时 ticker 只用于现货与永续成交额初筛。价格使用双方实时 bookTicker：

每个统计周期只读取各市场当前最新的 bookTicker。所有腿的本机接收年龄和接收时间差都达标时，基差才进入均值、总体标准差和分位数统计；否则跳过本次采样。被拒绝的基差会与2秒内出现的下一条合格基差进行复核，变化至少3bp时限频写入 `data/diagnostics/basis_recheck.jsonl`。

跨市场对齐使用网络线程中完整 WebSocket 消息解帧完成时记录的 `time.monotonic()` 时间，解析及快照传递时不重新打时间戳。现货没有交易所事件时间，不再借用ticker心跳或订阅额外ticker。永续另使用自身的 `E` 检查传输延迟，超过 `max_transport_lag_ms` 时独立重连；此检查要求本机系统时间准确。现货的本机接收时间不能证明其交易所侧生成时间。

```text
spot_mid = (spot_bid + spot_ask) / 2
futures_mid = (futures_bid + futures_ask) / 2
basis_bps = (futures_mid / spot_mid - 1) × 10000
```

当现货与永续使用不同报价币种时，程序订阅现货 `USDCUSDT@bookTicker`，先把永续中间价折算成现货报价币种。比如 `BTCUSDT` 现货与 `BTCUSDC` 永续：

```text
normalized_futures_mid = BTCUSDC futures_mid × USDCUSDT spot_mid
basis_bps = (normalized_futures_mid / BTCUSDT spot_mid - 1) × 10000
```

反方向 `BTCUSDC` 现货与 `BTCUSDT` 永续使用除法折算。转换盘口也受行情新鲜度检查；转换价格过期时不产生样本。

全部市场网络收发运行在独立线程/事件循环。每条 bookTicker 连接通过 `LatestBBOConnection` 在完整消息解帧后直接写入有界窗口，不经过 websockets 默认的 Assembler 数据帧队列，避免该队列高水位触发 `pause_reading()`。分片必须重组完整才入队；Ping/Pong、Close 和协议帧校验仍交给库处理。此适配仅用于完整 BBO 快照，不能用于增量深度流；依赖限定为 websockets 15.x。

窗口容量由 `raw_message_capacity` 控制（配置当前1024，缺省512），满时自动丢弃最旧完整消息，不解析被丢弃消息的JSON。解析协程每批最多 `decode_batch_size` 条（配置当前128，缺省32）、约2ms后让出网络事件循环；已过期的消息在JSON解析前丢弃。解析后按交易对覆盖最新报价槽。网络线程每50ms发布一次最新快照，主线程只取最新一份，无跨线程消息积压。统计循环按 `sample_interval_ms`（当前1000ms）读取双方有效mid；不合格的年龄/时间差样本跳过、不回补。50ms发布周期不是统计采样周期。

该设计消除了 BBO 库内数据帧队列背压，并将统计/绘制与网络事件循环隔离，但不是网络无延迟保证：TCP/TLS、解帧吞吐、CPU饱和、Python GIL竞争或线路停顿仍可能影响接收，因此保留报价过期与永续E延迟保护。终端分别显示主循环和网络循环延迟。

每条路线只维护一个总样本滑动队列。μ、σ使用滚动和，运行范围使用单调队列，更新均为均摊O(1)。P5～P95由NumPy后台精确计算（linear插值）：每轮逐交易对复制必要的长窗尾部，再让出事件循环，不一次复制全部交易对。每隔 `quantile_refresh_seconds` 启动一轮，未结束不排队。终端格式化和输出均在独立线程，使用轻量展示快照。窗口未装满时使用已有样本。

```text
μ = rolling_sum / current_sample_count
σ² = rolling_sum_of_squares / current_sample_count - μ²
```

`sigma_multiplier × σ` 仍然是研究初筛。开仓机会与平仓机会是两个独立往返事件：基差达到上阈值后回落至μ记一次开仓机会；达到下阈值后回升至μ记一次平仓机会。排序首先使用 `min(开仓机会, 平仓机会)`，然后使用两者总数。

## 配置

`filter_config.json`：

```json
{
  "min_spot_volume": 10000000,
  "min_futures_volume": 10000000,
  "top": 10,
  "positive_basis_only": true,
  "min_spot_price": 0.1,
  "quote_assets": ["USDT", "USDC"],
  "cross_quote_pairings": [
    ["USDT", "USDC"],
    ["USDC", "USDT"]
  ]
}
```

`min_spot_price` 使用现货实时 bookTicker 中间价。`quote_assets` 控制同报价币种配对；`cross_quote_pairings` 明确允许的跨币种方向，每项顺序固定为 `[现货报价币种, 永续报价币种]`。上面的配置同时生成 `BTCUSDT/BTCUSDC` 与 `BTCUSDC/BTCUSDT`，不存在任意币种的隐式组合。

`statistics_config.json`：

```json
{
  "total_window_samples": 432000,
  "long_window_samples": 144000,
  "short_window_samples": 54000,
  "quantile_low": 0.05,
  "quantile_high": 0.95,
  "sigma_multiplier": 2.0,
  "min_k_sigma_bps": 3.0,
  "open_threshold_bps": 7.0,
  "close_threshold_bps": -5.0,
  "bbo_record_interval_ms": 1000
}
```

三个窗口样本数、分位数、k值和上下事件阈值均支持热加载。必须满足 `short_window_samples <= long_window_samples <= total_window_samples`。

`futures_futures_statistics_config.json` 使用相同字段，独立控制USDT永续－USDC永续页面；两套参数、滑动样本、机会计数和排名互不影响。期货间价差统一定义为：

```text
USDC永续折算价 = USDC永续mid × USDCUSDT现货mid
spread_bps = (USDC永续折算价 / USDT永续mid - 1) × 10000
```

`runtime_config.json`：

```json
{
  "sample_interval_ms": 1000,
  "display_refresh_seconds": 1.0,
  "stale_seconds": 10.0,
  "quote_max_age_ms": 1000,
  "quote_match_tolerance_ms": 200,
  "max_transport_lag_ms": 2000,
  "persist_interval_seconds": 60.0,
  "quantile_refresh_seconds": 15.0,
  "background_write_queue_size": 10000,
  "book_symbols_per_connection": 80,
  "raw_message_capacity": 512,
  "decode_batch_size": 32,
  "data_directory": "data"
}
```

配置文件在后台读取并热加载。数据目录、写盘队列容量修改需重启；原始消息容量变化会重建子连接，解析批量立即热加载。写盘队列满时所有新任务均不等待并计数丢弃（包括机会记录，因此落盘不保证完整）；错误和丢弃数在终端显示。每条bookTicker连接最多订阅 `book_symbols_per_connection` 个合约，独立退避重连。报价币配置变化时清理不允许的缓存。终端显示解帧后缓冲区占用、溢出/过期丢弃数和主/网络事件循环延迟；原始消息计数按本轮连接统计，重连后重置。丢消息是正常过载保护，并不代表保证每个交易对都有样本。`positive_basis_only=true` 仅展示和保存均值为正的交易对。

接收链路回归测试：`python -m unittest test_main test_network -v`。`test_network.py` 使用真实本地 WebSocket 服务，覆盖不消费时的有界溢出、最新消息、分片重组、Ping/Pong、远端关闭，以及主线程暂停时网络仍更新；不是 Binance 现场网络压测。

## 按交易对持久化

每个入选交易对使用独立目录，例如：

```text
data/
  spot_futures/
    BNBUSDT/
      state.json
      opportunities.jsonl
    BTCUSDT__BTCUSDC/
      state.json
      opportunities.jsonl
  futures_futures/
    BTCUSDT__BTCUSDC/
      state.json
      opportunities.jsonl
```

`state.json` 每60秒覆盖一次，只保存当前统计摘要供查看。滑动样本队列不会持久化，程序启动后从空窗口重新统计，也不会从旧的 `state.json` 恢复。

同币种路线继续使用原交易对作为目录名；跨币种路线使用 `现货交易对__永续交易对`，因此四种路线的统计互不混合。

`opportunities.jsonl` 在完整的上侧或下侧往返事件完成时追加一行，通过 `event` 区分 `open_opportunity` 和 `close_opportunity`。

`bbo_above_threshold.jsonl` 记录高于 `μ + open_threshold_bps` 的BBO推送，并受写入间隔限制；该文件位于相同交易对目录中。

终端表格按 Unicode 实际显示宽度补齐，中英文、`μ`、`σ` 与全角范围符号混排时仍保持列对齐。

旧的SQLite数据库保留在 `data/`，但v2.0不再读取或写入。
