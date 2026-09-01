# Binance 现货－永续基差波动筛选器

从 Binance 现货与 USDT-M 永续全市场中筛选流动性达标且基差波动空间足够的交易对。可选正基差条件；程序只统计，不需要 API Key、不下单，也不估算收益。

直接在 Python IDE 中运行 `main.py`。需要长期运行时启动 `guardian.py`；主程序异常退出后，守护程序会等待3秒并自动重启，按 Ctrl+C 可同时停止。运行期间按 `K` 在“现货－永续”和“USDT永续－USDC永续”两个独立页面之间切换。

## 数据口径

24小时 ticker 只用于现货与永续成交额初筛。价格使用双方实时 bookTicker：

每个统计周期只读取各市场当前最新的 bookTicker。所有腿的本机接收年龄和接收时间差都达标时，基差才进入均值、总体标准差和分位数统计；否则跳过本次采样。被拒绝的基差会与2秒内出现的下一条合格基差进行复核，变化至少3bp时限频写入 `data/diagnostics/basis_recheck.jsonl`。

跨现货和永续流的对齐使用本机 `time.monotonic()` 接收时间；同时必须通过连接传输延迟检查。永续bookTicker直接使用每条消息的交易所事件时间 `E`；现货bookTicker没有 `E`，因此每条现货组合连接额外订阅一个同连接 `BTCUSDT@ticker` 作为带 `E` 的传输心跳（只检查连接积压，不参与定价）。本机接收时间看似新、但交易所事件时间落后超过 `max_transport_lag_ms` 时，程序立即关闭该子连接以清空TCP/WebSocket积压，并独立指数退避重连；超时消息不会进入最新报价槽或统计窗口。

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

bookTicker 持续覆盖每个交易对容量为1的最新报价槽；统计循环按 `sample_interval_ms`（当前1000ms）读取一次双方最新mid。BBO推送频率不会改变滑动统计的样本权重，也不存在等待消费的BBO业务队列。

每条路线只维护一个总样本滑动队列。μ、σ使用滚动和，运行范围使用单调队列，更新均为均摊O(1)。长窗、短窗分别截取总队列尾部指定数量的样本；P5～P95由单独线程每隔 `quantile_refresh_seconds` 计算并缓存，终端只读取缓存。窗口未装满时直接使用已有样本，不设置覆盖率或预热门槛。

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
  "data_directory": "data"
}
```

配置可以在运行期间保存并热加载；数据目录和后台写盘队列容量在启动时确定，修改后需重启。BBO始终覆盖最新槽位；诊断和高频BBO写盘在队列满时允许丢弃，机会记录与状态会短暂等待最多50ms。每条bookTicker连接最多订阅 `book_symbols_per_connection` 个交易对，超过后自动拆成多条连接；每条子连接独立进行1～30秒指数退避重连，不会因单条连接断开而重启其余连接。`quote_assets` 或跨币种配置热加载后，不再允许的ticker与bookTicker缓存会立即清除，新增币种等待下一条24小时ticker更新后自动建立路线。终端输出使用容量为1的画面槽，控制台变慢时只丢旧画面。`positive_basis_only` 为 `true` 时只显示并保存基差均值为正的交易对；设为 `false` 时不限制基差均值正负。

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
