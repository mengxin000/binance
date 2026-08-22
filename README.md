# Binance 现货－永续基差波动筛选器

从 Binance 现货与 USDT-M 永续全市场中筛选流动性达标且基差波动空间足够的交易对。可选正基差条件；程序只统计，不需要 API Key、不下单，也不估算收益。

直接在 Python IDE 中运行 `main.py`。需要长期运行时启动 `guardian.py`；主程序异常退出后，守护程序会等待3秒并自动重启，按 Ctrl+C 可同时停止。运行期间按 `K` 在“现货－永续”和“USDT永续－USDC永续”两个独立页面之间切换。

## 数据口径

24小时 ticker 只用于现货与永续成交额初筛。价格使用双方实时 bookTicker：

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

bookTicker 持续更新内存报价；统计循环每200ms读取一次双方最新mid，因此每个交易路线每秒最多形成5个固定频率样本。BBO推送频率不会改变滑动统计的样本权重。

每条路线只维护一个总样本滑动队列。μ、σ和运行范围使用总队列；长窗、短窗分别截取总队列尾部指定数量的样本计算P5～P95。窗口未装满时直接使用已有样本，不设置覆盖率或预热门槛。

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
  "sample_interval_ms": 200,
  "display_refresh_seconds": 1.0,
  "stale_seconds": 10.0,
  "persist_interval_seconds": 60.0,
  "data_directory": "data",
  "bbo_queue_maxsize": 20000
}
```

配置可以在运行期间保存并热加载；数据目录和BBO队列容量在启动时确定，修改后需重启。队列满时程序丢弃最旧快照并保留最新行情，终端显示当前队列长度和累计丢弃数。`positive_basis_only` 为 `true` 时只显示并保存基差均值为正的交易对；设为 `false` 时不限制基差均值正负。

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
