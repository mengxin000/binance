# Binance 现货－永续基差波动筛选器

从 Binance 现货与 USDT-M 永续全市场中筛选流动性达标且基差波动空间足够的交易对。可选正基差条件；程序只统计，不需要 API Key、不下单，也不估算收益。

直接在 Python IDE 中运行 `main.py`。

## 数据口径

24小时 ticker 只用于现货与永续成交额初筛。价格使用双方实时 bookTicker：

```text
spot_mid = (spot_bid + spot_ask) / 2
futures_mid = (futures_bid + futures_ask) / 2
basis_bps = (futures_mid / spot_mid - 1) × 10000
```

bookTicker 持续更新内存报价；统计循环每200ms读取一次双方最新mid，因此每个交易对每秒最多形成5个同步基差样本。

基差均值与标准差使用 Welford 在线增量算法，不需要把全部样本保存在内存。

```text
new_mean = old_mean + (current_basis - old_mean) / new_sample_count
```

`min_four_sigma_bps` 是研究初筛：只有 `4σ` 大于该值才进入榜单。`expansion_threshold_bps` 是独立的事件阈值：当前基差上穿 `μ + expansion_threshold_bps` 时记录一次扩差；回落至 `μ` 后复位。分位数范围使用最近 `quantile_sample_size` 个BBO基差样本的 P5～P95。

## 配置

`filter_config.json`：

```json
{
  "min_spot_volume": 10000000,
  "min_futures_volume": 10000000,
  "top": 10,
  "positive_basis_only": true,
  "min_spot_price": 0.1,
  "quote_assets": ["USDT", "USDC"]
}
```

`min_spot_price` 使用现货实时 bookTicker 中间价。`quote_assets` 是允许的报价币种；同名交易对只在现货和永续的完整名称完全一致时匹配，例如 `BTCUSDC` 只匹配 `BTCUSDC`，不会与 `BTCUSDT` 交叉配对。

`statistics_config.json`：

```json
{
  "min_samples": 300,
  "min_four_sigma_bps": 5.0,
  "expansion_threshold_bps": 5.0,
  "quantile_sample_size": 3600,
  "bbo_record_interval_ms": 1000
}
```

`runtime_config.json`：

```json
{
  "sample_interval_ms": 200,
  "display_refresh_seconds": 1.0,
  "stale_seconds": 10.0,
  "persist_interval_seconds": 60.0,
  "data_directory": "data"
}
```

配置可以在运行期间保存并热加载；数据目录在启动时确定，修改路径后需重启。`positive_basis_only` 为 `true` 时只显示并保存基差均值为正的交易对；设为 `false` 时不限制基差均值正负。

## 按交易对持久化

每个入选交易对使用独立目录，例如：

```text
data/
  BNBUSDT/
    state.json
    opportunities.jsonl
  WLFIUSDT/
    state.json
    opportunities.jsonl
```

`state.json` 每60秒覆盖一次，保存采样数、统计时长、基差均值、方差累计量、当前基差和扩差次数等恢复状态。启动时读取该文件继续累计。

`opportunities.jsonl` 只在基差向上穿越事件阈值时追加一行，记录当时的现货中间价、永续中间价、μ、σ、当前基差、偏离和扩差序号。没有扩差时不会创建或写入该文件。

`bbo_above_threshold.jsonl` 记录每条高于 `μ + expansion_threshold_bps` 的 BBO 推送；该文件位于相同交易对目录中。

终端表格按 Unicode 实际显示宽度补齐，中英文、`μ`、`σ` 与全角范围符号混排时仍保持列对齐。

旧的SQLite数据库保留在 `data/`，但v2.0不再读取或写入。
