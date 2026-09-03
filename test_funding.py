import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_funding_rate(symbol, limit=1000):
    """获取单个交易对的历史资金费率"""
    resp = requests.get(
        "https://fapi.binance.com/fapi/v1/fundingRate",
        params={"symbol": symbol, "limit": limit},
    )
    return resp.json()

# 示例：获取BTCUSDT的数据
btc_data = fetch_funding_rate("BTCUSDT", 30)

# 将数据转换为DataFrame，便于处理
df = pd.DataFrame(btc_data)
df['fundingRate'] = df['fundingRate'].astype(float)
df['fundingTime'] = pd.to_datetime(df['fundingTime'], unit='ms')
# print(df.head())


def get_usdt_futures_symbols():
    """获取所有U本位永续合约的交易对名称"""
    resp = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo")
    data = resp.json()
    # 筛选出状态为 'TRADING' 的交易对
    symbols = [
        s['symbol'] for s in data['symbols']
        if s['status'] == 'TRADING' and s['contractType'] == 'PERPETUAL'
    ]
    return symbols


# 注意：以下循环会请求大量API，建议仅用于测试
# 实际使用时应控制请求频率，或仅获取你关注的交易对列表
all_symbols = get_usdt_futures_symbols()
# all_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']  # 先测试少量交易对

results = []

test_symbols = all_symbols[:20]

for symbol in test_symbols:
    try:
        data = fetch_funding_rate(symbol, 30)  # 获取最近30条记录

        # 数据清洗，转换为浮点数
        rates = [float(item['fundingRate']) for item in data]

        # 筛选条件：30条记录必须都存在，且全部大于0
        if len(rates) == 30 and all(rate > 0 for rate in rates):
            avg_rate = sum(rates) / len(rates)
            results.append({
                "symbol": symbol,
                "avg_funding_rate": avg_rate,
                "positive_count": len([r for r in rates if r > 0])
            })
    except Exception as e:
        print(f"处理 {symbol} 时出错: {e}")

# 按平均费率降序排序（费率高的排在前面）
sorted_results = sorted(results, key=lambda x: x['avg_funding_rate'], reverse=True)

# 打印结果
for res in sorted_results:
    print(f"交易对: {res['symbol']}, 30日平均正费率: {res['avg_funding_rate']:.6f}")