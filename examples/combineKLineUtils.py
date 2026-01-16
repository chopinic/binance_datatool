import sys
import os
import asyncio

# 将项目根目录添加到sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import src.BinanceData as BinanceData

async def test():
    BinanceData.logLevel = "debug"
    stDate =  "2025-03-01"
    edDate =  "2025-03-10"
    symbols = await BinanceData.get_all_um_symbols()

    testSymbols = ["BTCUSDT","ETHUSDT"]
    kline_df = await BinanceData.get_kline_dataframe(
        symbol="BTCUSDT",
        start_date=stDate,
        end_date=edDate,
        frequency="5m"
    )

    metrics_df = await BinanceData.get_metrics_dataframe(
        symbol="BTCUSDT",
        start_date=stDate,
        end_date=edDate,
    )

    merged_df, warning_dict = BinanceData.merge_kline_and_metrics(kline_df, metrics_df, "BTCUSDT")

if __name__ == "__main__":
    asyncio.run(test())
