import asyncio
import BinanceData
import bdt_common.polars_utils

async def test():
    symbol = "UNTUSDT"
    st = "2024-08-27"
    ed = "2025-02-14"
    kline_df = await BinanceData.get_kline_dataframe(symbol,st,ed,frequency="5m")
    metrics_df = await BinanceData.get_metrics_dataframe( symbol=symbol, start_date=st, end_date=ed)
    merged_df, warning_dict = BinanceData.merge_kline_and_metrics(kline_df, metrics_df, symbol)
    merged_df.to_csv("./temp.csv")
    plotData(merged_df)
    print(warning_dict)
    
    # print("指标数据获取成功，共", len(metrics_df), "行")

if __name__ == "__main__":
    asyncio.run(test())
