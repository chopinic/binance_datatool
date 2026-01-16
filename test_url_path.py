#!/usr/bin/env python3
"""
Test script to verify URL path construction with enum values.
"""

from bdt_common.enums import DataType, TradeType, DataFrequency
from bhds.aws.path_builder import AwsKlinePathBuilder, AwsPathBuilder

# Test standard path builder
print("Testing standard AwsPathBuilder...")
path_builder = AwsPathBuilder(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    data_type=DataType.funding_rate
)

symbol_dir = path_builder.get_symbol_dir("BTCUSDT")
print(f"  Symbol dir: {symbol_dir}")
print(f"  Path as string: {str(symbol_dir)}")

# Test kline path builder
print("\nTesting AwsKlinePathBuilder...")
kline_path_builder = AwsKlinePathBuilder(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    time_interval="1m"
)

kline_symbol_dir = kline_path_builder.get_symbol_dir("BTCUSDT")
print(f"  Kline symbol dir: {kline_symbol_dir}")
print(f"  Path as string: {str(kline_symbol_dir)}")

# Check if enums are being converted to strings correctly
print("\nTesting enum string conversion...")
print(f"  TradeType.spot as string: '{str(TradeType.spot)}'")
print(f"  DataFrequency.daily as string: '{str(DataFrequency.daily)}'")
print(f"  DataType.kline as string: '{str(DataType.kline)}'")
print(f"  DataType.agg_trade as string: '{str(DataType.agg_trade)}'")
print(f"  DataType.funding_rate as string: '{str(DataType.funding_rate)}'")

# Test with futures
print("\nTesting futures paths...")
futures_path_builder = AwsPathBuilder(
    trade_type=TradeType.um_futures,
    data_freq=DataFrequency.monthly,
    data_type=DataType.liquidation_snapshot
)

futures_symbol_dir = futures_path_builder.get_symbol_dir("BTCUSDT")
print(f"  Futures symbol dir: {futures_symbol_dir}")
print(f"  Path as string: {str(futures_symbol_dir)}")

print("\nPath construction test completed!")