#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
下载前2个UM交易币对的最近5天kline和metrics（merits）数据的工具
"""

import asyncio
import os
from pathlib import Path
from datetime import datetime, timedelta
from itertools import chain
from typing import Dict, List

# 全局数据路径常量
BASE_DATA_PATH = "data"

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import create_aws_client_from_config
from bhds.aws.downloader import AwsDownloader
from bhds.aws.checksum import ChecksumVerifier
from bhds.aws.local import AwsDataFileManager


def get_recent_days(days: int = 5) -> List[str]:
    """
    获取最近几天的日期字符串列表（YYYY-MM-DD格式）
    
    Args:
        days: 天数
    
    Returns:
        日期字符串列表，按从旧到新排序
    """
    # 使用UTC时间以避免时区问题
    today = datetime.utcnow()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days-1, -1, -1)]


async def get_top_symbols(http_proxy: str, limit: int = 2) -> List[str]:
    """
    获取UM交易对的前N个币对
    
    Args:
        http_proxy: HTTP代理
        limit: 返回的币对数量
    
    Returns:
        币对列表
    """
    # 使用kline数据类型来获取币对列表
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        client = create_aws_client_from_config(
            trade_type=TradeType.um_futures,
            data_type=DataType.kline,
            data_freq=DataFrequency.daily,
            time_interval="1m",
            session=session,
            http_proxy=http_proxy
        )
        symbols = await client.list_symbols()
        return symbols[:limit]


async def filter_recent_files(files: List[Path], date_list: List[str]) -> List[Path]:
    """
    筛选出最近几天的文件
    
    Args:
        files: 文件路径列表
        date_list: 日期字符串列表（YYYY-MM-DD格式）
    
    Returns:
        筛选后的文件路径列表
    """
    # 过滤掉CHECKSUM文件，只处理zip文件
    zip_files = [f for f in files if f.name.endswith('.zip')]
    
    filtered_files = []
    
    # 先输出所有获取到的zip文件，方便调试
    print(f"  共获取到 {len(zip_files)} 个zip文件")
    if zip_files:
        print(f"  最新的5个文件: {', '.join([f.name for f in sorted(zip_files)[-5:]])}")
    
    # 改进的日期匹配逻辑
    for file_path in zip_files:
        # 文件名格式：SYMBOL-TIME_INTERVAL-YYYY-MM-DD.zip
        # 或：SYMBOL-YYYY-MM-DD.zip
        filename = file_path.name
        
        # 提取文件名中的日期部分
        import re
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
        if date_match:
            file_date = date_match.group()
            if file_date in date_list:
                filtered_files.append(file_path)
    
    # 如果没有找到匹配日期的文件，尝试获取最新的文件
    if not filtered_files and zip_files:
        print(f"  未找到匹配日期的文件，将获取最新的文件")
        # 按文件名排序，通常最新的文件会在后面
        sorted_files = sorted(zip_files)
        # 返回最新的文件
        filtered_files = [sorted_files[-1]]
    
    return filtered_files


async def download_data(
    http_proxy: str,
    symbols: List[str],
    data_dir: Path,
    time_interval: str = "1m",
    days: int = 5
) -> None:
    """
    下载指定币对的kline和metrics数据
    
    Args:
        http_proxy: HTTP代理
        symbols: 币对列表
        data_dir: 数据保存目录
        time_interval: K线时间间隔
        days: 下载天数
    """
    # 创建数据目录
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 获取最近5天的日期
    recent_days = get_recent_days(days)
    print(f"📅 将下载最近 {days} 天的数据: {', '.join(recent_days)}")
    print(f"🌍 当前UTC时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 创建下载器
    downloader = AwsDownloader(local_dir=data_dir, http_proxy=http_proxy, verbose=True)
    verifier = ChecksumVerifier(delete_mismatch=False)
    
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        # 下载K线数据
        print("\n📊 开始下载K线数据...")
        kline_client = create_aws_client_from_config(
            trade_type=TradeType.um_futures,
            data_type=DataType.kline,
            data_freq=DataFrequency.daily,
            time_interval=time_interval,
            session=session,
            http_proxy=http_proxy
        )
        
        # 批量获取所有K线文件
        kline_files_map = await kline_client.batch_list_data_files(symbols)
        
        # 筛选最近5天的K线文件
        filtered_kline_files = []
        for symbol, files in kline_files_map.items():
            recent_files = await filter_recent_files(files, recent_days)
            filtered_kline_files.extend(recent_files)
            print(f"   {symbol}: {len(recent_files)}个K线文件")
        
        if filtered_kline_files:
            print(f"📥 总共下载 {len(filtered_kline_files)} 个K线文件")
            await downloader.aws_download(filtered_kline_files)
        else:
            print("⚠️  没有找到符合条件的K线文件")
        
        # 下载Metrics数据（作为merits的替代）
        print("\n📊 开始下载Metrics数据...")
        metrics_client = create_aws_client_from_config(
            trade_type=TradeType.um_futures,
            data_type=DataType.metrics,
            data_freq=DataFrequency.daily,
            time_interval=None,  # metrics不需要time_interval
            session=session,
            http_proxy=http_proxy
        )
        
        # 批量获取所有Metrics文件
        metrics_files_map = await metrics_client.batch_list_data_files(symbols)
        
        # 筛选最近5天的Metrics文件
        filtered_metrics_files = []
        for symbol, files in metrics_files_map.items():
            recent_files = await filter_recent_files(files, recent_days)
            filtered_metrics_files.extend(recent_files)
            print(f"   {symbol}: {len(recent_files)}个Metrics文件")
        
        if filtered_metrics_files:
            print(f"📥 总共下载 {len(filtered_metrics_files)} 个Metrics文件")
            await downloader.aws_download(filtered_metrics_files)
        else:
            print("⚠️  没有找到符合条件的Metrics文件")
    
    # 验证文件
    print("\n🔍 开始验证文件...")
    all_unverified_files = []
    for symbol in symbols:
        # 验证K线文件
        kline_symbol_dir = data_dir / f"{BASE_DATA_PATH}/{TradeType.um_futures.value}/daily/{DataType.kline.value}/{symbol}/{time_interval}"
        if kline_symbol_dir.exists():
            manager = AwsDataFileManager(kline_symbol_dir)
            all_unverified_files.extend(manager.get_unverified_files())
        
        # 验证Metrics文件
        metrics_symbol_dir = data_dir / f"{BASE_DATA_PATH}/{TradeType.um_futures.value}/daily/{DataType.metrics.value}/{symbol}"
        if metrics_symbol_dir.exists():
            manager = AwsDataFileManager(metrics_symbol_dir)
            all_unverified_files.extend(manager.get_unverified_files())
    
    if all_unverified_files:
        results = verifier.verify_files(all_unverified_files)
        print(f"✅ 验证完成: {results['success']} 个成功, {results['failed']} 个失败")
    else:
        print("✅ 所有文件都已验证过")
    
    print("\n🎉 数据下载完成！")


async def test_download() -> None:
    """测试下载功能，只下载少量文件"""
    # 配置
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy") or "http://127.0.0.1:7890"
    data_dir = Path("d:/Codes/binance_datatool-main/data")  # 数据保存目录
    time_interval = "1m"  # K线时间间隔
    days = 1  # 测试时只下载1天的数据
    
    print("🚀 启动Binance UM交易对数据下载工具")
    print(f"📁 数据保存目录: {data_dir}")
    print(f"🌐 HTTP代理: {http_proxy if http_proxy else '无'}")
    
    # 获取前2个UM交易对
    print("\n🔍 获取交易对列表...")
    try:
        symbols = await get_top_symbols(http_proxy, limit=4)
        symbols = ["BTCUSDT", "ETHUSDT"]

        print(f"📋 选择的交易对: {', '.join(symbols)}")
    except Exception as e:
        print(f"❌ 获取交易对列表失败: {e}")
        return
    
    # 下载数据
    try:
        await download_data(http_proxy, symbols, data_dir, time_interval, days)
    except Exception as e:
        print(f"❌ 下载数据失败: {e}")
        import traceback
        traceback.print_exc()


def main() -> None:
    """主函数"""
    try:
        asyncio.run(test_download())
    except KeyboardInterrupt:
        print("\n⏹️  下载已被用户中断")
    except Exception as e:
        print(f"\n❌ 程序运行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()