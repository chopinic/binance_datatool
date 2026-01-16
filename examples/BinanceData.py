#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
整合功能：
1. 获取所有交易对
2. 给单个货币对和时间段，检测并下载K线、生成全息、间隙检测修复并返回DataFrame
3. 给单个货币对和时间段，检测并下载metrics数据并返回DataFrame
"""

# 添加当前项目的src目录到Python路径
import sys
from pathlib import Path

# 获取当前脚本所在目录的父目录（项目根目录）
project_root = Path(__file__).resolve().parent.parent
# 添加src目录到Python路径
sys.path.insert(0, str(project_root / "src"))

import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import style

# 全局数据路径常量
BASE_DATA_PATH = "data"

# 设置matplotlib样式
style.use('seaborn-v0_8-darkgrid')

from bdt_common.constants import HTTP_TIMEOUT_SEC
from bdt_common.enums import DataFrequency, DataType, TradeType
from bdt_common.network import create_aiohttp_session
from bdt_common.polars_utils import execute_polars_batch
from bhds.aws.client import create_aws_client_from_config
from bhds.aws.downloader import AwsDownloader
from bhds.aws.checksum import ChecksumVerifier
from bhds.aws.local import AwsDataFileManager
from bhds.aws.parser import create_aws_parser
from bhds.holo_kline.merger import Holo1mKlineMerger
from bhds.holo_kline.gap_detector import HoloKlineGapDetector
from bhds.holo_kline.splitter import HoloKlineSplitter
from bhds.holo_kline.resampler import HoloKlineResampler


async def get_all_um_symbols(http_proxy: str) -> List[str]:
    """
    获取所有UM交易对
    
    Args:
        http_proxy: HTTP代理
    
    Returns:
        所有UM交易对列表
    """
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
        return symbols


def filter_files_by_time_range(files: List[Path], start_date: str, end_date: str) -> List[Path]:
    """
    筛选指定时间范围内的文件
    
    Args:
        files: 文件路径列表
        start_date: 起始日期（YYYY-MM-DD格式）
        end_date: 结束日期（YYYY-MM-DD格式）
    
    Returns:
        筛选后的文件路径列表
    """
    # 分离zip文件和CHECKSUM文件
    zip_files = [f for f in files if f.name.endswith('.zip')]
    checksum_files = [f for f in files if f.name == 'CHECKSUM']
    
    filtered_files = []
    
    # 先输出所有获取到的zip文件，方便调试
    print(f"  共获取到 {len(zip_files)} 个zip文件, {len(checksum_files)} 个CHECKSUM文件")
    if zip_files:
        print(f"  最新的5个文件: {', '.join([f.name for f in sorted(zip_files)[-5:]])}")
    
    # 日期匹配逻辑 - 处理zip文件
    for file_path in zip_files:
        # 文件名格式：SYMBOL-TIME_INTERVAL-YYYY-MM-DD.zip
        # 或：SYMBOL-YYYY-MM-DD.zip
        filename = file_path.name
        
        # 提取文件名中的日期部分
        import re
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
        if date_match:
            file_date = date_match.group()
            # 检查日期是否在指定范围内
            if start_date <= file_date <= end_date:
                filtered_files.append(file_path)
    
    # 将CHECKSUM文件添加到结果列表中
    filtered_files.extend(checksum_files)
    
    return filtered_files


async def _download_single_symbol_data(
    http_proxy: str,
    symbol: str,
    data_dir: Path,
    data_type: DataType,
    time_interval: str,
    start_date: str,
    end_date: str
) -> bool:
    """
    下载单个符号的指定类型数据
    
    Args:
        http_proxy: HTTP代理
        symbol: 币对
        data_dir: 数据保存目录
        data_type: 数据类型（kline或metrics）
        time_interval: K线时间间隔（metrics不需要）
        start_date: 起始日期
        end_date: 结束日期
    
    Returns:
        是否成功下载
    """
    try:
        downloader = AwsDownloader(local_dir=data_dir, http_proxy=http_proxy, verbose=True)
        verifier = ChecksumVerifier(delete_mismatch=False)
        
        async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
            # 创建客户端
            client = create_aws_client_from_config(
                trade_type=TradeType.um_futures,
                data_type=data_type,
                data_freq=DataFrequency.daily,
                time_interval=time_interval,
                session=session,
                http_proxy=http_proxy
            )
            
            # 获取文件列表
            files = await client.list_data_files(symbol)
            range_files = filter_files_by_time_range(files, start_date, end_date)
            
            if not range_files:
                print(f"⚠️  没有找到 {symbol} 的{data_type.value}文件")
                return False
            
            # 下载文件
            print(f"📥 下载 {symbol} 的{data_type.value}数据...")
            print(f"  下载文件列表 ({len(range_files)} 个):")
            for file in range_files:
                print(f"    - {file.name}")
            await downloader.aws_download(range_files)
            
            # 验证文件
            data_type_path = f"{BASE_DATA_PATH}/{TradeType.um_futures.value}/daily/{data_type.value}/{symbol}"
            if data_type == DataType.kline:
                data_type_path += f"/{time_interval}"
            
            symbol_dir = data_dir / data_type_path
            manager = AwsDataFileManager(symbol_dir)
            unverified_files = manager.get_unverified_files()
            
            if unverified_files:
                results = verifier.verify_files(unverified_files)
                print(f"✅ 验证完成: {results['success']} 个成功, {results['failed']} 个失败")
                if results['failed'] > 0:
                    print(f"❌ 验证失败详情: {results['errors']}")
                return results['failed'] == 0
            
            return True
    except Exception as e:
        print(f"❌ 下载 {symbol} 的{data_type.value}数据失败: {e}")
        return False

async def _check_data_exists(
    data_dir: Path,
    symbol: str,
    data_type: DataType,
    time_interval: str,
    start_date: str,
    end_date: str
) -> bool:
    """
    检查指定类型的数据是否已经下载并验证
    
    Args:
        data_dir: 数据保存目录
        symbol: 币对
        data_type: 数据类型
        time_interval: K线时间间隔
        start_date: 起始日期
        end_date: 结束日期
    
    Returns:
        数据是否存在且已验证
    """
    from datetime import datetime, timedelta
    import re
    
    data_type_path = f"{BASE_DATA_PATH}/{TradeType.um_futures.value}/daily/{data_type.value}/{symbol}"
    if data_type == DataType.kline:
        data_type_path += f"/{time_interval}"
    
    symbol_dir = data_dir / data_type_path
    if not symbol_dir.exists():
        return False
    
    manager = AwsDataFileManager(symbol_dir)
    verified_files = manager.get_verified_files()
    
    if not verified_files:
        return False
    
    # 转换日期字符串为datetime对象
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 收集所有已验证文件的日期
    verified_dates = set()
    for file_path in verified_files:
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', file_path.name)
        if date_match:
            verified_dates.add(date_match.group())
    
    # 找出时间范围内缺少的日期
    missing_dates = []
    current_dt = start_dt
    while current_dt <= end_dt:
        current_date_str = current_dt.strftime('%Y-%m-%d')
        if current_date_str not in verified_dates:
            missing_dates.append(current_date_str)
        current_dt += timedelta(days=1)
    
    # 如果有缺少的日期，打印日志并返回False
    if missing_dates:
        print(f"  ⚠️  缺少以下日期的数据: {', '.join(missing_dates[:5])}{'...' if len(missing_dates) > 5 else ''}")
        return False
    
    return True  # 所有日期的数据都存在

async def get_kline_dataframe(
    http_proxy: str,
    symbol: str,
    start_date: str,
    end_date: str,
    data_dir: Path,
    parsed_data_dir: Path,
    time_interval: str = "1m",
    frequency: str = "1h"
) -> pl.DataFrame:
    """
    获取单个货币对的K线数据DataFrame
    
    Args:
        http_proxy: HTTP代理
        symbol: 币对
        start_date: 起始日期
        end_date: 结束日期
        data_dir: 数据保存目录
        parsed_data_dir: 解析后的数据目录
        time_interval: K线时间间隔
        frequency: 重采样频率（如"1h", "4h", "1d"等）
    
    Returns:
        K线数据的DataFrame
    """
    # 检查数据是否已下载
    data_exists = await _check_data_exists(
        data_dir=data_dir,
        symbol=symbol,
        data_type=DataType.kline,
        time_interval=time_interval,
        start_date=start_date,
        end_date=end_date
    )
    
    if not data_exists:
        # 下载数据
        await _download_single_symbol_data(
            http_proxy=http_proxy,
            symbol=symbol,
            data_dir=data_dir,
            data_type=DataType.kline,
            time_interval=time_interval,
            start_date=start_date,
            end_date=end_date
        )
    else:
        print(f"✅ {symbol} 的K线数据已存在，跳过下载")
    
    # 解析数据
    parse_downloaded_data(
        data_dir=data_dir,
        symbols=[symbol],
        time_interval=time_interval,
        parsed_data_dir=parsed_data_dir,
        start_date=start_date,
        end_date=end_date
    )
    
    # 生成全息K线
    import tempfile
    with tempfile.TemporaryDirectory(prefix="um_holo_") as temp_dir:
        temp_path = Path(temp_dir)
        
        # 生成全息K线
        holo_files = generate_holo_klines(parsed_data_dir, TradeType.um_futures, temp_path, symbols=[symbol])
        
        if not holo_files:
            print(f"❌ 无法生成 {symbol} 的全息K线")
            return pl.DataFrame()
        
        # 检测和处理间隙
        # symbols_with_gaps, _ = detect_and_process_gaps(holo_files, start_date=start_date, end_date=end_date)
        
        # 重采样到指定频率
        resampled_dir = temp_path / "resampled"
        resampled_dir.mkdir(parents=True, exist_ok=True)
        resampled_files = resample_holo_klines(holo_files, resampled_dir, frequency)
        
        # 获取最终DataFrame
        result_df = get_final_dataframe(resampled_files, symbol)
        
        # 将结果保存为CSV文件
        csv_path = Path("./tempKlines.csv")
        result_df.write_csv(csv_path)
        print(f"✅ 数据已保存到: {csv_path}")
        
        return result_df

def plot_dataframe(
    df: pl.DataFrame,
    data_type: Optional[str] = None,
    symbol: Optional[str] = None,
    save_path: Optional[Path] = None,
    figsize: Tuple[int, int] = (12, 6)
) -> None:
    """
    绘制DataFrame数据的折线图
    
    Args:
        df: 要绘制的DataFrame
        data_type: 数据类型，可选值为'kline'或'metrics'，如果不提供将自动检测
        symbol: 币对名称，用于标题
        save_path: 保存图片的路径，如果不提供则显示图片
        figsize: 图片尺寸
    """
    if df.is_empty():
        print("❌ 数据为空，无法绘图")
        return
    
    # 自动检测数据类型
    if data_type is None:
        if 'close' in df.columns:
            data_type = 'kline'
        else:
            data_type = 'metrics'
    
    # 创建图形
    plt.figure(figsize=figsize)
    
    if data_type == 'kline':
        # 绘制K线数据的close价格
        if 'candle_begin_time' in df.columns and 'close' in df.columns:
            # 确保时间列是datetime类型
            if df['candle_begin_time'].dtype != pl.Datetime:
                df = df.with_columns(pl.col('candle_begin_time').str.to_datetime())
            
            # 绘制折线图
            plt.plot(df['candle_begin_time'], df['close'], label='Close Price', color='blue', linewidth=1.5)
            
            # 设置x轴日期格式
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.xticks(rotation=45)
            
            plt.ylabel('Price')
            plt.title(f'{symbol} Close Price Chart' if symbol else 'Close Price Chart')
        else:
            print("❌ K线数据缺少必要的列: 'candle_begin_time' 或 'close'")
            return
    
    elif data_type == 'metrics':
        # 绘制metrics数据
        if 'timestamp' in df.columns:
            # 确保时间列是datetime类型
            if df['timestamp'].dtype != pl.Datetime:
                df = df.with_columns(pl.col('timestamp').str.to_datetime())
            
            # 获取数值列
            numeric_columns = df.select([pl.col(pl.NUMERIC_DTYPES)]).columns
            
            # 绘制所有数值列
            for col in numeric_columns:
                if col != 'timestamp':
                    plt.plot(df['timestamp'], df[col], label=col)
            
            # 设置x轴日期格式
            plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.xticks(rotation=45)
            
            plt.title(f'{symbol} Metrics Chart' if symbol else 'Metrics Chart')
        else:
            print("❌ Metrics数据缺少必要的列: 'timestamp'")
            return
    
    else:
        print(f"❌ 不支持的数据类型: {data_type}")
        return
    
    # 添加网格和图例
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    
    # 保存或显示图片
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f"✅ 图片已保存到: {save_path}")
    else:
        plt.show()
    
    # 关闭图形
    plt.close()


async def get_metrics_dataframe(
    http_proxy: str,
    symbol: str,
    start_date: str,
    end_date: str,
    data_dir: Path
) -> pl.DataFrame:
    """
    获取单个货币对的Metrics数据DataFrame
    
    Args:
        http_proxy: HTTP代理
        symbol: 币对
        start_date: 起始日期
        end_date: 结束日期
        data_dir: 数据保存目录
    
    Returns:
        Metrics数据的DataFrame
    """
    # 检查数据是否已下载
    data_exists = await _check_data_exists(
        data_dir=data_dir,
        symbol=symbol,
        data_type=DataType.metrics,
        time_interval="",
        start_date=start_date,
        end_date=end_date
    )
    
    if not data_exists:
        # 下载数据
        await _download_single_symbol_data(
            http_proxy=http_proxy,
            symbol=symbol,
            data_dir=data_dir,
            data_type=DataType.metrics,
            time_interval="",
            start_date=start_date,
            end_date=end_date
        )
    else:
        print(f"✅ {symbol} 的Metrics数据已存在，跳过下载")
    
    metrics_symbol_dir = data_dir / f"{BASE_DATA_PATH}/{TradeType.um_futures.value}/daily/{DataType.metrics.value}/{symbol}"
    if metrics_symbol_dir.exists():
        manager = AwsDataFileManager(metrics_symbol_dir)
        verified_files = manager.get_verified_files()
        
        if verified_files:
            try:
                # 根据时间范围筛选文件
                filtered_files = verified_files
                if start_date and end_date:
                    filtered_files = filter_files_by_time_range(verified_files, start_date, end_date)
                    
                    # 分离zip文件和其他文件
                    filtered_zip_files = [f for f in filtered_files if f.name.endswith('.zip')]
                    if filtered_zip_files:
                        print(f"     筛选出 {len(filtered_zip_files)} 个Metrics文件在 {start_date} - {end_date} 范围内")
                    else:
                        print(f"     没有找到在 {start_date} - {end_date} 范围内的Metrics文件")
                        return pl.DataFrame()
                else:
                    print(f"     未指定时间范围，解析所有 {len(verified_files)} 个Metrics文件")
                
                # 尝试创建metrics解析器
                metrics_parser = create_aws_parser(DataType.metrics)
                
                # 只处理zip文件
                filtered_zip_files = [f for f in filtered_files if f.name.endswith('.zip')]
                if not filtered_zip_files:
                    print(f"⚠️  没有找到可解析的Metrics zip文件")
                    return pl.DataFrame()
                
                # 读取所有符合条件的文件并合并
                dfs = []
                for zip_file in filtered_zip_files:
                    try:
                        # 从zip文件读取CSV数据
                        df = metrics_parser.read_csv_from_zip(zip_file)
                        dfs.append(df)
                        print(f"     ✅ 解析 {zip_file.name}")
                    except Exception as e:
                        print(f"     ❌ 解析 {zip_file.name} 失败: {e}")
                
                if dfs:
                    # 合并所有DataFrame
                    combined_df = pl.concat(dfs)
                    print(f"✅ 成功解析 {symbol} 的Metrics数据，共 {len(combined_df)} 行")
                    return combined_df
                else:
                    print(f"⚠️  没有成功解析任何Metrics文件")
                    return pl.DataFrame()
                    
            except Exception as e:
                print(f"❌ 解析 {symbol} 的Metrics数据失败: {e}")
                print("⚠️  当前版本可能不支持Metrics数据的解析")
                import traceback
                traceback.print_exc()
    
    return pl.DataFrame()


def parse_downloaded_data(
    data_dir: Path,
    symbols: List[str],
    time_interval: str,
    parsed_data_dir: Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> None:
    """
    解析下载的zip文件为CSV格式
    
    Args:
        data_dir: 下载的数据目录
        symbols: 币对列表
        time_interval: K线时间间隔
        parsed_data_dir: 解析后的数据保存目录
        start_date: 起始日期（YYYY-MM-DD格式），仅解析此日期之后的数据
        end_date: 结束日期（YYYY-MM-DD格式），仅解析此日期之前的数据
    """
    print(f"\n🔄 解析下载的数据...")
    
    kline_parser = create_aws_parser(DataType.kline)
    
    for symbol in symbols:
        print(f"   解析 {symbol}...")
        
        # 解析K线数据
        kline_symbol_dir = data_dir / f"{BASE_DATA_PATH}/{TradeType.um_futures.value}/daily/{DataType.kline.value}/{symbol}/{time_interval}"
        if kline_symbol_dir.exists():
            manager = AwsDataFileManager(kline_symbol_dir)
            verified_files = manager.get_verified_files()
            
            if verified_files:
                # 根据时间范围筛选文件
                filtered_files = verified_files
                if start_date and end_date:
                    filtered_files = filter_files_by_time_range(verified_files, start_date, end_date)
                    
                    # 分离zip文件和其他文件
                    filtered_zip_files = [f for f in filtered_files if f.name.endswith('.zip')]
                    if filtered_zip_files:
                        print(f"     筛选出 {len(filtered_zip_files)} 个文件在 {start_date} - {end_date} 范围内")
                    else:
                        print(f"     没有找到在 {start_date} - {end_date} 范围内的文件")
                        continue
                else:
                    print(f"     未指定时间范围，解析所有 {len(verified_files)} 个文件")
                
                # 确保解析目录存在（包含data/前缀）
                symbol_parsed_dir = parsed_data_dir / f"{BASE_DATA_PATH}/{TradeType.um_futures.value}/daily/{DataType.kline.value}/{symbol}/{time_interval}"
                symbol_parsed_dir.mkdir(parents=True, exist_ok=True)
                
                # 清理旧的CSV文件
                for csv_file in symbol_parsed_dir.glob("*.csv"):
                    csv_file.unlink()
                    print(f"     🗑️  删除旧的CSV文件: {csv_file.name}")
                
                # 只处理zip文件
                for zip_file in [f for f in filtered_files if f.name.endswith('.zip')]:
                    try:
                        # 从zip文件读取CSV数据
                        df = kline_parser.read_csv_from_zip(zip_file)
                        
                        # 保存为Parquet文件
                        parquet_file = symbol_parsed_dir / f"{zip_file.stem}.parquet"
                        df.write_parquet(parquet_file)
                        print(f"     ✅ 解析 {zip_file.name} -> {parquet_file.name}")
                    except Exception as e:
                        print(f"     ❌ 解析 {zip_file.name} 失败: {e}")
    
    print("✅ 数据解析完成")


def generate_holo_klines(
    parsed_data_dir: Path,
    trade_type: TradeType,
    output_dir: Path,
    symbols: Optional[List[str]] = None
) -> List[Path]:
    """
    生成全息k线
    
    Args:
        parsed_data_dir: 解析后的数据目录
        trade_type: 交易类型
        output_dir: 输出目录
        symbols: 要处理的符号列表，如果为None则处理所有符号
    
    Returns:
        生成的全息k线文件列表
    """
    print(f"\n🔄 生成全息k线...")
    merger = Holo1mKlineMerger(
        trade_type=trade_type,
        base_dir=parsed_data_dir,
        include_vwap=True,
        include_funding=True,
    )
    
    # 生成指定符号的全息k线
    lazy_frames = merger.generate_all(output_dir, target_symbols=symbols)
    if not lazy_frames:
        print("❌ 没有找到可处理的符号")
        return []
    
    # 执行Polars批处理以生成文件
    execute_polars_batch(lazy_frames, "Collecting kline data")
    
    # 获取生成的文件
    generated_files = list(output_dir.glob("*.parquet"))
    print(f"✅ 成功生成 {len(generated_files)} 个全息k线文件")
    return generated_files


def detect_and_process_gaps(
    holo_files: List[Path],
    min_days: int = 1,
    min_price_chg: float = 0.1,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Tuple[int, int]:
    """
    检测和处理间隙
    
    Args:
        holo_files: 全息k线文件列表
        min_days: 最小间隙天数
        min_price_chg: 最小价格变化百分比
        start_date: 起始日期（YYYY-MM-DD格式）
        end_date: 结束日期（YYYY-MM-DD格式）
    
    Returns:
        (有间隙的符号数, 生成的分割文件数)
    """
    print(f"\n🔄 检测间隙...")
    print(f"     Min days: {min_days}")
    print(f"     Min price change: {min_price_chg * 100}%")
    
    detector = HoloKlineGapDetector(min_days, min_price_chg)
    splitter = HoloKlineSplitter(prefix="SP")
    
    # 生成间隙检测任务
    gap_tasks = [detector.detect(file_path) for file_path in holo_files]
    gap_results = execute_polars_batch(gap_tasks, "Detecting gaps", return_results=True)
    
    symbols_with_gaps = 0
    total_splits = 0
    
    # 转换日期字符串为datetime对象
    has_time_filter = False
    filter_start = None
    filter_end = None
    
    if start_date and end_date:
        from datetime import datetime
        # 解析日期字符串
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # 转换为Polars datetime对象
        filter_start = pl.datetime(
            year=start_dt.year, month=start_dt.month, day=start_dt.day,
            time_zone="UTC"
        )
        filter_end = pl.datetime(
            year=end_dt.year, month=end_dt.month, day=end_dt.day,
            time_zone="UTC"
        ) + pl.duration(days=1) - pl.duration(microseconds=1)
        
        has_time_filter = True
    
    # 处理间隙结果
    for file_path, gaps_df in zip(holo_files, gap_results):
        if len(gaps_df) > 0:
            symbol = file_path.stem
            symbols_with_gaps += 1
            
            print(f"\n🔍 {symbol} - {len(gaps_df)} gap(s)")
            print("-" * 40)
            
            # 过滤出指定时间范围内的间隙
            if has_time_filter:
                gaps_df = gaps_df.filter(
                    (pl.col("prev_begin_time") >= filter_start) & 
                    (pl.col("candle_begin_time") <= filter_end)
                )
            
            for gap in gaps_df.sort("time_diff", descending=True).iter_rows(named=True):
                print(f"  {gap['prev_begin_time']} → {gap['candle_begin_time']}")
                print(f"  Duration: {gap['time_diff']}, Change: {gap['price_change']:.2%}")
            
            # 根据检测到的间隙分割k线数据
            print(f"  分割 {symbol}...")
            split_files = splitter.split_file(file_path, gaps_df)
            total_splits += len(split_files)
            
            for split_file in split_files:
                seg_df = pl.read_parquet(split_file)
                min_begin_time = seg_df["candle_begin_time"].min()
                max_begin_time = seg_df["candle_begin_time"].max()
                print(f"    {split_file.name}: {len(seg_df)} 行, {min_begin_time} 到 {max_begin_time}")
    
    print(f"\n📈 总结: {symbols_with_gaps}/{len(holo_files)} 个符号有间隙")
    print(f"         生成了 {total_splits} 个分割文件")
    
    return symbols_with_gaps, total_splits


def resample_holo_klines(holo_files: List[Path], output_dir: Path, frequency: str = "1h") -> List[Path]:
    """
    将全息k线重采样到指定频率
    
    Args:
        holo_files: 全息k线文件列表
        output_dir: 输出目录
        frequency: 重采样频率（如"1h", "4h", "1d"等）
    
    Returns:
        重采样后的文件列表
    """
    print(f"\n🔄 将数据重采样到{frequency}...")
    
    # 初始化重采样器
    resampler = HoloKlineResampler(resample_interval=frequency)
    
    resampled_files = []
    for file_path in holo_files:
        symbol = file_path.stem
        output_file = output_dir / f"{symbol}_{frequency}.parquet"
        
        try:
            # 读取全息k线数据
            df = pl.read_parquet(file_path)
            
            # 将DataFrame转换为LazyFrame
            ldf = df.lazy()
            
            # 重采样到指定频率
            resampled_ldf = resampler.resample(ldf)
            
            # 计算并保存重采样后的数据
            resampled_df = resampled_ldf.collect()
            resampled_df.write_parquet(output_file)
            resampled_files.append(output_file)
            
            print(f"   ✅ {symbol}: {len(df)} 行 → {len(resampled_df)} 行")
        except Exception as e:
            print(f"   ❌ {symbol}: 重采样失败 - {e}")
            import traceback
            traceback.print_exc()
    
    print(f"✅ 成功重采样 {len(resampled_files)} 个文件到1h")
    return resampled_files


def get_final_dataframe(resampled_files: List[Path], symbol: str) -> pl.DataFrame:
    """
    获取最终的DataFrame
    
    Args:
        resampled_files: 重采样后的文件列表
        symbol: 要获取的符号
    
    Returns:
        最终的DataFrame
    """
    for file_path in resampled_files:
        if file_path.stem.startswith(symbol):
            return pl.read_parquet(file_path)
    
    print(f"❌ 没有找到 {symbol} 的重采样文件")
    return pl.DataFrame()


async def main() -> None:
    """主函数"""
    # 配置
    http_proxy = "http://127.0.0.1:7890"  # 7890代理
    data_dir = Path("d:/Codes/binance_datatool-main/data")  # 数据保存目录
    parsed_data_dir = Path("d:/Codes/binance_datatool-main/parsed_data")  # 解析后的数据目录
    
    # 时间范围
    start_date = "2026-01-01"
    end_date = "2026-01-10"
    
    # 测试单个货币对
    test_symbol = "BTCUSDT"
    
    try:
        # 创建output目录用于保存图片
        output_dir = Path("d:/Codes/binance_datatool-main/output")
        output_dir.mkdir(exist_ok=True)
        
        # 1. 获取单个货币对的K线数据（重采样到5分钟）
        print(f"\n📊 获取 {test_symbol} 的K线数据（{start_date} ~ {end_date}）...")
        kline_df = await get_kline_dataframe(
            http_proxy=http_proxy,
            symbol=test_symbol,
            start_date=start_date,
            end_date=end_date,
            data_dir=data_dir,
            parsed_data_dir=parsed_data_dir,
            frequency="5m"
        )
        
        if not kline_df.is_empty():
            print(f"✅ 成功获取 {test_symbol} 的K线数据")
            print(f"   行数: {len(kline_df)}")
            print(f"   列: {list(kline_df.columns)}")
            
            # 绘制K线数据的close价格
            print(f"📊 绘制 {test_symbol} 的K线数据...")
            save_path = output_dir / f"{test_symbol}_close_5m_{start_date}_{end_date}.png"
            plot_dataframe(kline_df, data_type='kline', symbol=test_symbol, save_path=save_path)
        
        # 2. 获取单个货币对的Metrics数据
        print(f"\n📊 获取 {test_symbol} 的Metrics数据（{start_date} ~ {end_date}）...")
        metrics_df = await get_metrics_dataframe(
            http_proxy=http_proxy,
            symbol=test_symbol,
            start_date=start_date,
            end_date=end_date,
            data_dir=data_dir
        )
        
        if not metrics_df.is_empty():
            print(f"✅ 成功获取 {test_symbol} 的Metrics数据")
            print(f"   行数: {len(metrics_df)}")
            print(f"   列: {list(metrics_df.columns)}")
            metrics_df.to_csv("./tempMetrics.csv", index=False)

            # 绘制Metrics数据
            print(f"📊 绘制 {test_symbol} 的Metrics数据...")
            save_path = output_dir / f"{test_symbol}_metrics_{start_date}_{end_date}.png"
            plot_dataframe(metrics_df, data_type='metrics', symbol=test_symbol, save_path=save_path)
        
        print("\n✅ 所有功能测试完成")
        
    except Exception as e:
        print(f"❌ 程序运行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  程序已被用户中断")
    except Exception as e:
        print(f"\n❌ 程序运行失败: {e}")
        import traceback
        traceback.print_exc()