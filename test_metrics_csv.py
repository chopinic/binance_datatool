#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script to inspect metrics CSV file content.
"""

import sys
import polars as pl

# 添加项目src目录到Python路径
from pathlib import Path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root / "src"))

# 模拟一个典型的metrics CSV文件内容
sample_csv_content = """BTCUSDT,2026-01-14 00:05:00,12345.67,89012345.67,0.00012345,45678.90,45678.12,0.00001234,2026-01-14 00:05:00
BTCUSDT,2026-01-14 00:10:00,12345.67,89012345.67,0.00012345,45678.90,45678.12,0.00001234,2026-01-14 00:10:00
"""

# 保存为临时CSV文件
temp_csv_path = project_root / "temp_metrics_sample.csv"
temp_csv_path.write_text(sample_csv_content)

print("Created temporary metrics CSV sample")
print("\nSample content:")
print(sample_csv_content)

# 使用Polars读取并检查结构
try:
    # 尝试无模式读取
    df = pl.read_csv(temp_csv_path, has_header=False)
    print(f"\nPolars inferred schema:")
    print(df.schema)
    print(f"\nColumns: {df.columns}")
    print(f"Number of columns: {len(df.columns)}")
    
    # 清理临时文件
    temp_csv_path.unlink()
except Exception as e:
    print(f"Error reading CSV: {e}")
    
    # 清理临时文件
    if temp_csv_path.exists():
        temp_csv_path.unlink()