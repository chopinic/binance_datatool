#!/usr/bin/env python3
"""
Test script to verify Python 3.9 compatibility fixes.
"""

import sys
from pathlib import Path

print(f"Python version: {sys.version}")

# Test imports for all fixed modules
modules_to_test = [
    ("bhds.aws.downloader", ["AwsDownloader"]),
    ("bhds.tasks.aws_download", ["AwsDownloadTask"]),
    ("bhds.tasks.holo_1m_kline", ["GenHolo1mKlineTask"]),
    ("bhds.tasks.holo_resample", ["HoloResampleTask"]),
    ("bdt_common.rest_api.market", ["create_binance_market_api"]),
    ("bhds.api.completion.detector", ["create_detector"]),
    ("bhds.api.completion.task", ["CompletionTask", "CompletionOperation"]),
]

success_count = 0
total_count = sum(len(symbols) for _, symbols in modules_to_test)

try:
    for module_name, symbols in modules_to_test:
        for symbol in symbols:
            exec(f"from {module_name} import {symbol}")
            print(f"✓ Imported {symbol} from {module_name} successfully")
            success_count += 1
    
    if success_count == total_count:
        print(f"\n🎉 All {total_count} Python 3.9 compatibility tests passed!")
    else:
        print(f"\n❌ Only {success_count} of {total_count} tests passed!")
        sys.exit(1)
        
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)