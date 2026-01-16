#!/usr/bin/env python3
"""
Test script to verify the actual URL generation in client.py.
"""

import sys
from unittest.mock import MagicMock, patch

# Add src directory to path
sys.path.append('./src')

from bdt_common.enums import DataType, TradeType, DataFrequency
from bdt_common.network import create_aiohttp_session
from bhds.aws.client import AwsClient
from bhds.aws.path_builder import create_path_builder

# Create path builder
path_builder = create_path_builder(
    trade_type=TradeType.spot,
    data_freq=DataFrequency.daily,
    data_type=DataType.funding_rate
)

# Create a mock session
mock_session = MagicMock()

# Create AWS client with proxy setting
aws_client = AwsClient(
    path_builder=path_builder,
    session=mock_session,
    http_proxy="http://127.0.0.1:7890"
)

# Mock the get method to return a response
mock_response = MagicMock()
mock_response.status = 200
mock_response.text = "<ListBucketResult></ListBucketResult>"
mock_session.get.return_value.__aenter__.return_value = mock_response

# Call the list_symbols method to generate the URL
import asyncio
asyncio.run(aws_client.list_symbols())
    
    # Get the URL that was called
    call_args = mock_session.get.call_args
    if call_args:
        url = call_args[0][0]
        print(f"Generated URL: {url}")
        
        # Verify the URL format
        expected_prefix = "data/spot/daily/klines/"
        if expected_prefix in url:
            print(f"✅ URL contains the correct path: '{expected_prefix}'")
        else:
            print(f"❌ URL does not contain the expected path: '{expected_prefix}'")
            print("Path in URL:", url.split("prefix=")[-1].split("&")[0])
    else:
        print("❌ No URL was generated")