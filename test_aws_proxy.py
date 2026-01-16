#!/usr/bin/env python3
"""
Test script to verify AWS proxy configuration is working correctly.
"""

import asyncio
import os
from bdt_common.network import create_aiohttp_session, get_proxy_for_url
from bhds.aws.client import AwsClient, create_aws_client_from_config
from bdt_common.enums import DataType, TradeType, DataFrequency

async def test_aws_proxy_config():
    """Test AWS proxy configuration."""
    print("Testing AWS proxy configuration...")
    
    # Configure the proxy
    http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy") or "http://127.0.0.1:7890"
    print(f"Using proxy: {http_proxy}")
    
    # Test get_proxy_for_url function
    test_urls = [
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://10.0.0.1:8000",
        "http://172.16.0.1:8000",
        "http://192.168.1.1:8000",
        "https://example.com",
        "https://s3.amazonaws.com",
        "https://data.binance.vision"
    ]
    
    print("\nTesting get_proxy_for_url function:")
    for url in test_urls:
        proxy = get_proxy_for_url(url, http_proxy)
        status = "Using proxy" if proxy else "Bypassing proxy"
        print(f"  {url}: {status}")
    
    # Test creating AWS client with proxy
    print("\nTesting AWS client with proxy...")
    try:
        async with create_aiohttp_session(30) as session:
            client = create_aws_client_from_config(
                trade_type=TradeType.spot,
                data_type=DataType.kline,
                data_freq=DataFrequency.daily,
                time_interval="1m",
                session=session,
                http_proxy=http_proxy
            )
            
            print("  ✅ Successfully created AWS client")
            print(f"  Client proxy setting: {client.http_proxy}")
            
            # Test proxy configuration with AWS endpoint
            aws_url = "https://data.binance.vision"
            proxy_for_aws = get_proxy_for_url(aws_url, http_proxy)
            print(f"  Proxy for AWS endpoint {aws_url}: {'Enabled' if proxy_for_aws else 'Disabled'}")
            
            # Try to list symbols (this will make an actual request)
            print("\n  Trying to list symbols...")
            try:
                symbols = await client.list_symbols()
                print(f"    ✅ Successfully listed {len(symbols)} symbols")
                if symbols:
                    print(f"    Example symbols: {symbols[:5]}")
            except Exception as e:
                print(f"    ⚠️  Error listing symbols: {e}")
                print("    This might be due to network/proxy configuration or AWS issues")
    except Exception as e:
        print(f"  ❌ Error creating AWS client or session: {e}")
    
    print("\nAWS proxy configuration test completed!")

if __name__ == "__main__":
    asyncio.run(test_aws_proxy_config())