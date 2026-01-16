#!/usr/bin/env python3
"""
Test script to verify proxy configuration is working correctly.
"""

import asyncio
from bdt_common.network import create_aiohttp_session, is_local_address
from urllib.parse import urlparse

async def test_proxy_config():
    """Test the proxy configuration."""
    print("Testing proxy configuration...")
    
    # Test is_local_address function
    test_addresses = [
        "localhost",
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "192.168.1.100",
        "example.com",
        "s3.amazonaws.com"
    ]
    
    print("\nTesting is_local_address function:")
    for address in test_addresses:
        result = is_local_address(address)
        print(f"  {address}: {'Local' if result else 'Not Local'}")
    
    # Test creating a session
    print("\nTesting create_aiohttp_session...")
    try:
        async with create_aiohttp_session(10) as session:
            print("  ✅ Successfully created ClientSession")
            print(f"  Session timeout: {session.timeout.total} seconds")
            print(f"  Session has connector: {hasattr(session, 'connector')}")
    except Exception as e:
        print(f"  ❌ Error creating session: {e}")
    
    print("\nProxy configuration test completed!")

if __name__ == "__main__":
    asyncio.run(test_proxy_config())