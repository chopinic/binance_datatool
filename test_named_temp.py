#!/usr/bin/env python3
"""
Test script to verify NamedTemporaryFile usage.
"""

import tempfile
import os

print("Testing NamedTemporaryFile with delete parameter...")

# Test with delete=False
with tempfile.NamedTemporaryFile(mode="w", delete=False, prefix="test_") as temp_file:
    temp_file.write("test content")
    temp_file_path = temp_file.name
    print(f"  Temporary file created: {temp_file_path}")
    print(f"  File exists: {os.path.exists(temp_file_path)}")

# Verify file still exists after close
print(f"  File exists after close: {os.path.exists(temp_file_path)}")

# Clean up
os.unlink(temp_file_path)
print(f"  File removed: {not os.path.exists(temp_file_path)}")

print("\nTest completed successfully!")