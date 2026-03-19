"""Test script for category counter service."""

import os
import sys

# Set PYTHONPATH
sys.path.insert(0, os.path.dirname(__file__))

from services.category_counter.parser import CategoryCounterParser


def main():
    """Test category counter parser."""
    print("Testing category counter parser...\n")
    
    # Get HTTP proxy from env (mobile proxy)
    http_proxy = os.getenv("HTTP_PROXY", "")
    
    if http_proxy:
        # Hide credentials in log
        proxy_display = http_proxy.split('@')[1] if '@' in http_proxy else http_proxy
        print(f"Using mobile proxy: {proxy_display}")
    else:
        print("No proxy configured, using direct connection")
    
    # Initialize parser
    parser = CategoryCounterParser(http_proxy=http_proxy if http_proxy else None)
    
    # Parse all categories
    results = parser.parse_all_categories()
    
    # Print results
    print("\n" + "="*60)
    print("RESULTS:")
    print("="*60)
    
    for result in results:
        print(f"\n{result['name']}")
        print(f"  URL: {result['url']}")
        print(f"  Count: {result['count']:,}")
        print(f"  Timestamp: {result['timestamp']}")
    
    print("\n" + "="*60)
    print(f"Total categories parsed: {len(results)}/4")
    print("="*60)


if __name__ == "__main__":
    main()
