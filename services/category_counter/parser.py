"""Parser for counting Cian category listings."""

import re
import time
import random
from typing import List, Dict, Optional
from curl_cffi import requests
from datetime import datetime

from packages.flipper_core.html_to_md import HTMLToMarkdownConverter


class CategoryCounterParser:
    """Parser for counting listings in Cian categories."""
    
    CATEGORIES = [
        {
            "name": "Вторичка Москва",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&saved_search_id=52445533"
        },
        {
            "name": "Первичка Москва",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=2&offer_type=flat&region=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&with_newobject=1&saved_search_id=52445654"
        },
        {
            "name": "Первичка МО",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=2&offer_type=flat&region=4593&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&with_newobject=1&saved_search_id=52445692"
        },
        {
            "name": "Вторичка МО",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&region=4593&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&saved_search_id=52445702"
        }
    ]
    
    def __init__(self, http_proxy: Optional[str] = None):
        """Initialize parser with optional mobile proxy."""
        self.http_proxy = http_proxy
        self.converter = HTMLToMarkdownConverter()
        
    def _get_html(self, url: str) -> Optional[str]:
        """Fetch HTML from URL using curl_cffi with mobile proxy."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Setup proxy if configured
                proxy_dict = None
                if self.http_proxy:
                    proxy_dict = {"http": self.http_proxy, "https": self.http_proxy}
                    print(f"Using mobile proxy (attempt {attempt + 1})")
                
                # Make request
                response = requests.get(
                    url,
                    proxies=proxy_dict,
                    impersonate="chrome",
                    timeout=60
                )
                
                if response.status_code == 429:
                    print(f"Rate limited (429), retrying...")
                    time.sleep(4)
                    continue
                
                if response.status_code == 200:
                    # Check for captcha
                    if "Captcha" in response.text or "captcha" in response.text.lower():
                        print(f"Captcha detected, retrying...")
                        time.sleep(4)
                        continue
                    
                    return response.text
                
                print(f"HTTP {response.status_code}, retrying...")
                time.sleep(2)
                
            except Exception as e:
                print(f"Request failed: {e}")
                time.sleep(2)
        
        return None
    
    def _extract_count_from_markdown(self, markdown: str) -> Optional[int]:
        """Extract listing count from markdown text."""
        # Pattern: "Найдено {n} объявлений"
        patterns = [
            r'Найдено\s+(\d+(?:\s+\d+)*)\s+объявлени',
            r'найдено\s+(\d+(?:\s+\d+)*)\s+объявлени',
            r'(\d+(?:\s+\d+)*)\s+объявлени'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, markdown, re.IGNORECASE)
            if match:
                # Remove spaces from number (e.g., "123 456" -> "123456")
                count_str = match.group(1).replace(" ", "")
                return int(count_str)
        
        return None
    
    def parse_category(self, category: Dict[str, str]) -> Optional[Dict[str, any]]:
        """Parse single category and return count."""
        name = category["name"]
        url = category["url"]
        
        print(f"\nParsing category: {name}")
        print(f"URL: {url}")
        
        # Get HTML
        html = self._get_html(url)
        if not html:
            print(f"Failed to fetch HTML for {name}")
            return None
        
        # Convert to markdown
        try:
            markdown = self.converter.convert(html)
        except Exception as e:
            print(f"Failed to convert HTML to markdown: {e}")
            return None
        
        # Extract count
        count = self._extract_count_from_markdown(markdown)
        if count is None:
            print(f"Failed to extract count from markdown for {name}")
            return None
        
        print(f"Found {count} listings for {name}")
        
        return {
            "name": name,
            "url": url,
            "count": count,
            "timestamp": datetime.now()
        }
    
    def parse_all_categories(self) -> List[Dict[str, any]]:
        """Parse all categories and return results."""
        results = []
        
        for category in self.CATEGORIES:
            result = self.parse_category(category)
            if result:
                results.append(result)
            
            # Small delay between requests
            time.sleep(2)
        
        return results
