"""Parser for counting Cian category listings."""

import os
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

from curl_cffi import requests

from packages.flipper_core.html_to_md import HTMLToMarkdownConverter


def _looks_like_captcha_or_block(html: str) -> bool:
    if not html or not html.strip():
        return True
    try:
        import bs4
    except ImportError:
        low = html.lower()
        return (
            "captcha" in low
            or "подтвердите, что вы не робот" in low
            or "введите символы" in low
            or "пройдите проверку" in low
        )

    soup = bs4.BeautifulSoup(html, "html.parser")
    visible_text = soup.get_text(separator=" ", strip=True).lower()
    is_real_captcha = (
        "введите символы" in visible_text
        or "подтвердите, что вы не робот" in visible_text
        or "пройдите проверку" in visible_text
    )
    captcha_form = soup.find(
        "form", attrs={"action": lambda a: a and "captcha" in a.lower()}
    )
    if is_real_captcha or captcha_form:
        return True

    has_offer_links = bool(
        soup.find("a", href=lambda h: h and ("/sale/flat/" in h or "/rent/" in h))
    )
    if has_offer_links:
        return False

    title_tag = soup.find("title")
    title_text = (title_tag.string or "") if title_tag else ""
    has_cian_title = any(w in title_text.lower() for w in ("cian", "циан", "объявлен"))
    if has_cian_title:
        return False

    return "captcha" in visible_text


class CategoryCounterParser:
    """Parser for counting listings in Cian categories."""

    CATEGORIES = [
        {
            "name": "Вторичка Москва",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&region=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&saved_search_id=52445533",
        },
        {
            "name": "Первичка Москва",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=2&offer_type=flat&region=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&with_newobject=1&saved_search_id=52445654",
        },
        {
            "name": "Первичка МО",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=2&offer_type=flat&region=4593&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&with_newobject=1&saved_search_id=52445692",
        },
        {
            "name": "Вторичка МО",
            "url": "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2&object_type%5B0%5D=1&offer_type=flat&region=4593&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1&saved_search_id=52445702",
        },
    ]

    def __init__(
        self,
        http_proxy: Optional[str] = None,
        proxy_urls: Optional[List[str]] = None,
    ):
        self.http_proxy = http_proxy
        self._proxy_urls = [u for u in (proxy_urls or []) if u]
        self.converter = HTMLToMarkdownConverter()

    def _pick_proxy(self) -> Optional[str]:
        if self._proxy_urls:
            return random.choice(self._proxy_urls)
        return self.http_proxy

    def _get_html(self, url: str) -> Optional[str]:
        """GET страницы через curl_cffi; прокси — из списка или один HTTP_PROXY."""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                proxy_url = self._pick_proxy()
                proxy_dict = None
                if proxy_url:
                    proxy_dict = {"http": proxy_url, "https": proxy_url}
                    print(f"Using proxy (attempt {attempt + 1})")

                response = requests.get(
                    url,
                    proxies=proxy_dict,
                    impersonate="chrome",
                    timeout=60,
                )

                if response.status_code == 429:
                    print("Rate limited (429), retrying...")
                    time.sleep(4)
                    continue

                if response.status_code == 200:
                    if _looks_like_captcha_or_block(response.text):
                        print("Captcha detected, retrying...")
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
        patterns = [
            r"Найдено\s+(\d+(?:\s+\d+)*)\s+объявлени",
            r"найдено\s+(\d+(?:\s+\d+)*)\s+объявлени",
            r"(\d+(?:\s+\d+)*)\s+объявлени",
        ]

        for pattern in patterns:
            match = re.search(pattern, markdown, re.IGNORECASE)
            if match:
                count_str = match.group(1).replace(" ", "")
                return int(count_str)

        return None

    def _extract_count_from_html(self, html: str) -> Optional[int]:
        if not html:
            return None

        patterns = [
            r'"totalOffers"\s*:\s*(\d+)',
            r'"offersCount"\s*:\s*(\d+)',
            r'"total"\s*:\s*(\d+)\s*,\s*"page"',
            r'"total"\s*:\s*(\d+)\s*,\s*"currentPage"',
            r'"offerCount"\s*:\s*(\d+)',
            r"Найдено\s+(\d+(?:\s+\d+)*)\s+объявлени",
        ]
        for pat in patterns:
            m = re.search(pat, html, re.IGNORECASE)
            if not m:
                continue
            s = m.group(1).replace(" ", "")
            try:
                return int(s)
            except Exception:
                continue
        return None

    def parse_category(self, category: Dict[str, str]) -> Optional[Dict[str, any]]:
        name = category["name"]
        url = category["url"]

        print(f"\nParsing category: {name}")
        print(f"URL: {url}")

        max_attempts = int(os.environ.get("CATEGORY_COUNTER_MAX_RETRIES", "5").strip() or "5")
        max_attempts = max(1, min(max_attempts, 10))

        last_html = None
        for attempt in range(max_attempts):
            html = self._get_html(url)
            if not html:
                time.sleep(2)
                continue
            last_html = html

            count_html = self._extract_count_from_html(html)
            if count_html is not None:
                print(f"Found {count_html} listings for {name} (from HTML)")
                return {
                    "name": name,
                    "url": url,
                    "count": count_html,
                    "timestamp": datetime.now(),
                }

            try:
                markdown = self.converter.convert(html)
            except Exception as e:
                print(f"Failed to convert HTML to markdown: {e}")
                time.sleep(2)
                continue

            count_md = self._extract_count_from_markdown(markdown)
            if count_md is not None:
                print(f"Found {count_md} listings for {name} (from markdown)")
                return {
                    "name": name,
                    "url": url,
                    "count": count_md,
                    "timestamp": datetime.now(),
                }

            time.sleep(2)

        if last_html:
            print(f"Failed to extract count for {name}")
        else:
            print(f"Failed to fetch HTML for {name}")
        return None

    def parse_all_categories(self) -> List[Dict[str, any]]:
        results = []

        for category in self.CATEGORIES:
            result = self.parse_category(category)
            if result:
                results.append(result)

            time.sleep(2)

        return results
