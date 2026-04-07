"""Parser for counting Cian category listings."""

import json
import os
import re
import time
from typing import List, Dict, Optional
import urllib.error
import urllib.request
from curl_cffi import requests
from datetime import datetime

from packages.flipper_core.html_to_md import HTMLToMarkdownConverter


DEFAULT_DECODO_SCRAPE_URL = "https://scraper-api.decodo.com/v2/scrape"


def _decodo_authorization_header(token_or_header: str) -> str:
    t = (token_or_header or "").strip()
    if not t:
        raise ValueError("DECODO_AUTH_TOKEN пустой")
    if t.lower().startswith("basic "):
        return t
    return f"Basic {t}"


def _extract_html_from_decodo_response(data: object) -> Optional[str]:
    if data is None:
        return None
    if isinstance(data, str):
        s = data.strip()
        if s.startswith("<") and ("<html" in s.lower() or "<!doctype" in s.lower()):
            return data
        return None
    if not isinstance(data, dict):
        return None
    for key in ("html", "body", "content", "raw_html", "rawHtml"):
        val = data.get(key)
        if isinstance(val, str) and val.strip():
            return val
    results = data.get("results")
    if isinstance(results, list):
        for item in results:
            if not isinstance(item, dict):
                continue
            for key in ("content", "html", "body"):
                val = item.get(key)
                if isinstance(val, str) and val.strip():
                    return val
    return None


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

    # Если это похоже на выдачу Cian (есть ссылки на объявления) — не считаем блоком.
    has_offer_links = bool(
        soup.find("a", href=lambda h: h and ("/sale/flat/" in h or "/rent/" in h))
    )
    if has_offer_links:
        return False

    # Иногда Cian отдаёт "чистый" HTML с минимумом текста, но заголовок/brand есть.
    title_tag = soup.find("title")
    title_text = (title_tag.string or "") if title_tag else ""
    has_cian_title = any(w in title_text.lower() for w in ("cian", "циан", "объявлен"))
    if has_cian_title:
        return False

    # Фолбэк: если в видимом тексте встречается "captcha" — считаем блоком.
    return "captcha" in visible_text


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
        self._decodo_enabled = bool(os.environ.get("DECODO_AUTH_TOKEN", "").strip())
        
    def _get_html(self, url: str) -> Optional[str]:
        """Fetch HTML from URL.

        Приоритет:
        - Decodo Scraper API (если задан DECODO_AUTH_TOKEN): устойчивее к банам на Cian.
        - Прямой GET через curl_cffi (fallback): с опциональным прокси.
        """
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if self._decodo_enabled:
                    html = self._get_html_via_decodo(url)
                    if html and not _looks_like_captcha_or_block(html):
                        return html
                    print("Decodo вернул капчу/блок, повтор...")
                    time.sleep(3)
                    continue

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
                    print("Rate limited (429), retrying...")
                    time.sleep(4)
                    continue
                
                if response.status_code == 200:
                    # Check for captcha
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

    def _get_html_via_decodo(self, url: str) -> Optional[str]:
        api_url = (os.environ.get("DECODO_SCRAPER_URL", "").strip() or DEFAULT_DECODO_SCRAPE_URL).rstrip("/")
        token = os.environ.get("DECODO_AUTH_TOKEN", "").strip()
        if not token:
            return None

        payload = {
            "url": url,
            "proxy_pool": "premium",
            "geo": "Russia",
            "locale": "ru-ru",
            "successful_status_codes": [200, 201],
        }

        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            api_url,
            data=body,
            method="POST",
            headers={
                "accept": "application/json",
                "content-type": "application/json",
                "authorization": _decodo_authorization_header(token),
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=150.0) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            print(f"Decodo HTTP {e.code}: {err_body[:300]}")
            return None
        except urllib.error.URLError as e:
            print(f"Decodo network error: {e}")
            return None
        except Exception as e:
            print(f"Decodo request failed: {e}")
            return None

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            print("Decodo: ответ не JSON")
            return None

        return _extract_html_from_decodo_response(data)
    
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

    def _extract_count_from_html(self, html: str) -> Optional[int]:
        """Пробует вытащить число объявлений напрямую из HTML (быстрее/надёжнее, чем через markdown)."""
        if not html:
            return None

        patterns = [
            # Часто встречается в embedded JSON / initial state
            r'"totalOffers"\s*:\s*(\d+)',
            r'"offersCount"\s*:\s*(\d+)',
            r'"total"\s*:\s*(\d+)\s*,\s*"page"',
            r'"total"\s*:\s*(\d+)\s*,\s*"currentPage"',
            r'"offerCount"\s*:\s*(\d+)',
            # Человеческий текст на странице
            r'Найдено\s+(\d+(?:\s+\d+)*)\s+объявлени',
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
        """Parse single category and return count."""
        name = category["name"]
        url = category["url"]
        
        print(f"\nParsing category: {name}")
        print(f"URL: {url}")
        
        # Забор HTML и извлечение счётчика может "плавать" (капча/неполный HTML),
        # поэтому делаем несколько попыток на одну категорию.
        max_attempts = int(os.environ.get("DECODO_MAX_RETRIES", "5").strip() or "5")
        max_attempts = max(1, min(max_attempts, 10))

        last_html = None
        for attempt in range(max_attempts):
            html = self._get_html(url)
            if not html:
                time.sleep(2)
                continue
            last_html = html

            # Попытка №1: вытащить счётчик прямо из HTML
            count_html = self._extract_count_from_html(html)
            if count_html is not None:
                print(f"Found {count_html} listings for {name} (from HTML)")
                return {
                    "name": name,
                    "url": url,
                    "count": count_html,
                    "timestamp": datetime.now(),
                }

            # Попытка №2: через markdown
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
        """Parse all categories and return results."""
        results = []
        
        for category in self.CATEGORIES:
            result = self.parse_category(category)
            if result:
                results.append(result)
            
            # Small delay between requests
            time.sleep(2)
        
        return results
