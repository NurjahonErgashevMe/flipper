import logging
import httpx
import time
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
from firecrawl import FirecrawlApp
from .models import ParsedAdData

logger = logging.getLogger(__name__)

# Constants
FIRECRAWL_API_URL = "http://127.0.0.1:3003"
FIRECRAWL_API_KEY = "sk-local"
COOKIE_MANAGER_URL = "http://localhost:8000"
COOKIE_FETCH_TIMEOUT = 600.0  # 10 minutes for NoVNC
COOKIE_REFRESH_RETRY_WAIT = 5  # seconds


class AuthenticationError(Exception):
    """Raised when authentication verification fails"""
    pass


class AdParser:
    """Parser for real estate ads using Firecrawl and LLM extraction"""

    def __init__(self, firecrawl_url: str = FIRECRAWL_API_URL,
                 firecrawl_key: str = FIRECRAWL_API_KEY,
                 cookie_manager_url: str = COOKIE_MANAGER_URL):
        self.app = FirecrawlApp(api_url=firecrawl_url, api_key=firecrawl_key)
        self.cookie_manager_url = cookie_manager_url

    def get_cookies_str(self) -> str:
        """
        Fetch cookies from cookie manager service.
        Raises clear exceptions on 503 (empty/invalid cookies) or unexpected responses.
        """
        try:
            resp = httpx.get(
                f"{self.cookie_manager_url}/cookies",
                timeout=COOKIE_FETCH_TIMEOUT
            )

            # 503 = куки пустые, cookie_manager уже запустил recovery-сессию
            if resp.status_code == 503:
                detail = resp.json().get("detail", "cookies empty")
                raise Exception(
                    f"Cookie manager 503: {detail}. "
                    "Recovery session started — waiting for user to log in via NoVNC."
                )

            if resp.status_code != 200:
                raise Exception(
                    f"Cookie manager returned HTTP {resp.status_code}: {resp.text[:200]}"
                )

            data = resp.json()

            if not isinstance(data, list):
                raise Exception(
                    f"Cookie manager returned unexpected format: {type(data)} — {str(data)[:200]}"
                )

            if not data:
                raise Exception("Cookie manager returned empty cookie list.")

            cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in data])
            logger.info(f"Successfully fetched {len(data)} cookies ({len(cookie_str)} bytes)")
            return cookie_str

        except Exception as e:
            logger.error(f"Failed to fetch cookies from cookie_manager: {e}")
            raise

    def trigger_refresh(self) -> None:
        """Trigger cookie refresh on cookie manager service"""
        try:
            httpx.post(f"{self.cookie_manager_url}/refresh")
            logger.info("Cookie refresh triggered on cookie manager")
        except Exception as e:
            logger.error(f"Failed to trigger refresh: {e}")

    @staticmethod
    def _get_extraction_prompt() -> str:
        return (
            "Извлеките данные об объекте недвижимости строго в соответствии со схемой и описаниями полей. "
            "ОЧЕНЬ ВАЖНО: \n"
            "1. Цена (price, price_per_m2) и площадь (area) должны быть ТОЛЬКО голыми числами (например 20000000 или 45.5), без пробелов, запятых, слов 'руб' или 'м2'.\n"
            "2. Округ (okrug) должен быть строго одной из аббревиатур: ЦАО, САО, СВАО, ВАО, ЮВАО, ЮАО, ЮЗАО, ЗАО, СЗАО, ЗелАО, Троицкий, Новомосковский.\n"
            "3. Этажи (current и all) должны быть только целыми числами.\n"
            "4. Идентификатор (cian_id) только строка из цифр.\n"
            "Не добавляйте ничего лишнего. Если данных для поля нет или они конфликтуют с правилами, верните null."
        )

    def _fetch_from_firecrawl(self, url: str, cookies: str) -> Tuple[str, Dict[str, Any]]:
        logger.info(f"Calling Firecrawl for {url}")
        firecrawl_start = datetime.now()

        try:
            data = self.app.scrape(
                url=url,
                formats=[
                    "html",
                    {
                        "type": "json",
                        "schema": ParsedAdData.model_json_schema(),
                        "prompt": self._get_extraction_prompt(),
                    },
                ],
                only_main_content=False,
                headers={"cookie": cookies} if cookies else {},
                skip_tls_verification=False,
            )

            elapsed = (datetime.now() - firecrawl_start).total_seconds()
            logger.info(f"Firecrawl responded in {elapsed:.1f} seconds")

            return self._extract_response_data(data)

        except Exception as e:
            logger.error(f"Firecrawl API call failed for {url}: {e}")
            raise

    @staticmethod
    def _extract_response_data(data: Any) -> Tuple[str, Dict[str, Any]]:
        html_content = ""
        json_data = {}

        if isinstance(data, dict):
            res_data = data.get("data", data)
        elif hasattr(data, "data"):
            res_data = data.data
        else:
            res_data = data

        if isinstance(res_data, dict):
            html_content = res_data.get("html", "")
        elif hasattr(res_data, "html"):
            html_content = res_data.html

        if isinstance(res_data, dict):
            json_data = res_data.get("json", {})
        elif hasattr(res_data, "json"):
            json_data = res_data.json

        return html_content, json_data

    @staticmethod
    def _check_authentication(html_content: str) -> Tuple[bool, Optional[bool]]:
        if '"isAuthenticated":true' in html_content or '"isAuthenticated": true' in html_content:
            return True, True
        elif '"isAuthenticated":false' in html_content or '"isAuthenticated": false' in html_content:
            return True, False
        else:
            return False, None

    def _handle_authentication_failure(self, url: str) -> None:
        logger.warning(f"Authentication failed for {url}, triggering refresh...")
        self.trigger_refresh()

        logger.info(f"Waiting {COOKIE_REFRESH_RETRY_WAIT} seconds before retry...")
        time.sleep(COOKIE_REFRESH_RETRY_WAIT)

        try:
            new_cookies = self.get_cookies_str()
            if new_cookies:
                logger.info("New cookies obtained successfully")
            else:
                raise AuthenticationError("No new cookies available")
        except Exception as e:
            raise AuthenticationError(f"Failed to recover authentication: {e}")

    def _log_extracted_data(self, url: str, extracted_json: Dict[str, Any]) -> None:
        logger.info(f"=== FIRECRAWL EXTRACTED DATA FOR {url} ===")

        if not extracted_json:
            logger.warning("No data extracted")
            logger.info("=== END EXTRACTED DATA (EMPTY) ===")
            return

        for key, value in extracted_json.items():
            if isinstance(value, str) and len(value) > 500:
                logger.info(f"  {key}: {value[:500]}... (truncated {len(value)} chars)")
            else:
                logger.info(f"  {key}: {value}")

        logger.info("=== END EXTRACTED DATA ===")

    def parse_sync(self, url: str) -> ParsedAdData:
        start_time = datetime.now()
        logger.info(f"[PARSE START] Processing {url}")

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                cookie_str = self.get_cookies_str()

                html_content, extracted_json = self._fetch_from_firecrawl(url, cookie_str)

                auth_found, auth_value = self._check_authentication(html_content)
                logger.info(f"Auth check: found={auth_found}, value={auth_value}")

                if auth_found and not auth_value:
                    logger.warning("isAuthenticated is FALSE, retrying with new cookies...")
                    retry_count += 1
                    try:
                        self._handle_authentication_failure(url)
                        continue
                    except AuthenticationError:
                        if retry_count >= max_retries:
                            raise
                        continue

                elif not auth_found:
                    logger.warning("isAuthenticated not found in response, proceeding...")

                if not extracted_json:
                    logger.error(f"Firecrawl returned empty JSON for {url}")
                    raise ValueError("Empty JSON response from Firecrawl")

                self._log_extracted_data(url, extracted_json)

                extracted_json["url"] = url
                parsed_model = ParsedAdData(**extracted_json)

                total_elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(
                    f"[PARSE SUCCESS] Completed {url} in {total_elapsed:.1f}s "
                    f"(price={parsed_model.price}, area={parsed_model.area}, rooms={parsed_model.rooms})"
                )

                return parsed_model

            except Exception as e:
                retry_count += 1
                logger.error(f"Parse attempt {retry_count}/{max_retries} failed: {e}")

                if retry_count >= max_retries:
                    total_elapsed = (datetime.now() - start_time).total_seconds()
                    logger.error(f"[PARSE FAILED] {url} failed after {total_elapsed:.1f}s")
                    raise

                wait_time = 5 * retry_count
                logger.info(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)

    async def parse_async(self, url: str) -> ParsedAdData:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.parse_sync, url)