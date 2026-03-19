"""Client for HTML to Markdown conversion service."""

import os
import requests
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed


class HTMLToMarkdownConverter:
    """Client for converting HTML to Markdown using the Go service."""

    def __init__(
        self, 
        base_url: Optional[str] = None, 
        max_workers: int = 5
    ):
        """
        Initialize the converter client.

        Args:
            base_url: Base URL of the HTML to Markdown service.
                     If None, auto-detects based on environment:
                     - Inside Docker: http://html_to_markdown:8080
                     - Outside Docker: http://localhost:8090
            max_workers: Maximum number of parallel requests
        """
        if base_url is None:
            # Auto-detect: внутри Docker используем имя сервиса
            if os.path.exists("/.dockerenv"):
                base_url = "http://html_to_markdown:8080"
            else:
                base_url = "http://localhost:8090"
        
        self.base_url = base_url.rstrip("/")
        self.convert_url = f"{self.base_url}/convert"
        self.health_url = f"{self.base_url}/health"
        self.max_workers = max_workers

    def is_healthy(self) -> bool:
        """
        Check if the service is healthy and available.

        Returns:
            True if service is healthy, False otherwise
        """
        try:
            response = requests.get(self.health_url, timeout=5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def convert(self, html: str, timeout: int = 60) -> str:
        """
        Convert HTML to Markdown.

        Args:
            html: HTML content to convert
            timeout: Request timeout in seconds

        Returns:
            Converted Markdown string

        Raises:
            ValueError: If HTML is empty or conversion fails
            requests.RequestException: If request fails
        """
        if not html:
            raise ValueError("HTML content cannot be empty")

        payload = {"html": html}

        try:
            response = requests.post(
                self.convert_url,
                json=payload,
                timeout=timeout,
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()

            data = response.json()

            if not data.get("success"):
                error_msg = data.get("error", "Unknown error")
                details = data.get("details", "")
                raise ValueError(f"Conversion failed: {error_msg}. {details}")

            return data.get("markdown", "")

        except requests.RequestException as e:
            raise requests.RequestException(
                f"Failed to communicate with HTML to Markdown service: {e}"
            ) from e

    def convert_batch(
        self, 
        html_list: List[str], 
        timeout: int = 60
    ) -> List[Dict[str, any]]:
        """
        Convert multiple HTML documents in parallel.

        Args:
            html_list: List of HTML content strings to convert
            timeout: Request timeout in seconds per request

        Returns:
            List of dicts with 'index', 'markdown' (if success), and 'error' (if failed)
        """
        results = []

        def convert_single(index: int, html: str):
            try:
                markdown = self.convert(html, timeout=timeout)
                return {"index": index, "markdown": markdown, "error": None}
            except Exception as e:
                return {"index": index, "markdown": None, "error": str(e)}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(convert_single, i, html): i 
                for i, html in enumerate(html_list)
            }

            for future in as_completed(futures):
                results.append(future.result())

        # Sort by original index
        results.sort(key=lambda x: x["index"])
        return results
