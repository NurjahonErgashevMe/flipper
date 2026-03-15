import os
import requests
import time
import logging
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    firecrawl_api_key: str
    spreadsheet_id: str
    credentials_path: str = os.path.join(os.path.dirname(__file__), "credentials.json")

    # Proxy Settings
    proxy_username: str
    proxy_password: str
    # proxy_host: str
    # proxy_port: int
    change_ip_url: str
    http_proxy: str

    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(__file__), "..", ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # @property
    # def proxy_url_http(self) -> str:
    #     return f"http://{self.proxy_username}:{self.proxy_password}@{self.proxy_host}:{self.proxy_port}"


settings = Settings()


def change_ip():
    """Сменить IP мобильного прокси и подождать."""
    logger.info("[*] Меняем IP прокси...")
    try:
        resp = requests.get(settings.change_ip_url, timeout=30)
        logger.info(
            f"[*] Ответ смены IP: {resp.status_code} — {resp.text.strip()[:200]}"
        )
    except Exception as e:
        logger.error(f"[!] Ошибка смены IP: {e}")

    wait = 5
    logger.info(f"[*] Ждём {wait} сек после смены IP...")
    time.sleep(wait)
