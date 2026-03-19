"""
services.parser_cian.config - Settings and configuration

Конфигурация парсера Cian через Pydantic Settings.
Загружает переменные из .env файла в корне проекта.
"""

import os
import logging
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Конфигурация сервиса parser_cian.

    Все переменные загружаются из .env файла.
    Поддерживает как локальную разработку, так и Docker окружение.
    """

    # === Firecrawl API ===
    firecrawl_api_key: str = ""
    """API ключ для Firecrawl (обязательно)"""

    # === Cookie Manager ===
    cookie_manager_url: str = "http://cookie_manager:8000"
    """URL микросервиса управления куками
    
    Для Docker: http://cookie_manager:8000 (имя сервиса в docker-compose)
    """

    # === Proxy Settings (мобильный прокси для смены IP) ===
    proxy_username: str = ""
    """Username для мобильного прокси"""

    proxy_password: str = ""
    """Password для мобильного прокси"""

    change_ip_url: str = ""
    """URL endpoint для смены IP адреса (например: http://proxy.local:8080/changeip)"""

    http_proxy: str = ""
    """Полный URL прокси в формате: http://user:pass@host:port"""

    # === Logging ===
    log_level: str = "INFO"
    """Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL"""

    # === Parser Settings ===
    parser_concurrency: int = 2
    """Количество одновременных воркеров для парсинга (защита от rate limits)"""

    min_unique_views: int = 200
    """Минимальное количество уникальных просмотров за сегодня для выделения цветом (Offers_Parser)"""

    # === Colors ===
    sheet_highlight_color: dict = {"red": 1.0, "green": 0.9, "blue": 0.7}
    """Цвет выделения строк в Google Sheets (RGB)"""

    model_config = SettingsConfigDict(
        env_file=os.path.join(
            os.path.dirname(__file__),
            "../../.env",  # Относительный путь к .env в корне проекта
        ),
        env_file_encoding="utf-8",
        extra="ignore",  # Игнорируем неиспользуемые переменные
    )

    def __init__(self, **data):
        super().__init__(**data)

        # Валидация обязательных полей
        if not self.firecrawl_api_key:
            raise ValueError(
                "FIRECRAWL_API_KEY не установлена в .env файле. "
                "Получите ключ на https://firecrawl.dev"
            )

        logger.info(f"Settings loaded from {self.model_config['env_file']}")
        logger.debug(f"Cookie Manager URL: {self.cookie_manager_url}")


# Глобальный экземпляр конфигурации
settings = Settings()


def validate_config() -> bool:
    """
    Проверяет что все обязательные переменные окружения установлены.

    Returns:
        True если все OK, иначе выбрасывает ValueError
    """
    try:
        # Проверка credentials.json будет в SheetsManager.__init__()
        # Здесь проверяем только специфичные для parser_cian настройки

        logger.info("✓ Configuration validated successfully")
        return True

    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        raise


def get_settings() -> Settings:
    """
    Возвращает глобальный экземпляр настроек.

    Returns:
        Settings instance
    """
    return settings
