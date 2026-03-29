"""
services.parser_cian.config - Settings and configuration

Конфигурация парсера Cian через Pydantic Settings.
Загружает переменные из .env файла в корне проекта.
"""

import os
import logging

from pydantic import Field
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

    firecrawl_base_url: str = "http://localhost:3002"
    """Self-hosted Firecrawl (без /v2/scrape). В Docker: http://flippercrawl-api-1:3002"""

    # === Decodo Scraper API (HTML списков объявлений, обход бана IP) ===
    # Подключение: CianParser сам читает DECODO_* из окружения (приоритет над прокси для списков).

    decodo_scraper_url: str = "https://scraper-api.decodo.com/v2/scrape"
    """Endpoint v2/scrape"""

    decodo_auth_token: str = ""
    """Decodo Scraper API: base64 для заголовка Authorization: Basic <token> (как в кабинете)"""

    decodo_max_retries: int = 5
    """Повторы запроса при капче/битом HTML"""

    html_to_markdown_url: str = ""
    """Базовый URL сервиса go-html-to-md (например http://html_to_markdown:8080). Пусто — markdown не тянем из Decodo-потока."""

    # === Cookie Manager ===
    cookie_manager_url: str = "http://cookie_manager:8000"
    """URL микросервиса управления куками
    
    Для Docker: http://cookie_manager:8000 (имя сервиса в docker-compose)
    """

    # === Logging ===
    log_level: str = "INFO"
    """Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL"""

    # === Parser Settings ===
    parser_concurrency: int = 20
    """Параллельных воркеров к Firecrawl (PARSER_CONCURRENCY в .env).
    Лимит инстанса flippercrawl-api; при ReadTimeout уменьшите. Google Sheets — max ~60 read/min (SHEETS_READ_SPACING_SEC)."""

    min_unique_views: int = 200
    """Минимальное количество уникальных просмотров за сегодня для выделения цветом (Offers_Parser)"""

    # === Avans Parser Settings ===
    avans_search_url: str = Field(
        default="https://www.cian.ru/cat.php?context=%D0%92%D0%BD%D0%B5%D1%81%D0%BB%D0%B8+%D0%B0%D0%B2%D0%B0%D0%BD%D1%81%7C%D0%B2%D0%BD%D0%B5%D1%81%D0%B5%D0%BD+%D0%B0%D0%B2%D0%B0%D0%BD%D1%81%7C%D0%B2%D0%BD%D0%B5%D1%81%D0%B5%D0%BD+%D0%B7%D0%B0%D0%B4%D0%B0%D1%82%D0%BE%D0%BA%7C%D0%B2%D0%BD%D0%B5%D1%81%D0%BB%D0%B8+%D0%B7%D0%B0%D0%B4%D0%B0%D1%82%D0%BE%D0%BA&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&is_first_floor=0&m2=1&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1",
        description="Ссылка для парсинга активных авансов (статичная с кучей фильтров)"
    )
    avans_max_pages: int = Field(default=2, description="Количество страниц для парсинга авансов")

    # Telegram Notification Settings
    tg_bot_token: str = Field(default="", description="Токен Telegram бота")
    tg_chat_id: str = Field(default="", description="ID чата для отправки уведомлений")

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

        if not self.firecrawl_api_key:
            raise ValueError(
                "FIRECRAWL_API_KEY не установлена в .env файле. "
                "Получите ключ на https://firecrawl.dev"
            )

        self._push_decodo_to_environ()

        logger.info(f"Settings loaded from {self.model_config['env_file']}")
        logger.debug(f"Cookie Manager URL: {self.cookie_manager_url}")

    def _push_decodo_to_environ(self) -> None:
        """Pydantic Settings загружает .env в поля, но не в os.environ.
        CianParser.decodo_scraper читает os.environ — синхронизируем."""
        _pairs = [
            ("DECODO_AUTH_TOKEN", (self.decodo_auth_token or "").strip()),
            ("DECODO_SCRAPER_URL", (self.decodo_scraper_url or "").strip()),
            ("DECODO_MAX_RETRIES", str(self.decodo_max_retries)),
        ]
        for key, val in _pairs:
            if val and not os.environ.get(key, "").strip():
                os.environ[key] = val


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


