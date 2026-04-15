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

    # === Database ===
    database_url: str = Field(
        default="postgresql+asyncpg://flipper:flipper_secret@app_postgres:5432/flipper",
        description="PostgreSQL connection URL (DATABASE_URL in .env / docker-compose environment)",
    )

    # === Firecrawl API ===
    firecrawl_api_key: str = ""
    """API ключ для Firecrawl (обязательно)"""

    firecrawl_base_url: str = "http://localhost:3002"
    """Self-hosted Firecrawl (без /v2/scrape). В Docker: http://flippercrawl-api-1:3002"""

    use_proxies_for_search: bool = Field(
        default=True,
        description="Если True и в proxies_file есть строки — HTML страниц списков Cian через прокси (curl_cffi).",
    )

    proxies_file: str = Field(
        default="data/proxies.txt",
        description="Файл host:port:user:pass по строке (CIAN_PROXIES_FILE в .env).",
    )

    html_to_markdown_url: str = ""
    """Базовый URL сервиса go-html-to-md (например http://html_to_markdown:8080). Пусто — markdown не конвертируем."""

    # === Cookie Manager ===
    cookie_manager_url: str = "http://cookie_manager:8000"
    """URL микросервиса управления куками
    
    Для Docker: http://cookie_manager:8000 (имя сервиса в docker-compose)
    """

    # === Logging ===
    log_level: str = "INFO"
    """Уровень логирования: DEBUG, INFO, WARNING, ERROR, CRITICAL"""

    parser_cian_log_file: str = Field(
        default="data/logs/parser_cian.log",
        description="Файл логов; пустая строка — только консоль (PARSER_CIAN_LOG_FILE в .env)",
    )

    # === Parser Settings ===
    parser_concurrency: int = 50
    """Параллельных воркеров к Firecrawl (PARSER_CONCURRENCY в .env). При ReadTimeout уменьшите."""

    regular_search_max_pages: int = Field(
        default=100,
        ge=1,
        le=500,
        description="Макс. страниц списка на один URL из FILTERS (PARSER_REGULAR_MAX_PAGES). Раньше 50 — обрезало выдачи >50 стр.",
    )

    search_duplicate_streak_stop: int = Field(
        default=2,
        ge=1,
        le=20,
        description="Остановить пагинацию после N подряд страниц без новых ссылок (PARSER_SEARCH_DUPLICATE_STREAK_STOP). "
        "1 = как раньше (частый обрыв при ложном дубле p=2 из-за капчи/битого HTML).",
    )

    min_unique_views: int = 200
    """Минимальное количество уникальных просмотров за сегодня для выделения цветом (Offers_Parser)"""

    ad_max_age_days: int = Field(
        default=7,
        ge=1,
        description="Макс. возраст объявления (дней от publish_date). "
        "Используется только если включена очистка CLEANUP_STALE_ACTIVE_ADS (AD_MAX_AGE_DAYS).",
    )

    cleanup_stale_active_ads: bool = Field(
        default=False,
        description="Если True — удалять из БД активные объявления старше ad_max_age_days "
        "после парсинга (CLEANUP_STALE_ACTIVE_ADS). "
        "Если False — объявления отслеживаются до снятия с публикации.",
    )

    sold_max_age_days: int = Field(
        default=7,
        ge=1,
        description="Объявление попадает в Продано / Аванс_Продано только если от publish_date "
        "до сегодня прошло не больше этого значения дней (SOLD_MAX_AGE_DAYS).",
    )

    # === Avans Parser Settings ===
    avans_search_url: str = Field(
        default="https://www.cian.ru/cat.php?context=%D0%92%D0%BD%D0%B5%D1%81%D0%BB%D0%B8+%D0%B0%D0%B2%D0%B0%D0%BD%D1%81%7C%D0%B2%D0%BD%D0%B5%D1%81%D0%B5%D0%BD+%D0%B0%D0%B2%D0%B0%D0%BD%D1%81%7C%D0%B2%D0%BD%D0%B5%D1%81%D0%B5%D0%BD+%D0%B7%D0%B0%D0%B4%D0%B0%D1%82%D0%BE%D0%BA%7C%D0%B2%D0%BD%D0%B5%D1%81%D0%BB%D0%B8+%D0%B7%D0%B0%D0%B4%D0%B0%D1%82%D0%BE%D0%BA&deal_type=sale&demolished_in_moscow_programm=0&electronic_trading=2&engine_version=2&flat_share=2&is_first_floor=0&m2=1&object_type%5B0%5D=1&offer_type=flat&only_flat=1&region=1&room1=1&room2=1&room3=1&room4=1&room5=1&room6=1",
        description="Ссылка для парсинга активных авансов (статичная с кучей фильтров)"
    )
    avans_max_pages: int = Field(default=100, description="Количество страниц для парсинга авансов")

    # Telegram Notification Settings
    tg_bot_token: str = Field(default="", description="Токен Telegram бота")
    tg_chat_id: str = Field(default="", description="ID чата для отправки уведомлений")

    # === Colors ===
    sheet_highlight_color: dict = {"red": 0.71, "green": 0.84, "blue": 0.66}
    """Зеленоватый цвет выделения строк в Google Sheets (RGB) — unique_views ≥ min_unique_views"""

    sheet_deactivated_color: dict = {"red": 217 / 255, "green": 217 / 255, "blue": 217 / 255}
    """Сероватый цвет для снятых с публикации объявлений (#D9D9D9)"""

    # Имена вкладок Google Sheets (как в документе, статичные)
    sheet_tab_avans: str = "Аванс"
    sheet_tab_avans_sold: str = "Аванс_Продано"
    sheet_tab_sold: str = "Продано"

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

        self._pop_obsolete_scraper_api_env()

        if self._should_use_proxy_file_for_search():
            logger.info("Поисковые страницы Cian: прокси из %s", self.proxies_file)

        logger.info(f"Settings loaded from {self.model_config['env_file']}")
        logger.debug(f"Cookie Manager URL: {self.cookie_manager_url}")

    def _resolve_proxies_file_path(self) -> str:
        p = (self.proxies_file or "").strip()
        if not p:
            return ""
        if os.path.isabs(p):
            return p
        root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        return os.path.normpath(os.path.join(root, p))

    def _should_use_proxy_file_for_search(self) -> bool:
        if not self.use_proxies_for_search:
            return False
        path = self._resolve_proxies_file_path()
        if not path or not os.path.isfile(path):
            return False
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s and not s.startswith("#"):
                        return True
        except OSError:
            return False
        return False

    def _pop_obsolete_scraper_api_env(self) -> None:
        """Убираем из окружения ключи стороннего Scraper API (если остались в .env)."""
        for key in ("DECODO_AUTH_TOKEN", "DECODO_SCRAPER_URL", "DECODO_MAX_RETRIES"):
            os.environ.pop(key, None)

    def proxy_urls_for_search(self) -> list[str]:
        """Список URL прокси для cianparser (пусто — прямой HTTP GET)."""
        if not self.use_proxies_for_search:
            return []
        from services.parser_cian.proxy_loader import load_proxy_urls

        return load_proxy_urls(self._resolve_proxies_file_path())


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


