"""
services.parser_cian.search_parser - Search URL extraction with cianparser

Модуль для извлечения индивидуальных ссылок объявлений из поисковых страниц Cian.
Использует локальную встроенную библиотеку cianparser.
"""

import re
import logging
from typing import List

logger = logging.getLogger(__name__)


def extract_ad_urls_from_search(
    search_url: str,
    location: str = "Москва",
    max_urls: int = 3,
    http_proxy: str = None
) -> List[str]:
    """
    Извлекает ссылки на объявления из поисковой страницы Cian.
    
    Args:
        search_url: URL поисковой страницы (например, с фильтрами)
        location: Город (по умолчанию "Москва")
        max_urls: Сколько первых ссылок вернуть (по умолчанию 3)
        http_proxy: URL прокси (если нужно)
        
    Returns:
        Список URLs объявлений
        
    Raises:
        ImportError: Если cianparser недоступен
        Exception: Если парсинг поисковой страницы не удался
    """
    try:
        # Импортируем локальный cianparser
        import cianparser
        from cianparser.flat.list import FlatListPageParser
    except ImportError as e:
        logger.error(f"cianparser not available: {e}")
        raise ImportError("Ensure cianparser is installed in services/parser_cian/cianparser/")
    
    logger.info(f"Extracting URLs from search page: {search_url}")
    
    # Определяем тип сделки (продажа или аренда)
    deal_type = "sale" if "sale" in search_url else "rent"
    
    try:
        # Инициализируем парсер
        proxies = [http_proxy] if http_proxy else None
        parser = cianparser.CianParser(
            location=location,
            proxies=proxies
        )
        
        # Настраиваем парсер для извлечения ссылок
        parser.__parser__ = FlatListPageParser(
            session=parser.__session__,
            accommodation_type="flat",
            deal_type=deal_type,
            rent_period_type=None,
            location_name=location,
            with_saving_csv=False,
            with_extra_data=False,
            additional_settings={"start_page": 1, "end_page": 1},
        )
        
        # Форматируем URL для парсера (заменяем номер страницы на {})
        if "&p=" in search_url or "?p=" in search_url:
            url_format = re.sub(r"([&?])p=\d+", r"\g<1>p={}", search_url)
        else:
            separator = "&" if "?" in search_url else "?"
            url_format = search_url + f"{separator}p={{}}"
        
        logger.debug(f"URL format for parser: {url_format}")
        
        # Запускаем парсер
        parser.__run__(url_format)
        
        # Извлекаем ссылки
        parsed_data = parser.__parser__.result
        all_urls = [item["url"] for item in parsed_data if "url" in item]
        
        logger.info(f"Found {len(all_urls)} ad URLs on search page")
        
        # Возвращаем первые max_urls
        extracted = all_urls[:max_urls]
        logger.info(f"Returning top {len(extracted)} URLs: {extracted}")
        
        return extracted
        
    except Exception as e:
        logger.error(f"Failed to extract URLs from search page: {e}", exc_info=True)
        raise


def extract_batch_from_searches(
    search_urls: List[str],
    location: str = "Москва",
    max_urls_per_search: int = 3,
    http_proxy: str = None,
    change_ip_callback=None
) -> List[str]:
    """
    Извлекает ссылки из нескольких поисковых страниц.
    
    Args:
        search_urls: Список поисковых URL
        location: Город
        max_urls_per_search: Количество ссылок с каждой поисковой страницы
        http_proxy: URL прокси
        change_ip_callback: Функция для смены IP перед каждой страницей
        
    Returns:
        Объединенный список всех извлеченных ссылок объявлений
    """
    all_ad_urls = []
    
    for i, search_url in enumerate(search_urls, 1):
        logger.info(f"Processing search URL {i}/{len(search_urls)}")
        
        # Меняем IP если нужно
        if change_ip_callback:
            try:
                change_ip_callback()
            except Exception as e:
                logger.warning(f"Failed to change IP: {e}")
        
        try:
            urls = extract_ad_urls_from_search(
                search_url=search_url,
                location=location,
                max_urls=max_urls_per_search,
                http_proxy=http_proxy
            )
            all_ad_urls.extend(urls)
            logger.info(f"Added {len(urls)} URLs from search page {i}")
            
        except Exception as e:
            logger.error(f"Skipping search URL {i} due to error: {e}")
            continue
    
    logger.info(f"Total ad URLs extracted: {len(all_ad_urls)}")
    return all_ad_urls
