"""
services.parser_cian.search_parser - Search URL extraction with cianparser

Модуль для извлечения индивидуальных ссылок объявлений из поисковых страниц Cian.
Использует локальную встроенную библиотеку cianparser.
"""

import re
import logging
from typing import List, Optional, Sequence, Dict

logger = logging.getLogger(__name__)


def extract_ad_urls_from_search(
    search_url: str,
    location: str = "Москва",
    max_pages: int = 50,
    http_proxy: Optional[str] = None,
    proxy_urls: Optional[Sequence[str]] = None,
    list_html_loader=None,
    duplicate_streak_to_stop: int = 2,
) -> List[str]:
    """
    Извлекает ссылки на объявления из поисковой страницы Cian.
    
    Args:
        search_url: URL поисковой страницы (например, с фильтрами)
        location: Город (по умолчанию "Москва")
        max_pages: Максимальное количество страниц пагинации для парсинга (по умолчанию 50)
        http_proxy: один URL прокси (если proxy_urls не задан)
        proxy_urls: список прокси для ProxyPool (резидентские и т.д.) — приоритет над http_proxy
        list_html_loader: опциональный callable(url) -> str для HTML списков (иначе прямой GET + прокси)
        duplicate_streak_to_stop: стоп после стольких подряд страниц без новых URL (ложный дубль первых страниц)

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
    
    logger.info(f"Extracting URLs from search page: {search_url} (up to {max_pages} pages)")
    
    # Определяем тип сделки (продажа или аренда)
    deal_type = "sale" if "sale" in search_url else "rent"
    
    try:
        # Инициализируем парсер
        if proxy_urls:
            proxies = list(proxy_urls)
        elif http_proxy:
            proxies = [http_proxy]
        else:
            proxies = None
        parser = cianparser.CianParser(
            location=location,
            proxies=proxies,
            list_html_loader=list_html_loader,
        )
        
        all_urls = set()
        dup_streak = 0
        streak_n = max(1, int(duplicate_streak_to_stop or 1))

        for page in range(1, max_pages + 1):
            logger.info(f"Parsing search page {page} / {max_pages}")
            
            # Настраиваем парсер для извлечения ссылок для конкретной страницы
            parser.__parser__ = FlatListPageParser(
                session=parser.__session__,
                accommodation_type="flat",
                deal_type=deal_type,
                rent_period_type=None,
                location_name=location,
                with_saving_csv=False,
                with_extra_data=False,
                additional_settings={"start_page": page, "end_page": page},
            )
            
            # Форматируем URL для парсера (заменяем номер страницы на {})
            if "&p=" in search_url or "?p=" in search_url:
                url_format = re.sub(r"([&?])p=\d+", r"\g<1>p={}", search_url)
            else:
                separator = "&" if "?" in search_url else "?"
                url_format = search_url + f"{separator}p={{}}"
            
            # Запускаем парсер только для текущей страницы
            parser.__run__(url_format)
            
            # Извлекаем ссылки и сразу отрезаем мусорные параметры (?context=... и т.д.)
            parsed_data = parser.__parser__.result
            page_urls = []
            for item in parsed_data:
                if "url" in item:
                    clean_url = item["url"].split("?")[0]
                    if clean_url.endswith("/"):
                        clean_url = clean_url[:-1]  # Нормализуем слеш
                    clean_url += "/" # Добавим обратно чтобы все были /
                    page_urls.append(clean_url)
            
            if not page_urls:
                logger.info("No URLs found on this page. Stopping pagination.")
                break

            # Те же карточки, что уже видели (часто p=2 = копия p=1 из-за капчи/битого HTML).
            # Одна такая страница не должна обрывать весь обход — ждём streak подряд.
            new_urls = set(page_urls) - all_urls
            if not new_urls:
                dup_streak += 1
                logger.warning(
                    f"Page {page}: нет новых ссылок (дубликат выдачи), подряд {dup_streak}/{streak_n}"
                )
                if dup_streak >= streak_n:
                    logger.info("Stopping pagination: лимит подряд дублирующихся страниц.")
                    break
                continue

            dup_streak = 0
            all_urls.update(page_urls)
            logger.info(f"Added {len(new_urls)} new URLs. Total unique so far: {len(all_urls)}")
            
        final_urls = list(all_urls)
        logger.info(f"Completed extracting {len(final_urls)} total unique ad URLs")
        
        return final_urls
        
    except Exception as e:
        logger.error(f"Failed to extract URLs from search page: {e}", exc_info=True)
        raise


def extract_batch_from_searches(
    search_urls: List[str],
    location: str = "Москва",
    max_pages: int = 50,
    http_proxy: Optional[str] = None,
    proxy_urls: Optional[Sequence[str]] = None,
    change_ip_callback=None,
    list_html_loader=None,
    duplicate_streak_to_stop: int = 2,
) -> List[str]:
    """
    Извлекает ссылки из нескольких поисковых страниц.
    
    Args:
        search_urls: Список поисковых URL
        location: Город
        max_pages: Максимальное количество страниц для каждой поисковой ссылки
        http_proxy: один URL прокси
        proxy_urls: список прокси (ротация через cianparser.ProxyPool)
        change_ip_callback: Функция для смены IP перед каждой страницей
        list_html_loader: callable(url) -> str — внешняя загрузка HTML списков
        duplicate_streak_to_stop: подряд страниц без новых URL перед остановкой

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
                max_pages=max_pages,
                http_proxy=http_proxy,
                proxy_urls=proxy_urls,
                list_html_loader=list_html_loader,
                duplicate_streak_to_stop=duplicate_streak_to_stop,
            )
            all_ad_urls.extend(urls)
            logger.info(f"Added {len(urls)} URLs from search page {i}")
            
        except Exception as e:
            logger.error(f"Skipping search URL {i} due to error: {e}")
            continue
    
    logger.info(f"Total ad URLs extracted: {len(all_ad_urls)}")
    return all_ad_urls


def extract_urls_by_searches(
    search_urls: List[str],
    location: str = "Москва",
    max_pages: int = 50,
    http_proxy: Optional[str] = None,
    proxy_urls: Optional[Sequence[str]] = None,
    change_ip_callback=None,
    list_html_loader=None,
    duplicate_streak_to_stop: int = 2,
) -> Dict[str, List[str]]:
    """
    То же, что extract_batch_from_searches, но возвращает отображение:
    search_url -> список ссылок объявлений.
    """
    urls_by_search: Dict[str, List[str]] = {}

    for i, search_url in enumerate(search_urls or [], 1):
        if not search_url:
            continue
        logger.info(f"Processing search URL {i}/{len(search_urls)}")

        if change_ip_callback:
            try:
                change_ip_callback()
            except Exception as e:
                logger.warning(f"Failed to change IP: {e}")

        try:
            urls = extract_ad_urls_from_search(
                search_url=search_url,
                location=location,
                max_pages=max_pages,
                http_proxy=http_proxy,
                proxy_urls=proxy_urls,
                list_html_loader=list_html_loader,
                duplicate_streak_to_stop=duplicate_streak_to_stop,
            )
            urls_by_search[search_url] = urls
            logger.info(f"Added {len(urls)} URLs from search page {i}")
        except Exception as e:
            logger.error(f"Skipping search URL {i} due to error: {e}")
            continue

    logger.info(
        "Total search URLs processed: %s; total extracted URLs: %s",
        len(urls_by_search),
        sum(len(v) for v in urls_by_search.values()),
    )
    return urls_by_search
