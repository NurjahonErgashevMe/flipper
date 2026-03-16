"""
flipper_core.utils - Common utilities for all services

Общие функции используемые несколькими микросервисами.
"""

import logging
import time
import requests

logger = logging.getLogger(__name__)


def change_ip(change_ip_url: str, timeout: int = 30, wait_after: int = 5) -> bool:
    """
    Меняет IP адрес через мобильный прокси.
    
    Args:
        change_ip_url: URL endpoint для смены IP (например, http://proxy:8080/changeip)
        timeout: Timeout для запроса в секундах
        wait_after: Время ожидания после смены IP в секундах
        
    Returns:
        True если успешно, False если ошибка
    """
    logger.info(f"[*] Меняем IP прокси (URL: {change_ip_url})...")
    try:
        resp = requests.get(change_ip_url, timeout=timeout)
        logger.info(f"[*] Ответ смены IP: {resp.status_code} — {resp.text.strip()[:200]}")
        
        if resp.status_code == 200:
            logger.info(f"[*] Ждём {wait_after} сек после смены IP...")
            time.sleep(wait_after)
            return True
        else:
            logger.warning(f"[!] Неожиданный статус код: {resp.status_code}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error(f"[!] Timeout при смене IP (>{timeout}s)")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"[!] Ошибка подключения к прокси: {e}")
        return False
    except Exception as e:
        logger.error(f"[!] Ошибка смены IP: {e}")
        return False


def retry_with_backoff(
    func,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 30.0,
    backoff_factor: float = 2.0,
):
    """
    Декоратор для повторных попыток с экспоненциальной задержкой.
    
    Args:
        func: Функция для выполнения
        max_retries: Максимальное количество попыток
        initial_delay: Начальная задержка в секундах
        max_delay: Максимальная задержка в секундах
        backoff_factor: Множитель для экспоненциального увеличения задержки
        
    Returns:
        Результат функции или None если все попытки неудачны
    """
    def wrapper(*args, **kwargs):
        delay = initial_delay
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Attempt {attempt + 1}/{max_retries} for {func.__name__}")
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                else:
                    logger.error(f"All {max_retries} attempts failed for {func.__name__}")
        
        raise last_exception
    
    return wrapper


def log_section(title: str):
    """
    Логирует заголовок секции для читаемости логов.
    
    Args:
        title: Название секции
    """
    separator = "=" * (len(title) + 4)
    logger.info(separator)
    logger.info(f"  {title}")
    logger.info(separator)
