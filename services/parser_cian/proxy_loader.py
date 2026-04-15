"""
Загрузка HTTP(S) прокси из текстового файла (резидентские и др.).

Формат строки: host:port:username:password
(пароль может содержать «:» — тогда всё после третьего «:» считается паролем.)
"""

from __future__ import annotations

import logging
import os
from typing import List
from urllib.parse import quote

logger = logging.getLogger(__name__)


def _line_to_proxy_url(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    parts = line.split(":")
    if len(parts) < 4:
        logger.warning("Пропуск строки прокси (нужно host:port:user:password): %s", line[:120])
        return None
    host, port, user = parts[0], parts[1], parts[2]
    password = ":".join(parts[3:])
    user_q = quote(user, safe="")
    pass_q = quote(password, safe="")
    return f"http://{user_q}:{pass_q}@{host}:{port}"


def load_proxy_urls(path: str) -> List[str]:
    """
    Читает файл построчно, возвращает список URL для curl_cffi/ProxyPool.
    """
    if not path or not os.path.isfile(path):
        return []
    out: List[str] = []
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                u = _line_to_proxy_url(raw)
                if u:
                    out.append(u)
    except OSError as e:
        logger.warning("Не удалось прочитать %s: %s", path, e)
        return []
    return out
