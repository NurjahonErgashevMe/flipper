import asyncio
import os
import sys
import logging
import re
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'services', 'parser_cian'))

from services.parser_cian.parser import AdParser

load_dotenv()

async def debug_html():
    if not os.getenv("FIRECRAWL_API_KEY"):
        logger.error("❌ Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    parser = AdParser(cookie_manager_url="http://localhost:8000")
    url = "https://www.cian.ru/sale/flat/326100259/"
    
    logger.info(f"🔍 Получаем HTML для отладки...")
    logger.info(f"📍 URL: {url}")
    
    # Получаем куки
    cookies_str = await parser._get_cookies()
    
    # Запрос к Firecrawl
    import httpx
    
    payload = {
        "url": url,
        "formats": ["rawHtml"],  # rawHtml возвращает оригинальный HTML со всеми script тегами
        "waitFor": 0,
        "headers": {
            "Cookie": cookies_str
        } if cookies_str else {}
    }
    
    headers = {
        "Authorization": f"Bearer {parser.api_key}",
        "Content-Type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            parser.firecrawl_api_url,
            json=payload,
            headers=headers
        )
    
    if response.status_code == 200:
        result = response.json()
        if result.get("success") and "data" in result:
            html = result["data"].get("rawHtml", "")  # Используем rawHtml вместо html
            
            # Сохраняем HTML
            with open("debug_data.html", "w", encoding="utf-8") as f:
                f.write(html)
            logger.info(f"💾 HTML сохранен в debug_data.html ({len(html)} символов)")
            
            # Ищем creationDate разными паттернами
            patterns = [
                (r'"creationDate"\s*:\s*"(\d{4}-\d{2}-\d{2})T[^"]*"', "Pattern 1: с T и временем"),
                (r'"creationDate"\s*:\s*"(\d{4}-\d{2}-\d{2})"', "Pattern 2: только дата"),
                (r'creationDate["\']?\s*:\s*["\']?(\d{4}-\d{2}-\d{2})', "Pattern 3: гибкий"),
            ]
            
            logger.info("\n🔎 Поиск creationDate в HTML:")
            found = False
            for pattern, description in patterns:
                match = re.search(pattern, html)
                if match:
                    logger.info(f"✅ {description}: {match.group(1)}")
                    found = True
                else:
                    logger.info(f"❌ {description}: не найдено")
            
            if not found:
                # Ищем любое упоминание creation
                creation_mentions = re.findall(r'.{0,50}creation.{0,50}', html, re.IGNORECASE)
                if creation_mentions:
                    logger.info(f"\n📝 Найдено {len(creation_mentions)} упоминаний 'creation':")
                    for i, mention in enumerate(creation_mentions[:5], 1):
                        logger.info(f"  {i}. {mention}")
                else:
                    logger.warning("⚠️ Слово 'creation' вообще не найдено в HTML")
        else:
            logger.error("❌ Нет данных в ответе Firecrawl")
    else:
        logger.error(f"❌ Ошибка запроса: {response.status_code}")

if __name__ == "__main__":
    asyncio.run(debug_html())
