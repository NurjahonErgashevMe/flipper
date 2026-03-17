import httpx
import json
import os
from dotenv import load_dotenv

load_dotenv()

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")
COOKIE_MANAGER_URL = "http://localhost:8000/cookies"
# TARGET_URL = "https://www.cian.ru/sale/flat/327607745/"
TARGET_URL = "https://www.cian.ru/sale/flat/326100259/"

def get_cookies():
    """Получаем куки из Cookie Manager"""
    try:
        response = httpx.get(COOKIE_MANAGER_URL, timeout=10)
        response.raise_for_status()
        cookies = response.json()
        print(f"✅ Получено {len(cookies)} кук из Cookie Manager")
        return cookies
    except Exception as e:
        print(f"❌ Ошибка получения кук: {e}")
        return []

def scrape_with_firecrawl(url: str, cookies: list):
    """Парсим URL через Firecrawl с куками"""
    
    # Формируем cookie string (как в parser.py)
    cookie_str = ""
    if cookies:
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
    
    payload = {
        "url": url,
        "formats": ["html", "markdown"],
        "waitFor": 0,
        "onlyMainContent": True,
        "headers": {
            "Cookie": cookie_str
        } if cookie_str else {}
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}"
    }
    
    print(f"🚀 Отправляем запрос в Firecrawl API v2...")
    print(f"📍 URL: {url}")
    print(f"🍪 Куки: {len(cookies)}")
    
    try:
        response = httpx.post(
            "https://api.firecrawl.dev/v2/scrape",
            json=payload,
            headers=headers,
            timeout=60
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get("success"):
            print(f"✅ Парсинг успешен!")
            return data.get("data", {})
        else:
            print(f"❌ Ошибка: {data}")
            return None
            
    except Exception as e:
        print(f"❌ Ошибка запроса: {e}")
        return None

def save_html(html_content: str, filename: str = "history.html"):
    """Сохраняем HTML в файл"""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"💾 HTML сохранен в {filename}")
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")

def main():
    print("=" * 60)
    print("🔥 Firecrawl Test with Cookies")
    print("=" * 60)
    
    # Получаем куки
    cookies = get_cookies()
    
    # Парсим
    result = scrape_with_firecrawl(TARGET_URL, cookies)
    
    if result:
        data = result.get("data", {})
        html = data.get("html", "")
        markdown = data.get("markdown", "")
        
        print(f"\n📊 Результат:")
        print(f"  HTML размер: {len(html)} символов")
        print(f"  Markdown размер: {len(markdown)} символов")
        
        # Сохраняем HTML
        if html:
            save_html(html, "history.html")
        
        # Сохраняем markdown для справки
        if markdown:
            with open("history.md", "w", encoding="utf-8") as f:
                f.write(markdown)
            print(f"💾 Markdown сохранен в history.md")
        
        # Сохраняем полный JSON ответ
        with open("history.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"💾 Полный ответ сохранен в history.json")
        
    else:
        print("❌ Не удалось получить данные")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
