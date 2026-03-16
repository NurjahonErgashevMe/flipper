import asyncio
import os
import sys
from dotenv import load_dotenv

# Добавляем папку сервиса в пути поиска Python, 
# чтобы внутренние импорты парсера (например, from models import ...) работали без ошибок
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, 'services', 'parser_cian'))

from services.parser_cian.parser import AdParser

# Загружаем переменные из .env
load_dotenv()

async def test_single_ad():
    if not os.getenv("FIRECRAWL_API_KEY"):
        print("[X] Ошибка: FIRECRAWL_API_KEY не установлен в .env")
        return

    # Подключаемся к менеджеру кук (он запущен в Docker и доступен на 8000 порту локалхоста)
    print("[*] Подключаемся к Cookie Manager...")
    parser = AdParser(cookie_manager_url="http://localhost:8000")
    
    url = "https://www.cian.ru/sale/flat/325133590/"
    print(f"[*] Тестируем парсинг через Cloud Firecrawl API: {url}")
    
    try:
        data = await parser.parse_async(url)
        print("\n[✓] Успешно спарсили данные!")
        print(f"Цена: {data.price} руб.")
        print(f"Площадь: {data.area} м²")
        print(f"Метро: {data.address.metro_station if data.address else 'Нет данных'}")
        print(f"ID на Циан: {data.cian_id}")
    except Exception as e:
        print(f"\n[X] Провал парсинга: {e}")

if __name__ == "__main__":
    asyncio.run(test_single_ad())