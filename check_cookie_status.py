"""
Скрипт для проверки статуса Cookie Manager
"""
import requests
import json

def check_status():
    try:
        response = requests.get("http://localhost:8000/status")
        if response.status_code == 200:
            data = response.json()
            print("=" * 60)
            print("📊 СТАТУС COOKIE MANAGER")
            print("=" * 60)
            print(f"Всего кук: {data['cookie_count']}")
            print(f"\nКритичные куки:")
            for name, status in data.get('critical_cookies', {}).items():
                emoji = "✅" if status == "present" else "❌"
                print(f"  {emoji} {name}: {status}")
            print(f"\nПоследняя проверка валидна: {data['last_check_valid']}")
            print(f"Провалов подряд: {data['consecutive_failures']}")
            print(f"Recovery в процессе: {data['recovery_in_progress']}")
            print(f"Интервал проверки: {data['check_interval_seconds']}с")
            print("=" * 60)
            
            # Если куки невалидны
            if data.get('critical_cookies', {}).get('DMIR_AUTH') == 'EMPTY' or \
               data.get('critical_cookies', {}).get('remixsid') == 'EMPTY':
                print("\n⚠️  ВНИМАНИЕ: Критичные куки пусты!")
                print("Необходимо авторизоваться:")
                print("1. Откройте NoVNC: http://localhost:8080/vnc.html")
                print("2. Авторизуйтесь на Cian.ru")
                print("3. Нажмите зеленую кнопку ГОТОВО")
                print("\nИли запустите recovery вручную:")
                print("curl -X POST http://localhost:8000/refresh")
        else:
            print(f"❌ Ошибка: {response.status_code}")
    except Exception as e:
        print(f"❌ Не удалось подключиться к Cookie Manager: {e}")
        print("Убедитесь, что сервис запущен: docker-compose up -d cookie_manager")

if __name__ == "__main__":
    check_status()
