import os
import json
import asyncio
import httpx
import logging
from playwright.async_api import async_playwright

logger = logging.getLogger(__name__)

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
# Это IP-адрес или домен вашего сервера, где запущен NoVNC. 
NOVNC_URL = os.getenv("NOVNC_URL", "http://127.0.0.1:8080/vnc.html")

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}


async def send_telegram_message(text: str, reply_markup: dict = None):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        logger.error("Telegram credentials not set!")
        return

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(url, json=payload)
            if resp.status_code != 200:
                logger.error(f"Telegram API Error: {resp.status_code} - {resp.text}")
        except Exception as e:
            logger.error(f"Failed to send TG message: {e}")


async def check_session_validity(cookie_str: str) -> bool:
    """
    Проверяет валидность сессии, делает 3 попытки, если сессия кажется невалидной.
    """
    headers = HEADERS.copy()
    headers["cookie"] = cookie_str

    attempts = 3
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(attempts):
            try:
                # Делаем запрос к профилю, как просил пользователь
                response = await client.get(
                    "https://my.cian.ru/profile", headers=headers
                )
                html_text = response.text

                # Ищем признак успешной авторизации.
                if '"isAuthenticated":true' in html_text:
                    return True  # Успешно!

                logger.warning(
                    f"Session check attempt {attempt + 1} failed: 'isAuthenticated':true not found."
                )
            except Exception as e:
                logger.warning(f"Session check attempt {attempt + 1} error: {e}")

            # Если это не последняя попытка, ждем перед повтором
            if attempt < attempts - 1:
                await asyncio.sleep(5)

    return False  # Все 3 попытки провалились


async def start_recovery_session(cookies_file_path: str):
    """
    Запускает видимый браузер (Xvfb) для ручной переавторизации.
    Инжектит кнопку ГОТОВО и ждет ее нажатия.
    """
    # Явно берём переменную DISPLAY из окружения (выставляемую в entrypoint.sh)
    display = os.environ.get("DISPLAY", ":99")
    logger.info(f"Starting recovery browser on DISPLAY={display}")

    async with async_playwright() as p:
        # headless=False ОБЯЗАТЕЛЬНО для NoVNC
        # env= передаём DISPLAY, иначе Playwright не найдёт X-сервер
        browser = await p.chromium.launch(
            headless=False,
            env={"DISPLAY": display},
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-software-rasterizer",
            ],
        )
        context = await browser.new_context(
            user_agent=HEADERS["user-agent"], viewport={"width": 1280, "height": 800}
        )
        page = await context.new_page()

        # Отправляем уведомление в Telegram.
        msg = (
            "⚠️ <b>Сессия Циан истекла (или куки пустые)!</b>\n\n"
            "Парсинг приостановлен.\n\n"
            f"1. Откройте NoVNC: {NOVNC_URL}\n"
            "2. Авторизуйтесь на Циан.\n"
            "3. Нажмите огромную зеленую кнопку <b>ГОТОВО</b> в углу экрана браузера VNC."
        )
        await send_telegram_message(msg)

        # Инжектим огромную кнопку «ГОТОВО» максимально надежно
        inject_btn_js = """
        (() => {
            const createButton = () => {
                if (document.getElementById('finish-auth-btn')) return;
                
                const btn = document.createElement('button');
                btn.id = 'finish-auth-btn';
                btn.innerText = '✅ ГОТОВО (Нажать после входа)';
                btn.style.cssText = `
                    position: fixed !important;
                    top: 20px !important;
                    left: 50% !important;
                    transform: translateX(-50%) !important;
                    z-index: 2147483647 !important;
                    padding: 15px 30px !important;
                    font-size: 20px !important;
                    font-weight: bold !important;
                    color: white !important;
                    background-color: #4CAF50 !important;
                    border: 3px solid white !important;
                    border-radius: 10px !important;
                    cursor: pointer !important;
                    box-shadow: 0 5px 15px rgba(0,0,0,0.5) !important;
                    display: block !important;
                    visibility: visible !important;
                    opacity: 1 !important;
                `;
                
                btn.onclick = (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    btn.innerText = 'СОХРАНЯЕМ...';
                    btn.style.backgroundColor = '#ff9800';
                    window.authFinished = true;
                };
                
                // Добавляем либо в body, либо прямо в html
                (document.body || document.documentElement).appendChild(btn);
            };

            // Запускаем проверку каждую секунду (на случай перерисовки страницы)
            setInterval(createButton, 1000);
            createButton();
        })()
        """
        await context.add_init_script(inject_btn_js)

        # Переходим на страницу профиля или логина
        logger.info("Navigating to profile page...")
        try:
            await page.goto(
                "https://my.cian.ru/profile",
                wait_until="domcontentloaded",
                timeout=60000,
            )
        except Exception as e:
            logger.warning(f"Initial goto failed (maybe redirect?): {e}")

        logger.info("Waiting for user to click the READY button in NoVNC...")

        # Ждем, пока пользователь не кликнет кнопку (флаг window.authFinished станет true)
        # Проверяем флаг в цикле, так как на разных страницах/фреймах он может сбрасываться
        logger.info("Entering polling loop for window.authFinished...")
        loop_count = 0
        while True:
            try:
                # Пробуем проверить флаг на основной странице
                is_finished = await page.evaluate("window.authFinished === true")
                if is_finished:
                    logger.info("Detected window.authFinished === true!")
                    break

                if loop_count % 30 == 0:
                    logger.info(f"Still waiting... Browser URL: {page.url}")
            except Exception as e:
                # Если страница в процессе навигации, просто ждем
                if loop_count % 30 == 0:
                    logger.warning(f"Poll attempt failed (likely navigation): {e}")
                pass

            loop_count += 1
            await asyncio.sleep(1)

        logger.info("User clicked READY! Extracting cookies...")

        # Забираем куки
        cookies = await context.cookies()

        # Сохраняем в файл
        with open(cookies_file_path, "w", encoding="utf-8") as f:
            json.dump(cookies, f)

        await browser.close()

        # Уведомляем об успехе
        await send_telegram_message(
            "✅ <b>Успешно!</b>\nСвежие куки получены и сохранены. Парсинг возобновлен."
        )
