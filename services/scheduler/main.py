"""
services.scheduler.main — Планировщик задач парсинга CIAN.

Запускает parser_cian (avans → offers) и category_counter по расписанию
через docker compose на том же хосте (типичный деплой: Linux VPS, socket
Docker смонтирован в контейнер). Отказоустойчивость: lock, таймауты,
retry с backoff, Telegram-алерты.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ---------------------------------------------------------------------------
# Конфигурация (из переменных окружения / .env)
# ---------------------------------------------------------------------------

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

COMPOSE_PROJECT_DIR = os.getenv("SCHEDULER_COMPOSE_DIR", "/app")

MAX_RETRIES = int(os.getenv("SCHEDULER_MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = int(os.getenv("SCHEDULER_RETRY_BACKOFF_BASE", "30"))

JOB_TIMEOUT_PARSER = int(os.getenv("SCHEDULER_JOB_TIMEOUT_PARSER", str(3 * 3600)))
JOB_TIMEOUT_COUNTER = int(os.getenv("SCHEDULER_JOB_TIMEOUT_COUNTER", str(30 * 60)))

LOCK_ACQUIRE_TIMEOUT = int(os.getenv("SCHEDULER_LOCK_TIMEOUT", str(6 * 3600)))

LOG_FILE = os.getenv("SCHEDULER_LOG_FILE", "data/logs/scheduler.log")
LOG_LEVEL = os.getenv("SCHEDULER_LOG_LEVEL", "INFO").upper()

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------


def _configure_logging() -> None:
    try:
        sys.stdout.reconfigure(errors="backslashreplace")
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(errors="backslashreplace")
    except Exception:
        pass

    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if LOG_FILE:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
            )
        )

    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)


_configure_logging()
logger = logging.getLogger("scheduler")

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


async def send_alert(text: str) -> None:
    """Отправляет алерт в Telegram (если настроен)."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, timeout=15.0)
            if resp.status_code != 200:
                logger.warning("Telegram API %s: %s", resp.status_code, resp.text)
    except Exception as exc:
        logger.error("Telegram send error: %s", exc)


# ---------------------------------------------------------------------------
# Docker Compose runner
# ---------------------------------------------------------------------------

_flipper_bind_env_done = False
_flipper_bind_env: dict[str, str] = {}


async def _flipper_bind_env_for_compose() -> dict[str, str]:
    """Пути к credentials/data на **хосте Docker** для volume в compose run.

    Клиент compose внутри контейнера шедулера резолвит ./credentials.json в
    пути вида /app/... — для демона это не тот файл. Нужны абсолютные пути
    на хосте: берём из bind-mount проекта (docker inspect) или из
    SCHEDULER_HOST_BIND_ROOT.
    """
    global _flipper_bind_env_done, _flipper_bind_env
    if _flipper_bind_env_done:
        return _flipper_bind_env
    _flipper_bind_env_done = True

    manual = os.getenv("SCHEDULER_HOST_BIND_ROOT", "").strip()
    if manual:
        root = manual.replace("\\", "/").rstrip("/")
        _flipper_bind_env = {
            "FLIPPER_CREDENTIALS_SOURCE": f"{root}/credentials.json",
            "FLIPPER_DATA_SOURCE": f"{root}/data",
            "FLIPPER_COOKIES_SOURCE": f"{root}/services/cookie_manager/cookies.json",
        }
        logger.info("Бинды compose: SCHEDULER_HOST_BIND_ROOT=%s", root)
        return _flipper_bind_env

    compose_dir = os.path.normpath(COMPOSE_PROJECT_DIR)
    for ref in (
        os.getenv("HOSTNAME", "").strip(),
        os.getenv("SCHEDULER_CONTAINER_NAME", "flipper_scheduler").strip(),
    ):
        if not ref:
            continue
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "inspect",
            "-f",
            "{{json .Mounts}}",
            ref,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0:
            continue
        try:
            mounts = json.loads(out.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            continue
        for m in mounts:
            dest = os.path.normpath(
                (m.get("Destination") or "").rstrip("/") or "/"
            )
            if dest != compose_dir:
                continue
            src = (m.get("Source") or "").strip()
            if not src:
                continue
            root = src.replace("\\", "/").rstrip("/")
            _flipper_bind_env = {
                "FLIPPER_CREDENTIALS_SOURCE": f"{root}/credentials.json",
                "FLIPPER_DATA_SOURCE": f"{root}/data",
                "FLIPPER_COOKIES_SOURCE": f"{root}/services/cookie_manager/cookies.json",
            }
            logger.info(
                "Бинды compose: корень репозитория на хосте Docker (inspect %s)=%s",
                ref,
                root,
            )
            return _flipper_bind_env

    logger.warning(
        "Не удалось определить корень репозитория на хосте для volume. "
        "Укажи SCHEDULER_HOST_BIND_ROOT (абсолютный путь на машине с Docker) "
        "или смонтируй проект в %s и пересоздай контейнер шедулера.",
        COMPOSE_PROJECT_DIR,
    )
    return _flipper_bind_env


async def run_docker_compose(
    service: str,
    args: list[str],
    timeout: int,
) -> int:
    """Запускает `docker compose --profile manual run --rm <service> <args>`.

    Без --no-deps: как у ручного `docker compose run`, поднимаются/проверяются
    depends_on (cookie_manager, postgres и т.д.) перед одноразовым контейнером.

    Возвращает exit code. При таймауте убивает процесс и возвращает -1.
    """
    project_name = os.getenv("COMPOSE_PROJECT_NAME", "flipper")
    cmd = [
        "docker", "compose",
        "--project-name", project_name,
        "--project-directory", COMPOSE_PROJECT_DIR,
        "--profile", "manual",
        "run", "--rm", service,
    ] + args

    logger.info("CMD: %s", " ".join(cmd))

    sub_env = os.environ.copy()
    sub_env.update(await _flipper_bind_env_for_compose())

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=sub_env,
    )

    try:
        stdout_data, _ = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.error("TIMEOUT (%ss) для %s %s — убиваю процесс", timeout, service, args)
        proc.kill()
        await proc.wait()
        return -1

    output = (stdout_data or b"").decode("utf-8", errors="replace")
    for line in output.rstrip().split("\n")[-30:]:
        logger.info("[%s] %s", service, line)

    code = proc.returncode or 0
    logger.info("%s %s завершился с кодом %s", service, args, code)
    return code


# ---------------------------------------------------------------------------
# Задачи
# ---------------------------------------------------------------------------

_parsing_lock = asyncio.Lock()
_counter_lock = asyncio.Lock()
_shutdown_event = asyncio.Event()


def _backoff(attempt: int) -> float:
    return min(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)), 600)


async def _run_with_retry(
    service: str,
    args: list[str],
    timeout: int,
    label: str,
) -> bool:
    """Запускает сервис с retry. Возвращает True при успехе."""
    for attempt in range(1, MAX_RETRIES + 1):
        if _shutdown_event.is_set():
            logger.info("Shutdown requested — прерываю %s", label)
            return False

        code = await run_docker_compose(service, args, timeout)
        if code == 0:
            return True

        if attempt < MAX_RETRIES:
            delay = _backoff(attempt)
            logger.warning(
                "%s: попытка %s/%s неудачна (code=%s). Retry через %.0fs",
                label, attempt, MAX_RETRIES, code, delay,
            )
            await asyncio.sleep(delay)
        else:
            logger.error(
                "%s: ВСЕ %s попыток провалились (последний code=%s)",
                label, MAX_RETRIES, code,
            )
    return False


async def job_parsing() -> None:
    """10:00 / 18:00 — avans, затем offers."""
    now = datetime.now().strftime("%H:%M")
    job_label = f"parsing-{now}"

    try:
        acquired = _parsing_lock.acquire()
        done = await asyncio.wait_for(acquired, timeout=LOCK_ACQUIRE_TIMEOUT)
    except asyncio.TimeoutError:
        msg = f"[{job_label}] Lock не освободился за {LOCK_ACQUIRE_TIMEOUT}s — пропускаю"
        logger.error(msg)
        await send_alert(f"<b>Scheduler</b>\n{msg}")
        return
    if not done:
        return

    try:
        logger.info("===== %s START =====", job_label)

        avans_ok = await _run_with_retry(
            "parser_cian",
            ["--mode", "avans"],
            JOB_TIMEOUT_PARSER,
            f"{job_label}/avans",
        )
        if not avans_ok:
            await send_alert(
                f"<b>Scheduler</b>\n"
                f"parser_cian --mode avans FAILED после {MAX_RETRIES} попыток ({job_label})"
            )

        offers_ok = await _run_with_retry(
            "parser_cian",
            ["--mode", "offers"],
            JOB_TIMEOUT_PARSER,
            f"{job_label}/offers",
        )
        if not offers_ok:
            await send_alert(
                f"<b>Scheduler</b>\n"
                f"parser_cian --mode offers FAILED после {MAX_RETRIES} попыток ({job_label})"
            )

        status = "OK" if (avans_ok and offers_ok) else "PARTIAL" if (avans_ok or offers_ok) else "FAILED"
        logger.info("===== %s END (%s) =====", job_label, status)

    except Exception as exc:
        logger.exception("Необработанная ошибка в %s: %s", job_label, exc)
        await send_alert(f"<b>Scheduler</b>\n{job_label} exception: {exc}")
    finally:
        _parsing_lock.release()


async def job_category_counter() -> None:
    """09:00 — category_counter."""
    job_label = "category_counter"

    try:
        acquired = _counter_lock.acquire()
        done = await asyncio.wait_for(acquired, timeout=60)
    except asyncio.TimeoutError:
        logger.warning("%s: lock занят, пропускаю", job_label)
        return
    if not done:
        return

    try:
        logger.info("===== %s START =====", job_label)

        ok = await _run_with_retry(
            "category_counter",
            ["python", "-m", "services.category_counter.main"],
            JOB_TIMEOUT_COUNTER,
            job_label,
        )
        if not ok:
            await send_alert(
                f"<b>Scheduler</b>\n"
                f"category_counter FAILED после {MAX_RETRIES} попыток"
            )

        logger.info("===== %s END (%s) =====", job_label, "OK" if ok else "FAILED")

    except Exception as exc:
        logger.exception("Необработанная ошибка в %s: %s", job_label, exc)
        await send_alert(f"<b>Scheduler</b>\n{job_label} exception: {exc}")
    finally:
        _counter_lock.release()


# ---------------------------------------------------------------------------
# Планировщик
# ---------------------------------------------------------------------------

MSK = "Europe/Moscow"


def build_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone=MSK)

    scheduler.add_job(
        job_category_counter,
        CronTrigger(hour=9, minute=00, timezone=MSK),
        id="category_counter_09",
        name="category_counter @ 12:10 MSK (TEST)",
        misfire_grace_time=3600,
        max_instances=1,
    )

    scheduler.add_job(
        job_parsing,
        CronTrigger(hour=10, minute=00, timezone=MSK),
        id="parsing_10",
        name="parsing (avans+offers) @ 12:10 MSK (TEST)",
        misfire_grace_time=3600,
        max_instances=1,
    )

    scheduler.add_job(
        job_parsing,
        CronTrigger(hour=18, minute=0, timezone=MSK),
        id="parsing_18",
        name="parsing (avans+offers) @ 18:00 MSK",
        misfire_grace_time=3600,
        max_instances=1,
    )

    return scheduler


async def healthcheck_loop() -> None:
    """Периодически проверяем, что docker daemon доступен."""
    while not _shutdown_event.is_set():
        try:
            proc = await asyncio.create_subprocess_exec(
                "docker", "info",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            code = await asyncio.wait_for(proc.wait(), timeout=15)
            if code != 0:
                logger.error("docker info вернул code=%s — Docker daemon недоступен?", code)
                await send_alert("<b>Scheduler</b>\nDocker daemon недоступен (docker info != 0)")
        except asyncio.TimeoutError:
            logger.error("docker info timeout — Docker daemon не отвечает")
            await send_alert("<b>Scheduler</b>\nDocker daemon не отвечает (timeout)")
        except FileNotFoundError:
            logger.error("docker CLI не найден в PATH")
            await send_alert("<b>Scheduler</b>\ndocker CLI не найден")
        except Exception as exc:
            logger.error("healthcheck error: %s", exc)

        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=300)
            break
        except asyncio.TimeoutError:
            pass


async def main() -> None:
    loop = asyncio.get_event_loop()

    def _signal_handler() -> None:
        logger.info("Получен сигнал завершения — начинаю graceful shutdown")
        _shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    scheduler = build_scheduler()
    scheduler.start()
    logger.info("Scheduler запущен. Задачи:")
    for job in scheduler.get_jobs():
        logger.info("  %s  [trigger: %s]", job.name, job.trigger)

    hc_task = asyncio.create_task(healthcheck_loop())

    await _shutdown_event.wait()

    logger.info("Shutdown: останавливаю планировщик")
    scheduler.shutdown(wait=True)
    hc_task.cancel()
    try:
        await hc_task
    except asyncio.CancelledError:
        pass
    logger.info("Scheduler остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
