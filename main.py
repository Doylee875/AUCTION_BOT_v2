"""
STALCRAFT Auction Bot — точка входа.

Сценарий запуска:
    1. Синхронизация БД (GitHub → БД)
    2. Получение истории продаж
    3. Пересчёт аналитики
    4. Автоотбор предметов для мониторинга
    5. Мониторинг предметов (вотчер активных лотов)

Аргументы:
    --force     Принудительная синхронизация БД, даже если уже выполнялась сегодня
    --no-fetch  Пропустить синхронизацию БД (шаги 1)
"""

import argparse
import asyncio
import logging
import sys

import api.utils.logger

log: logging.Logger


# ---------------------------------------------------------------------------
# Шаг 1 — Синхронизация БД
# ---------------------------------------------------------------------------

def sync_database(force: bool) -> None:
    """Синхронизирует каталог предметов из GitHub в локальную БД."""
    import api.fetcher

    log.info("[1/5] Синхронизация БД (force=%s)", force)
    api.fetcher.run(force=force)
    log.info("[1/5] Синхронизация завершена")


# ---------------------------------------------------------------------------
# Шаги 2–5 — Асинхронный пайплайн
# ---------------------------------------------------------------------------

async def run_pipeline() -> None:
    """Последовательно выполняет шаги 2–5 в рамках одного event-loop."""

    from db.connection import open_connection
    from config import settings

    conn = open_connection(settings.db_path)
    db_lock = asyncio.Lock()

    try:
        await fetch_sales(conn, db_lock)
        await recalculate_analytics(conn, db_lock)
        await autoselect_items(conn, db_lock)
        await monitor_items(conn, db_lock)
    finally:
        conn.close()


async def fetch_sales(conn, db_lock: asyncio.Lock) -> None:
    """Шаг 2 — Загрузка истории продаж с аукциона."""
    import api.fetcher_anal

    log.info("[2/5] Загрузка истории продаж")
    await api.fetcher_anal.fetch_sales(conn=conn, db_lock=db_lock)
    log.info("[2/5] История продаж загружена")


async def recalculate_analytics(conn, db_lock: asyncio.Lock) -> None:
    """Шаг 3 — Пересчёт аналитических показателей."""
    import api.fetcher_anal

    log.info("[3/5] Пересчёт аналитики")
    await api.fetcher_anal.recalculate(conn=conn, db_lock=db_lock)
    log.info("[3/5] Аналитика пересчитана")


async def autoselect_items(conn, db_lock: asyncio.Lock) -> None:
    """Шаг 4 — Автоотбор предметов для мониторинга."""
    from analytics.autoselect import run_autoselect

    log.info("[4/5] Автоотбор предметов для мониторинга")
    await run_autoselect(conn=conn, db_lock=db_lock)
    log.info("[4/5] Автоотбор завершён")


async def monitor_items(conn, db_lock: asyncio.Lock) -> None:
    """Шаг 5 — Мониторинг выбранных предметов (вотчер активных лотов)."""
    from api.fetcher_lots import run_lots_watcher

    log.info("[5/5] Запуск мониторинга предметов")
    await run_lots_watcher(conn=conn, db_lock=db_lock)
    log.info("[5/5] Мониторинг завершён")


# ---------------------------------------------------------------------------
# Вспомогательное
# ---------------------------------------------------------------------------

def _start_tracker_server() -> None:
    """Запускает веб-трекер в фоновом daemon-потоке."""
    import threading
    from tracker_server import run_server

    t = threading.Thread(target=run_server, daemon=True, name="tracker-server")
    t.start()
    log.info("Tracker-сервер запущен в фоне")


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="STALCRAFT Auction Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Примеры:\n"
            "  python main.py                # стандартный запуск\n"
            "  python main.py --force        # принудительная синхронизация БД\n"
            "  python main.py --no-fetch     # без синхронизации БД\n"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Принудительная синхронизация БД (даже если выполнялась сегодня)",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        help="Пропустить синхронизацию БД",
    )
    args = parser.parse_known_args()[0]

    # Настройка логирования
    api.utils.logger.setup_logging(
        level=api.utils.logger.settings.log_level,
        fmt=api.utils.logger.settings.log_format,
    )

    global log
    log = api.utils.logger.get_logger(__name__)

    log.info("=== STALCRAFT Auction Bot запущен ===")

    _start_tracker_server()

    # Шаг 1 — синхронизация БД (опционально)
    if args.no_fetch:
        log.info("[1/5] Синхронизация БД пропущена (--no-fetch)")
    else:
        sync_database(force=args.force)

    # Шаги 2–5 — асинхронный пайплайн
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        log.info("Остановлено пользователем (Ctrl+C)")
        sys.exit(0)

    log.info("=== Работа завершена ===")


if __name__ == "__main__":
    main()