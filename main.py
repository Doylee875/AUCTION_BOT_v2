"""
main.py
=======
Точка входа STALCRAFT Auction Bot.

Основной сценарий (без флагов):
  1. Синхронизация каталога предметов (GitHub → БД)
  2. Загрузка истории продаж
  3. Пересчёт аналитики
  4. Автоотбор предметов для мониторинга
  5. Мониторинг активных лотов

Аргументы:
  --force       Принудительная синхронизация каталога, даже если уже была сегодня.
  --no-fetch    Пропустить синхронизацию каталога (шаги 2–5 выполняются как обычно).
  --no-history  Пропустить загрузку истории продаж и пересчёт аналитики (шаги 2–3),
                сразу перейти к автоотбору (шаг 4) и мониторингу (шаг 5).
"""

from __future__ import annotations

import argparse
import asyncio
import threading

import api.utils.logger
from api.utils.logger import get_logger
from config import settings
from analytics import calcul_anal

# ---------------------------------------------------------------------------
# Фоновые сервисы
# ---------------------------------------------------------------------------

def _start_tracker_server() -> None:
    """Запускает tracker_server в фоновом daemon-потоке."""
    from tracker_server import run_server

    t = threading.Thread(target=run_server, daemon=True, name="tracker-server")
    t.start()


# ---------------------------------------------------------------------------
# Шаги основного сценария
# ---------------------------------------------------------------------------

def _step_sync_db(force: bool) -> None:
    """Шаг 1: синхронизация каталога предметов GitHub → БД."""
    import api.fetcher

    api.fetcher.run(force=force)


async def _step_fetch_sales(conn, db_lock) -> None:
    """Шаг 2: загрузка истории продаж."""
    pass
    #TODO


async def _step_anal_calc(conn, db_lock,force_sync) -> None:
    """Шаг 3: пересчёт аналитики."""
    await calcul_anal.calc_anal()
    #TODO

async def _step_autoselect_watched(conn, db_lock) -> None:
    """Шаг 4: автоотбор предметов для мониторинга на основе арбитражного скоринга."""
    pass
    #TODO


async def _step_monitor_lots(conn, db_lock) -> None:
    """Шаг 5: мониторинг активных лотов по watched_items."""
    pass
    #TODO
    # from api.fetcher_lots import run_lots_watcher

    # await run_lots_watcher(conn=conn, db_lock=db_lock)


# ---------------------------------------------------------------------------
# Основной async-сценарий (шаги 2–4 в одном event-loop)
# ---------------------------------------------------------------------------

async def _run_main_pipeline(force_sync: bool, no_history: bool = False) -> None:
    """Выполняет шаги 2–4 с общим соединением и db_lock.

    Все пишущие корутины сериализованы через asyncio.Lock, что устраняет
    гонки WAL-чекпоинта при одновременном conn.commit() из одного Connection.

    Args:
        force_sync:  Признак принудительной синхронизации (передаётся для контекста).
        no_history:  Пропустить шаги 2–3 (загрузка истории + аналитика),
                     сразу перейти к автоотбору (шаг 4).
    """
    from db.connection import open_connection

    db_lock = asyncio.Lock()
    conn = open_connection(settings.db_path)

    log = get_logger(__name__)

    try:
        # Шаг 2: продажи
        if no_history:
            log.info("Шаг 2: пропущен (--no-history).")
        else:
            log.info("Шаг 2: загрузка истории продаж")
            await _step_fetch_sales(conn, db_lock)

        # Шаг 3: анализ продаж
        log.info("Шаг 3: анализ продаж")
        await _step_anal_calc(conn, db_lock,force_sync)    
            
        # Шаг 4: автоотбор предметов
        log.info("Шаг 4: автоотбор предметов для мониторинга.")
        await _step_autoselect_watched(conn, db_lock)

        # # Шаг 5: мониторинг лотов (работает в цикле)
        log.info("Шаг 5: запуск мониторинга активных лотов.")
        await _step_monitor_lots(conn, db_lock)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main(force: bool = False, no_fetch: bool = False, no_history: bool = False) -> None:
    """Запускает основной сценарий бота.

    Args:
        force:      Принудительная синхронизация каталога.
        no_fetch:   Пропустить синхронизацию каталога (шаг 1).
        no_history: Пропустить загрузку истории продаж и пересчёт аналитики (шаги 2–3).
    """
    api.utils.logger.setup_logging(
        level=settings.log_level,
        fmt=settings.log_format,
        date_fmt=settings.log_date_format
    )
    log = get_logger(__name__)
    log.info("Запуск STALCRAFT Auction Bot.")


    # Шаг 1: синхронизация каталога
    if no_fetch:
        log.info("Шаг 1: синхронизация пропущена (--no-fetch).")
    else:
        mode = "принудительная" if force else "стандартная"
        log.info("Шаг 1: синхронизация каталога (%s).", mode)
        _step_sync_db(force=force)

    # Шаги 2–5
    asyncio.run(_run_main_pipeline(force_sync=force, no_history=no_history))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="STALCRAFT Auction Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Основной сценарий:
  1. Синхронизация каталога предметов (GitHub → БД)
  2. Загрузка истории продаж
  3. Пересчёт аналитики
  4. Автоотбор предметов для мониторинга
  5. Мониторинг активных лотов

Флаги:
  --force       Принудительная синхронизация каталога.
  --no-fetch    Пропустить шаг 1.
  --no-history  Пропустить шаги 2–3, сразу перейти к шагу 4.
        """,
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Принудительная синхронизация каталога, даже если уже была сегодня.",
    )
    parser.add_argument(
        "--no-fetch",
        action="store_true",
        dest="no_fetch",
        help="Пропустить синхронизацию каталога (шаги 2–5 выполняются как обычно).",
    )
    parser.add_argument(
        "--no-history",
        action="store_true",
        dest="no_history",
        help="Пропустить загрузку истории продаж и пересчёт аналитики (шаги 2–3), "
             "сразу перейти к автоотбору (шаг 4) и мониторингу (шаг 5).",
    )

    args = parser.parse_known_args()[0]

    main(force=args.force, no_fetch=args.no_fetch, no_history=args.no_history)