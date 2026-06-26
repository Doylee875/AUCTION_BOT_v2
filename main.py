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


async def _step_fetch_sales_and_analytics(conn, db_lock) -> None:
    """Шаги 2–3: загрузка истории продаж + пересчёт аналитики."""
    import api.fetcher_anal

    await api.fetcher_anal.setup_analysis(conn=conn, db_lock=db_lock)


async def _step_autoselect_watched(conn, db_lock) -> None:
    """Шаг 4: автоотбор предметов для мониторинга на основе арбитражного скоринга.

    Делегирует всю логику отбора и скоринга в analytics.auto_select.
    Полностью перезаписывает watched_items по результатам каждого прогона.
    """
    from analytics.auto_select import find_candidates, sync_candidates_to_watched

    log = get_logger(__name__ + ".autoselect")

    async with db_lock:
        candidates = find_candidates(conn)
        log.info("Автоотбор: найдено %d кандидатов после скоринга.", len(candidates))

        if not candidates:
            log.warning("Автоотбор: нет кандидатов, watched_items не изменён.")
            return

        added, total = sync_candidates_to_watched(conn, candidates)
        log.info(
            "Автоотбор: watched_items перезаписан — %d предметов (%d новых).",
            total, added,
        )


async def _step_monitor_lots(conn, db_lock) -> None:
    """Шаг 5: мониторинг активных лотов по watched_items."""
    from api.fetcher_lots import run_lots_watcher

    await run_lots_watcher(conn=conn, db_lock=db_lock)


# ---------------------------------------------------------------------------
# Основной async-сценарий (шаги 2–5 в одном event-loop)
# ---------------------------------------------------------------------------

async def _run_main_pipeline(force_sync: bool, no_history: bool = False) -> None:
    """Выполняет шаги 2–5 с общим соединением и db_lock.

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
        # Шаги 2–3: продажи + аналитика
        if no_history:
            log.info("Шаги 2–3: пропущены (--no-history).")
        else:
            log.info("Шаги 2–3: загрузка истории продаж и пересчёт аналитики.")
            await _step_fetch_sales_and_analytics(conn, db_lock)

        # Шаг 4: автоотбор предметов
        log.info("Шаг 4: автоотбор предметов для мониторинга.")
        await _step_autoselect_watched(conn, db_lock)

        # Шаг 5: мониторинг лотов (работает в цикле)
        log.info("Шаг 5: запуск мониторинга активных лотов.")
        await _step_monitor_lots(conn, db_lock)

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def main(force: bool = False, no_fetch: bool = False, no_history: bool = False) -> None:
    """Запускает бота по основному сценарию.

    Args:
        force:      Принудительная синхронизация каталога.
        no_fetch:   Пропустить синхронизацию каталога (шаг 1).
        no_history: Пропустить загрузку истории продаж и пересчёт аналитики (шаги 2–3).
    """
    api.utils.logger.setup_logging(
        level=settings.log_level,
        fmt=settings.log_format,
    )
    log = get_logger(__name__)
    log.info("Запуск STALCRAFT Auction Bot.")

    _start_tracker_server()

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