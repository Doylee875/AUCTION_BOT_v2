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
  --force     Принудительная синхронизация каталога, даже если уже была сегодня.
  --no-fetch  Пропустить синхронизацию каталога (шаги 2–5 выполняются как обычно).
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
    """Шаг 4: автоотбор предметов для мониторинга на основе аналитики.

    Критерии отбора (по гранулярности 'daily', агрегат по последним 14 дням):
      - AVG(liquidity)     >= autoselect_min_liquidity    (по умолчанию 0.5)
      - AVG(sales_per_day) >= autoselect_min_sales_per_day (по умолчанию 1.0)
      - Минимум 3 дня с данными (COUNT бакетов), чтобы не реагировать на выбросы
      - low_sample = 0 во всех вошедших бакетах
      - qlt = ptn = upgrade_level = -1 (агрегатная строка, не срез артефакта)

    Почему 'daily':
      Единственная гранулярность, где sales_per_day физически означает
      «продаж за сутки». В weekly/monthly это «продаж за неделю/месяц»,
      в window_weekday — «продаж за 4-часовое окно», что делало бы пороги
      несопоставимыми. Гранулярности '30d' в БД не существует.

    Предметы из минус-листа (ignored_items) пропускаются.
    """
    from watched_items import load_watched_item_ids, load_ignored_ids, save_watched_items

    log = get_logger(__name__ + ".autoselect")

    async with db_lock:
        min_liquidity: float = getattr(settings, "autoselect_min_liquidity", 0.5)
        min_sales_per_day: float = getattr(settings, "autoselect_min_sales_per_day", 1.0)
        min_days: int = getattr(settings, "autoselect_min_days", 3)

        rows = conn.execute(
            """
            SELECT item_id
            FROM analytics_summary
            WHERE granularity    = 'daily'
              AND qlt            = -1
              AND ptn            = -1
              AND upgrade_level  = -1
              AND low_sample     = 0
            GROUP BY item_id
            HAVING COUNT(bucket_key)    >= ?
               AND AVG(liquidity)       >= ?
               AND AVG(sales_per_day)   >= ?
            """,
            (min_days, min_liquidity, min_sales_per_day),
        ).fetchall()

        candidates: set[str] = {r[0] for r in rows}

        log.info(
            "Автоотбор: найдено %d кандидатов (daily, liquidity≥%.2f, sales/day≥%.2f, дней≥%d).",
            len(candidates), min_liquidity, min_sales_per_day, min_days,
        )

        if not candidates:
            return

        ignored: set[str] = load_ignored_ids(conn)
        already_watched: set[str] = load_watched_item_ids(conn)

        new_items: set[str] = candidates - ignored - already_watched

        if not new_items:
            log.info("Автоотбор: все %d кандидатов уже отслеживаются.", len(candidates))
            return

        merged = already_watched | new_items
        added, _ = save_watched_items(conn, merged)
        log.info(
            "Автоотбор: добавлено %d новых предметов (из %d кандидатов, %d уже были).",
            added, len(candidates), len(candidates) - len(new_items),
        )


async def _step_monitor_lots(conn, db_lock) -> None:
    """Шаг 5: мониторинг активных лотов по watched_items."""
    from api.fetcher_lots import run_lots_watcher

    await run_lots_watcher(conn=conn, db_lock=db_lock)


# ---------------------------------------------------------------------------
# Основной async-сценарий (шаги 2–5 в одном event-loop)
# ---------------------------------------------------------------------------

async def _run_main_pipeline(force_sync: bool) -> None:
    """Выполняет шаги 2–5 с общим соединением и db_lock.

    Все пишущие корутины сериализованы через asyncio.Lock, что устраняет
    гонки WAL-чекпоинта при одновременном conn.commit() из одного Connection.
    """
    from db.connection import open_connection

    db_lock = asyncio.Lock()
    conn = open_connection(settings.db_path)

    log = get_logger(__name__)

    try:
        # Шаги 2–3: продажи + аналитика
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

def main(force: bool = False, no_fetch: bool = False) -> None:
    """Запускает бота по основному сценарию.

    Args:
        force:    Принудительная синхронизация каталога.
        no_fetch: Пропустить синхронизацию каталога (шаг 1).
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
    asyncio.run(_run_main_pipeline(force_sync=force))


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

    args = parser.parse_known_args()[0]

    main(force=args.force, no_fetch=args.no_fetch)