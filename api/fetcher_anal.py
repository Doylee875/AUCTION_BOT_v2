"""
api/fetcher_anal.py
===================
Загрузка и обновление истории продаж с аукциона.

Использует items.fetch_* для хранения состояния (нет отдельной item_fetch_state).
После загрузки вызывает recalculate_for_any() по attr_type предмета.
"""

import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import api.utils.logger
from api.client import StalcraftClient
from api.pool import StalcraftClientPool, build_pool
from config import settings
from db.connection import get_connection
from db.sales import (
    dispatch_sale,
    dispatch_sales_batch,
    update_fetch_state,
    update_fetch_state_offset,
)
from analytics.metrics import (
    apply_relative_volume,
    recalculate_baselines,
    recalculate_for_any,
)
from schema import init_db
from watched_items import add_ignored

log = api.utils.logger.get_logger(__name__)

PRICE_HISTORY_ENDPOINT = "/{region}/auction/{item_id}/history"
FETCH_LIMIT   = 200
INVALID_TOTAL = -9223372036854776000   # Long.MIN_VALUE из API
LINE_OF_STOP  = timedelta(days=30)


def _history_path(item_id: str) -> str:
    return PRICE_HISTORY_ENDPOINT.format(
        region=settings.region.value,
        item_id=item_id,
    )


# ---------------------------------------------------------------------------
# Запрос к API
# ---------------------------------------------------------------------------

async def _fetch_page(
    scc: StalcraftClient,
    item_id: str,
    offset: int = 0,
    limit: int = FETCH_LIMIT,
) -> tuple[int, list[dict[str, Any]]]:
    response = await scc.get(
        _history_path(item_id),
        params={"limit": limit, "offset": offset},
    )
    return response.get("total", 0), response.get("prices") or []


# ---------------------------------------------------------------------------
# Полная загрузка (первый запуск или продолжение прерванной)
# ---------------------------------------------------------------------------

async def _fetch_full(
    scc: StalcraftClient,
    item_id: str,
    conn: sqlite3.Connection,
    db_lock: asyncio.Lock,
    fetch_time: datetime,
    start_offset: int = 0,
) -> tuple[int, int]:
    """
    Постраничная загрузка истории, начиная с start_offset.
    Сохраняет промежуточный offset после каждой страницы.
    Останавливается на данных старше LINE_OF_STOP.

    Returns:
        (total, total_saved)
    """
    offset      = start_offset
    total       = 0
    total_saved = 0
    cutoff      = fetch_time - LINE_OF_STOP
    request_delay = settings.request_delay

    while True:
        total, prices = await _fetch_page(scc, item_id, offset=offset)
        if not prices:
            break

        fresh = [
            p for p in prices
            if datetime.fromisoformat(p["time"].replace("Z", "+00:00")) >= cutoff
        ]

        if fresh:
            async with db_lock:
                cursor = conn.cursor()
                for sale in fresh:
                    dispatch_sale(cursor, item_id, sale)
                update_fetch_state_offset(conn, item_id, total, offset + len(prices), fetch_time)
                conn.commit()
            total_saved += len(fresh)
            log.debug(
                "%s: offset=%d свежих=%d/%d сохранено=%d/%d",
                item_id, offset, len(fresh), len(prices), total_saved, total,
            )

        if len(fresh) < len(prices):
            log.debug("%s: граница %dd на offset=%d (total=%d)", item_id, LINE_OF_STOP.days, offset, total)
            break

        if len(prices) < FETCH_LIMIT or offset + FETCH_LIMIT >= total:
            break

        offset += FETCH_LIMIT
        await asyncio.sleep(request_delay)

    return total, total_saved


# ---------------------------------------------------------------------------
# Инкрементальное обновление
# ---------------------------------------------------------------------------

def _should_skip(item_id: str, new_total: int, prev_total: int) -> bool:
    if new_total == INVALID_TOTAL or new_total <= 0:
        log.warning("%s: невалидный total=%d, пропускаю.", item_id, new_total)
        return True
    if new_total <= prev_total:
        log.debug("%s: новых нет (total %d <= %d).", item_id, new_total, prev_total)
        return True
    return False


def _filter_new(
    prices: list[dict],
    last_fetch_time: int | None,
    delta: int,
) -> list[dict]:
    """
    Отбирает новые записи по delta-смещению и/или по времени.
    Оба критерия объединяются (union) для надёжности.
    """
    if not prices:
        return []

    by_delta: set[int] = set(range(min(delta, len(prices))))
    by_time:  set[int] = set()

    if last_fetch_time:
        last_dt = datetime.fromtimestamp(last_fetch_time, tz=timezone.utc)
        by_time = {
            i for i, sale in enumerate(prices)
            if datetime.fromisoformat(sale["time"].replace("Z", "+00:00")) > last_dt
        }

    new_idx = by_delta | by_time
    return [prices[i] for i in sorted(new_idx)] if new_idx else []


# ---------------------------------------------------------------------------
# Обработка одного предмета
# ---------------------------------------------------------------------------

async def _process_item(
    scc: StalcraftClient,
    conn: sqlite3.Connection,
    db_lock: asyncio.Lock,
    row: tuple[Any, ...],
) -> None:
    item_id, prev_total, last_fetch_time, last_offset, attr_type = row
    fetch_time = datetime.now(timezone.utc)
    request_delay = settings.request_delay

    if prev_total == 0 or last_offset > 0:
        if last_offset > 0:
            log.info("  %s: продолжаю с offset=%d.", item_id, last_offset)
        new_total, saved = await _fetch_full(
            scc, item_id, conn, db_lock, fetch_time, start_offset=last_offset,
        )
        if saved:
            log.debug("  %s: сохранено %d записей.", item_id, saved)
        async with db_lock:
            update_fetch_state(conn, item_id, new_total, fetch_time)
            if new_total == 0:
                if add_ignored(conn, item_id):
                    log.info("  %s: total=0 от API, добавлен в минус-лист.", item_id)
            conn.commit()
        recalculate_for_any(conn, item_id, attr_type)
    else:
        new_total, prices = await _fetch_page(scc, item_id)

        if _should_skip(item_id, new_total, prev_total):
            await asyncio.sleep(request_delay)
            return

        delta      = new_total - prev_total
        new_prices = _filter_new(prices, last_fetch_time, delta)
        async with db_lock:
            if new_prices:
                dispatch_sales_batch(
                    conn, item_id, {"total": new_total, "prices": new_prices}, fetch_time,
                )
                log.info("  %s: сохранено %d новых записей.", item_id, len(new_prices))
            else:
                log.debug("  %s: фильтры не дали новых записей.", item_id)
            update_fetch_state(conn, item_id, new_total, fetch_time)
            conn.commit()
        recalculate_for_any(conn, item_id, attr_type)

    await asyncio.sleep(request_delay)


async def _worker(
    worker_id: int,
    client: StalcraftClient,
    queue: asyncio.Queue,
    conn: sqlite3.Connection,
    db_lock: asyncio.Lock,
) -> None:
    while True:
        row = await queue.get()
        try:
            if row is None:
                return
            idx, total, item_row = row
            item_id = item_row[0]
            log.info("worker=%d client=%s %d/%d %s …", worker_id, client.name, idx, total, item_id)
            await _process_item(client, conn, db_lock, item_row)
        except Exception as exc:
            item_for_log = item_row[0] if row else "?"
            log.error("worker=%d item=%s: %s", worker_id, item_for_log, exc)
        finally:
            queue.task_done()


# ---------------------------------------------------------------------------
# Главный обход
# ---------------------------------------------------------------------------

async def fetch_all_price_histories(
    pool: StalcraftClientPool,
    conn: sqlite3.Connection,
    db_lock: asyncio.Lock | None = None,
) -> None:
    """
    Обходит все предметы из items и загружает / обновляет историю продаж.

    Состояние загрузки читается из items.fetch_*.
    recalculate_for_any() вызывается вне db_lock — тяжёлая операция
    не блокирует остальных воркеров.

    Args:
        pool:    Пул API-клиентов.
        conn:    Соединение с БД — должно быть единственным пишущим соединением
                 в event loop. При совместном запуске с run_lots_watcher передавать
                 то же соединение и тот же db_lock (см. main._run_analysis_and_lots).
        db_lock: Общий asyncio.Lock для сериализации записи в conn.
                 Если None — создаётся локальный лок (режим одиночного запуска).
    """
    rows = conn.execute(
        "SELECT item_id, fetch_total, fetch_time, fetch_offset, attr_type FROM items"
        " WHERE item_id NOT IN (SELECT item_id FROM ignored_items)"
    ).fetchall()

    ignored_count = conn.execute("SELECT COUNT(*) FROM ignored_items").fetchone()[0]
    total_items   = len(rows)
    log.info(
        "Найдено %d предметов (пропущено %d из минус-листа), workers=%d.",
        total_items, ignored_count, len(pool.clients),
    )

    queue: asyncio.Queue = asyncio.Queue()
    for idx, row in enumerate(rows, start=1):
        queue.put_nowait((idx, total_items, row))
    for _ in pool.clients:
        queue.put_nowait(None)

    if db_lock is None:
        db_lock = asyncio.Lock()

    workers = [
        asyncio.create_task(
            _worker(i, pool.client_for_worker(i), queue, conn, db_lock),
        )
        for i in range(len(pool.clients))
    ]
    await queue.join()
    await asyncio.gather(*workers, return_exceptions=True)

    log.info("Пересчёт базовых линий…")
    recalculate_baselines(conn)
    apply_relative_volume(conn)
    log.info("relative_volume обновлён.")

    row = conn.execute(
        "SELECT COUNT(*), MIN(fetch_time), MAX(fetch_time) FROM items WHERE fetch_time IS NOT NULL"
    ).fetchone()
    if row and row[1] and row[2]:
        elapsed = row[2] - row[1]
        log.info("Итог: обновлено %d позиций, заняло %d с (%dm %02ds).",
                 row[0], elapsed, elapsed // 60, elapsed % 60)


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

async def setup_analysis(
    conn: sqlite3.Connection | None = None,
    db_lock: asyncio.Lock | None = None,
) -> None:
    """Инициализирует БД и загружает историю продаж.

    Args:
        conn:    Уже открытое соединение с БД. При совместном запуске с
                 run_lots_watcher следует передать общее соединение, чтобы
                 оба корутина пользовались одним и тем же объектом
                 sqlite3.Connection и не конкурировали на WAL-чекпоинте.
                 Если None — создаётся и закрывается внутри.
        db_lock: Общий asyncio.Lock для сериализации записей. При None
                 создаётся локальный (режим одиночного запуска).
    """
    log.info("Инициализация анализа…")
    pool = build_pool(settings)
    await pool.open()
    try:
        if conn is not None:
            # Соединение передано снаружи — не закрываем его здесь
            init_db(conn)
            await fetch_all_price_histories(pool, conn, db_lock=db_lock)
        else:
            with get_connection() as _conn:
                init_db(_conn)
                await fetch_all_price_histories(pool, _conn, db_lock=db_lock)
    finally:
        await pool.close()
    log.info("Загрузка истории завершена.")
