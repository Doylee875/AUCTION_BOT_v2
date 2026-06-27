"""
fetcher_sales.py
================
Загрузка истории продаж из STALCRAFT API в таблицы sales / sale_attrs.

Алгоритм на каждый item_id:
  1. GET offset=0  → узнаём актуальный total.
  2. total == 0    → минус-лист, пропускаем.
  3. Режим B (обновление свежего, offset=0..N)  — пока sold_at > newest_fetched.
  4. Режим A (дополучение вглубь)               — resume_offset .. cutoff_ts.
  5. Состояние пагинации сохраняется в fetch_state после каждой страницы.

Параллелизм: asyncio.TaskGroup, по одному воркеру на item_id,
клиент берётся из пула по worker_id % len(pool).
Запись в SQLite сериализуется через asyncio.Lock (db_lock из main.py).
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

from api.SZ_client import StalcraftClient
from api.pool import StalcraftClientPool
from api.utils.logger import get_logger
from config import settings

log = get_logger(__name__)

_LIMIT = 200  # максимум, принимаемый API

_INTEGER_ATTR_NAMES = frozenset({"qlt", "ptn"})
_INTEGER_ATTR_SUFFIXES = ("_count", "_level")

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _iso_to_ts(iso: str) -> int:
    """ISO-8601 → unix timestamp (UTC). Stdlib, без dateutil."""
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return int(dt.timestamp()) if dt.tzinfo else int(dt.replace(tzinfo=timezone.utc).timestamp())


def _now_ts() -> int:
    return int(time.time())


def _attr_col_type(key: str, value: Any = None) -> str:
    """Определяет тип SQLite-колонки по имени и значению атрибута.

    list/dict → TEXT (JSON); иначе по имени: INTEGER или REAL.
    Тип фиксируется при первом ALTER TABLE — последующие значения
    приводятся через _coerce_attr_value без изменения схемы.
    """
    if isinstance(value, (list, dict)):
        return "TEXT"
    return "INTEGER" if (key in _INTEGER_ATTR_NAMES or key.endswith(_INTEGER_ATTR_SUFFIXES)) else "REAL"


def _coerce_attr_value(value: Any) -> Any:
    """Приводит значение атрибута к типу, поддерживаемому SQLite.

    list/dict → JSON-строка; скаляры — как есть.
    """
    return json.dumps(value, ensure_ascii=False) if isinstance(value, (list, dict)) else value


# ---------------------------------------------------------------------------
# Динамическая таблица sale_attrs
# ---------------------------------------------------------------------------

# Кэш известных колонок: инвалидируется при рестарте, обновляется через ALTER TABLE.
_known_attr_cols: set[str] = set()


def _init_attr_cols_cache(conn: sqlite3.Connection) -> None:
    """Инициализирует кэш известных колонок из реальной схемы БД.

    Вызывать один раз при старте, до запуска воркеров.
    Защищает от ситуации «БД уже содержит колонки, кэш пуст» при рестарте.
    """
    rows = conn.execute("PRAGMA table_info(sale_attrs)").fetchall()
    _known_attr_cols.update(r[1] for r in rows)


def _ensure_attr_cols(
    conn: sqlite3.Connection,
    key_values: dict[str, Any],
) -> None:
    """Добавляет в sale_attrs колонки для новых атрибутов (ALTER TABLE ADD COLUMN).

    Принимает словарь key→sample_value, чтобы определить тип колонки по значению
    (list/dict → TEXT, иначе — по имени ключа).
    DDL выполняется редко (только при новых ключах).
    """
    new_keys = key_values.keys() - _known_attr_cols
    if not new_keys:
        return

    cur = conn.cursor()
    for key in new_keys:
        col_type = _attr_col_type(key, key_values[key])
        try:
            cur.execute(f"ALTER TABLE sale_attrs ADD COLUMN {key} {col_type}")
            log.info("sale_attrs: добавлена колонка %s %s", key, col_type)
        except sqlite3.OperationalError as exc:
            # Колонка уже существует (кэш рассинхронизировался) — не падаем.
            if "duplicate column name" not in str(exc).lower():
                raise
            log.warning("sale_attrs: колонка %s уже существует (пропуск DDL)", key)
        _known_attr_cols.add(key)

    conn.commit()


# ---------------------------------------------------------------------------
# Запись в БД
# ---------------------------------------------------------------------------

def _upsert_sales(
    conn: sqlite3.Connection,
    item_id: str,
    prices: list[dict[str, Any]],
) -> list[tuple[int, dict[str, Any]]]:
    """INSERT OR IGNORE в sales, возвращает [(sale_id, additional), ...] для вставленных."""
    cur = conn.cursor()
    inserted: list[tuple[int, dict[str, Any]]] = []

    for p in prices:
        cur.execute(
            """
            INSERT INTO sales (item_id, price, amount, sold_at, raw_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (item_id, sold_at, price, amount) DO NOTHING
            """,
            (item_id, p["price"], p["amount"], _iso_to_ts(p["time"]),
             json.dumps(p, ensure_ascii=False)),
        )
        additional = p.get("additional") or {}
        if cur.rowcount and additional:
            inserted.append((cur.lastrowid, additional))  # type: ignore[arg-type]

    conn.commit()
    return inserted


def _upsert_attrs(conn: sqlite3.Connection, rows: list[tuple[int, dict[str, Any]]]) -> None:
    """Вставляет атрибуты в sale_attrs. DDL выполняется до INSERT.

    Нескалярные значения (list, dict) сериализуются в JSON-строку.
    """
    if not rows:
        return

    # Собираем первое встреченное значение каждого ключа для определения типа колонки
    sample: dict[str, Any] = {}
    for _, attrs in rows:
        for k, v in attrs.items():
            if k not in sample:
                sample[k] = v

    _ensure_attr_cols(conn, sample)

    cur = conn.cursor()
    for sale_id, attrs in rows:
        coerced = {k: _coerce_attr_value(v) for k, v in attrs.items()}
        cols = ", ".join(coerced)
        placeholders = ", ".join("?" * len(coerced))
        cur.execute(
            f"""
            INSERT INTO sale_attrs (sale_id, {cols})
            VALUES (?, {placeholders})
            ON CONFLICT (sale_id) DO UPDATE SET
            {", ".join(f"{k}=excluded.{k}" for k in coerced)}
            """,
            (sale_id, *coerced.values()),
        )
    conn.commit()


def _write_prices(
    conn: sqlite3.Connection,
    item_id: str,
    prices: list[dict[str, Any]],
) -> None:
    """Записывает продажи и их атрибуты за один вызов."""
    attr_rows = _upsert_sales(conn, item_id, prices)
    _upsert_attrs(conn, attr_rows)


# ---------------------------------------------------------------------------
# Состояние пагинации
# ---------------------------------------------------------------------------

_STATE_FIELDS = ("total_known", "fetched_offset", "oldest_fetched", "newest_fetched", "status")
_STATE_DEFAULTS: dict[str, Any] = {
    "total_known": None,
    "fetched_offset": 0,
    "oldest_fetched": None,
    "newest_fetched": None,
    "status": "pending",
}


def _load_state(conn: sqlite3.Connection, item_id: str) -> dict[str, Any]:
    row = conn.execute(
        f"SELECT {', '.join(_STATE_FIELDS)} FROM fetch_state WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    return dict(zip(_STATE_FIELDS, row)) if row else _STATE_DEFAULTS.copy()


def _save_state(conn: sqlite3.Connection, item_id: str, **kwargs: Any) -> None:
    fields_upsert = ", ".join(f"{k}=excluded.{k}" for k in kwargs)
    conn.execute(
        f"""
        INSERT INTO fetch_state (item_id, {", ".join(kwargs)})
        VALUES (?, {", ".join("?" * len(kwargs))})
        ON CONFLICT (item_id) DO UPDATE SET {fields_upsert}, last_run_at={_now_ts()}
        """,
        (item_id, *kwargs.values()),
    )
    conn.commit()


def _add_to_ignored(conn: sqlite3.Connection, item_id: str) -> None:
    conn.execute("INSERT OR IGNORE INTO ignored_items (item_id) VALUES (?)", (item_id,))
    conn.commit()
    log.info("Минус-лист: %s (total=0)", item_id)


# ---------------------------------------------------------------------------
# Пагинация одного item_id
# ---------------------------------------------------------------------------

async def _get_prices(client: StalcraftClient, path: str, offset: int) -> list[dict]:
    page = await client.get(path, params={"limit": _LIMIT, "offset": offset})
    return page.get("prices") or []


async def _fetch_item(
    item_id: str,
    client: StalcraftClient,
    conn: sqlite3.Connection,
    db_lock: asyncio.Lock,
    cutoff_ts: int,
) -> None:
    path = f"/{settings.region.value}/auction/{item_id}/history"

    # --- Страница 0: получаем total и первую порцию свежих данных ---
    first = await client.get(path, params={"limit": _LIMIT, "offset": 0})
    new_total: int = first["total"]
    first_prices: list[dict] = first.get("prices") or []

    if new_total == 0:
        async with db_lock:
            _add_to_ignored(conn, item_id)
        return

    async with db_lock:
        state = _load_state(conn, item_id)

    newest_fetched: int | None = state["newest_fetched"]
    oldest_fetched: int | None = state["oldest_fetched"]
    old_offset: int = state["fetched_offset"]
    old_total: int | None = state["total_known"]

    # --- Режим B: обновление свежего (offset 0 → пока sold_at > newest_fetched) ---
    b_offset = 0
    while True:
        page_prices = first_prices if b_offset == 0 else await _get_prices(client, path, b_offset)
        if not page_prices:
            break

        fresh = [p for p in page_prices
                 if newest_fetched is None or _iso_to_ts(p["time"]) > newest_fetched]

        if fresh:
            new_newest = max(_iso_to_ts(p["time"]) for p in fresh)
            async with db_lock:
                _write_prices(conn, item_id, fresh)
                newest_fetched = max(newest_fetched or 0, new_newest)
                _save_state(conn, item_id, newest_fetched=newest_fetched,
                            total_known=new_total, status="running")

        if len(fresh) < len(page_prices):  # дошли до уже известных — стоп
            break

        b_offset += _LIMIT
        if b_offset >= new_total:
            break

    # --- Режим A: дополучение вглубь ---
    if oldest_fetched is not None and oldest_fetched <= cutoff_ts:
        async with db_lock:
            _save_state(conn, item_id, total_known=new_total, status="done")
        return

    # Вычисляем resume_offset с учётом сдвига из-за новых записей
    if old_total is not None:
        resume_offset = old_offset + max(0, new_total - old_total)
    else:
        resume_offset = b_offset  # первый запуск: продолжаем сразу после режима B

    a_offset = resume_offset
    while a_offset < new_total:
        if a_offset == 0 and b_offset == 0:
            page_prices = first_prices   # страница 0 уже есть в памяти
        else:
            page_prices = await _get_prices(client, path, a_offset)

        if not page_prices:
            break

        chunk: list[dict] = []
        oldest_on_page: int | None = None
        stop = False
        for p in page_prices:
            ts = _iso_to_ts(p["time"])
            oldest_on_page = ts
            if ts < cutoff_ts:
                stop = True
                break
            chunk.append(p)

        if chunk:
            if oldest_on_page is not None:
                oldest_fetched = min(oldest_fetched, oldest_on_page) if oldest_fetched else oldest_on_page
            async with db_lock:
                _write_prices(conn, item_id, chunk)
                _save_state(
                    conn, item_id,
                    fetched_offset=a_offset + len(chunk),
                    oldest_fetched=oldest_fetched,
                    total_known=new_total,
                    status="running",
                )

        if stop:
            break

        a_offset += _LIMIT

    async with db_lock:
        _save_state(conn, item_id, total_known=new_total, status="done")

    log.debug("Готово: %s (total=%d)", item_id, new_total)


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

async def fetch_all_sales(
    conn: sqlite3.Connection,
    db_lock: asyncio.Lock,
    pool: StalcraftClientPool,
    *,
    period_days: int = 7,
) -> None:
    """Загружает историю продаж для всех item_id из таблицы items.

    Args:
        conn:        Соединение с SQLite (из main.py).
        db_lock:     asyncio.Lock для сериализации записи (из main.py).
        pool:        Пул клиентов (из main.py).
        period_days: Глубина загрузки в днях.
    """
    cutoff_ts = _now_ts() - period_days * 86_400
    log.info("Загрузка продаж: период %d дн., cutoff=%s", period_days,
             datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).isoformat())

    item_ids = [
        r[0] for r in conn.execute(
            "SELECT i.item_id FROM items i"
            " WHERE i.item_id NOT IN (SELECT item_id FROM ignored_items)"
            " ORDER BY i.item_id"
        )
    ]
    log.info("Предметов для обработки: %d", len(item_ids))

    # Инициализируем кэш колонок из реальной схемы БД до старта воркеров.
    # Без этого при рестарте кэш пуст, и _ensure_attr_cols пытается добавить
    # уже существующие колонки → OperationalError: duplicate column name.
    _init_attr_cols_cache(conn)

    t0 = time.monotonic()

    async def _worker(worker_id: int, item_id: str) -> None:
        client = pool.client_for_worker(worker_id)
        try:
            await _fetch_item(item_id, client, conn, db_lock, cutoff_ts)
        except Exception:
            log.exception("Ошибка при загрузке продаж: item_id=%s", item_id)

    async with asyncio.TaskGroup() as tg:
        for i, item_id in enumerate(item_ids):
            tg.create_task(_worker(i, item_id))

    log.info("История загружена: %d предметов за %.1fs", len(item_ids), time.monotonic() - t0)