"""
db/sales.py
===========
Сбор и чтение истории продаж.

Схема (определена в schema.py):
  sales       — история продаж всех предметов
  sale_attrs  — атрибуты продаж (qlt/ptn для артефактов, upgrade_level для улучшаемых)
  items       — содержит fetch_total / fetch_time / fetch_offset (состояние загрузки)

Публичный интерфейс:
  dispatch_sale()                — вставить одну продажу (+ атрибуты)
  dispatch_sales_batch()         — вставить батч продаж из ответа API
  get_sales()                    — получить продажи для аналитики (с опциональным фильтром атрибутов)
  get_distinct_attr_slices()     — уникальные срезы атрибутов предмета
  get_fetch_state() / update_fetch_state*() — состояние загрузки
  get_all_items_for_fetch()      — список предметов для обхода fetcher'ом
"""

import sqlite3
from datetime import datetime, timedelta, timezone

from schema import ATTR_TYPE_ARTIFACT, ATTR_TYPE_QLT_ONLY, ATTR_TYPE_UPGRADE
from analytics.bucket import (
    get_supported_granularities,
)
import api.utils.logger

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Временные константы (МСК = UTC+3)
# ---------------------------------------------------------------------------

_MSK_OFFSET_SEC:  int = 3 * 3600
_WINDOW_SHIFT_SEC: int = 2 * 3600   # окна начинаются с 02:00 МСК
_WINDOW_SIZE_SEC:  int = 4 * 3600   # 6 окон по 4 часа

_SQL_WINDOW_EXPR = "(((sold_at + {o} - {s}) % 86400) / {w})".format(
    o=_MSK_OFFSET_SEC, s=_WINDOW_SHIFT_SEC, w=_WINDOW_SIZE_SEC,
)
_SQL_DAY_TYPE_EXPR = (
    "CASE WHEN strftime('%w', sold_at + {o}, 'unixepoch') IN ('0', '6') "
    "THEN 'weekend' ELSE 'weekday' END"
).format(o=_MSK_OFFSET_SEC)


# ---------------------------------------------------------------------------
# Вставка продаж
# ---------------------------------------------------------------------------

def _parse_sold_at(iso_time: str) -> int:
    return int(datetime.fromisoformat(iso_time.replace("Z", "+00:00")).timestamp())


def _insert_sale(cursor: sqlite3.Cursor, item_id: str, sale: dict) -> int | None:
    """
    Вставляет строку в sales. Возвращает id новой строки или None если дубль.
    """
    cursor.execute(
        """
        INSERT OR IGNORE INTO sales (item_id, price, amount, sold_at)
        VALUES (:item_id, :price, :amount, :sold_at)
        """,
        {
            "item_id": item_id,
            "price":   sale["price"],
            "amount":  sale["amount"],
            "sold_at": _parse_sold_at(sale["time"]),
        },
    )
    return cursor.lastrowid if cursor.rowcount else None


def _insert_sale_attrs(cursor: sqlite3.Cursor, sale_id: int, additional: dict) -> None:
    """
    Вставляет атрибуты в sale_attrs. Единая функция для всех типов:
      - артефакт:  additional = {"qlt": 3, "ptn": 13}
      - qlt_only:  additional = {"qlt": 2}
      - upgrade:   additional = {"upgrade_level": 7}
    Если additional пустой — запись не создаётся (обычный предмет).
    """
    if not additional:
        # log.info("_insert_sale_attrs не дали additional")
        return
    cursor.execute(
        """
        INSERT OR IGNORE INTO sale_attrs (sale_id, qlt, ptn, upgrade_level)
        VALUES (:sale_id, :qlt, :ptn, :upgrade_level)
        """,
        {
            "sale_id":       sale_id,
            "qlt":           additional.get("qlt"),
            "ptn":           additional.get("ptn"),
            "upgrade_level": additional.get("upgrade_level"),
        },
    )


def dispatch_sale(cursor: sqlite3.Cursor, item_id: str, sale: dict) -> None:
    """
    Вставляет одну продажу + атрибуты (если есть).

    Публичная функция уровня каталога продаж — используется как из
    dispatch_sales_batch(), так и напрямую из fetcher_anal.py при
    постраничной загрузке (где нужен контроль над промежуточными commit'ами).
    """
    # log.info(sale)
    sale_id = _insert_sale(cursor, item_id, sale)
    if sale_id is None:
        return
    additional = sale.get("additional") or {}
    _insert_sale_attrs(cursor, sale_id, additional)


def dispatch_sales_batch(
    conn: sqlite3.Connection,
    item_id: str,
    api_response: dict,
    fetch_time: datetime | None = None,
) -> None:
    """
    Вставляет весь ответ API одним батчем.
    Не обновляет fetch_state — это делает fetcher_anal.py явно,
    чтобы иметь контроль над промежуточными коммитами.
    """
    if fetch_time is None:
        fetch_time = datetime.now(timezone.utc)

    # log.info(api_response)
    prices: list[dict] = api_response.get("prices", [])
    try:
        cursor = conn.cursor()
        for sale in prices:
            dispatch_sale(cursor, item_id, sale)
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Ошибка вставки батча для item_id=%s", item_id)
        raise


# ---------------------------------------------------------------------------
# Состояние загрузки (хранится в items.fetch_*)
# ---------------------------------------------------------------------------

def get_all_items_for_fetch(conn: sqlite3.Connection) -> list[tuple[str, int, int | None, int]]:
    """
    Возвращает (item_id, fetch_total, fetch_time, fetch_offset) для всех предметов.
    fetch_time — unix timestamp или None если ещё не грузили.
    """
    return conn.execute(
        "SELECT item_id, fetch_total, fetch_time, fetch_offset FROM items"
    ).fetchall()


def get_fetch_state(conn: sqlite3.Connection, item_id: str) -> dict | None:
    row = conn.execute(
        "SELECT fetch_total, fetch_time, fetch_offset FROM items WHERE item_id = ?",
        (item_id,),
    ).fetchone()
    if row is None:
        return None
    return {"fetch_total": row[0], "fetch_time": row[1], "fetch_offset": row[2]}


def is_fetch_stale(conn: sqlite3.Connection, item_id: str, max_age: timedelta) -> bool:
    state = get_fetch_state(conn, item_id)
    if state is None or state["fetch_time"] is None:
        return True
    last = datetime.fromtimestamp(state["fetch_time"], tz=timezone.utc)
    return (datetime.now(timezone.utc) - last) > max_age


def update_fetch_state_offset(
    conn: sqlite3.Connection,
    item_id: str,
    total: int,
    offset: int,
    fetch_time: datetime,
) -> None:
    """Промежуточное обновление — сохраняет текущий offset при прерванной загрузке."""
    conn.execute(
        """
        UPDATE items SET fetch_total = ?, fetch_time = ?, fetch_offset = ?
        WHERE item_id = ?
        """,
        (total, int(fetch_time.timestamp()), offset, item_id),
    )


def update_fetch_state(
    conn: sqlite3.Connection,
    item_id: str,
    total: int,
    fetch_time: datetime | None = None,
) -> None:
    """Финальное обновление — сбрасывает offset в 0 (загрузка завершена).
    Заодно обновляет last_sale_at из максимального sold_at в таблице sales.
    """
    if fetch_time is None:
        fetch_time = datetime.now(timezone.utc)
    row = conn.execute(
        "SELECT MAX(sold_at) FROM sales WHERE item_id = ?", (item_id,)
    ).fetchone()
    last_sale_at = row[0] if row else None
    conn.execute(
        """
        UPDATE items SET fetch_total = ?, fetch_time = ?, fetch_offset = 0,
                         last_sale_at = ?
        WHERE item_id = ?
        """,
        (total, int(fetch_time.timestamp()), last_sale_at, item_id),
    )


# ---------------------------------------------------------------------------
# Чтение продаж для аналитики
# ---------------------------------------------------------------------------

def _build_time_filter(
    granularity: str,
    bucket_key: str,
) -> tuple[str, dict]:
    """
    Строит SQL-условие по времени для заданной гранулярности и bucket_key.
    Возвращает (where_fragment, params_dict).
    Не включает фильтр по item_id — его добавляет вызывающий код.
    """
    from datetime import date as date_cls, time as time_cls, datetime as dt

    params: dict = {}

    if granularity == "window_weekday":
        parts     = bucket_key.split("_")
        window_id = int(parts[0])
        day_type  = parts[1]
        params["window_id"] = window_id
        params["day_type"]  = day_type
        return (
            f"{_SQL_WINDOW_EXPR} = :window_id AND {_SQL_DAY_TYPE_EXPR} = :day_type",
            params,
        )

    if granularity == "daily":
        target = date_cls.fromisoformat(bucket_key)
        start  = int(dt.combine(target, dt.min.time()).replace(tzinfo=timezone.utc).timestamp()) - _MSK_OFFSET_SEC
        params["ts_start"] = start
        params["ts_end"]   = start + 86400
        return "sold_at >= :ts_start AND sold_at < :ts_end", params

    if granularity == "weekly":
        iso_year, iso_week = int(bucket_key[:4]), int(bucket_key[6:])
        start_date = date_cls.fromisocalendar(iso_year, iso_week, 1)
        start = int(dt.combine(start_date, dt.min.time()).replace(tzinfo=timezone.utc).timestamp()) - _MSK_OFFSET_SEC
        params["ts_start"] = start
        params["ts_end"]   = start + 7 * 86400
        return "sold_at >= :ts_start AND sold_at < :ts_end", params

    if granularity == "monthly":
        year, month = int(bucket_key[:4]), int(bucket_key[5:7])
        start_date  = date_cls(year, month, 1)
        start = int(dt.combine(start_date, dt.min.time()).replace(tzinfo=timezone.utc).timestamp()) - _MSK_OFFSET_SEC
        next_m = date_cls(year + 1, 1, 1) if month == 12 else date_cls(year, month + 1, 1)
        end   = int(dt.combine(next_m, dt.min.time()).replace(tzinfo=timezone.utc).timestamp()) - _MSK_OFFSET_SEC
        params["ts_start"] = start
        params["ts_end"]   = end
        return "sold_at >= :ts_start AND sold_at < :ts_end", params

    if granularity == "hourly":
        date_part, hour_part = bucket_key.split("T")
        target_date = date_cls.fromisoformat(date_part)
        hour        = int(hour_part)
        start = int(dt.combine(target_date, time_cls(hour, 0)).replace(tzinfo=timezone.utc).timestamp()) - _MSK_OFFSET_SEC
        params["ts_start"] = start
        params["ts_end"]   = start + 3600
        return "sold_at >= :ts_start AND sold_at < :ts_end", params

    raise ValueError(f"Неподдерживаемая гранулярность: {granularity!r}")


def get_sales(
    conn: sqlite3.Connection,
    item_id: str,
    granularity: str,
    bucket_key: str,
    *,
    qlt: int | None = None,
    ptn: int | None = None,
    upgrade_level: int | None = None,
    artifact_aggregate: bool = False,
    since: int | None = None,
    until: int | None = None,
) -> list[sqlite3.Row]:
    """
    Возвращает продажи для заданного предмета / среза / атрибутов.

    Параметры атрибутов — все опциональные:
      qlt, ptn                      — фильтр по артефакту (JOIN sale_attrs)
      qlt + artifact_aggregate=True — агрегат по всем ptn данного qlt (L2)
      qlt (без ptn, без флага)      — фильтр по qlt_only предмету (ptn IS NULL)
      upgrade_level                 — фильтр по улучшаемому предмету
      без атрибутов                 — все продажи предмета в данном временном срезе

    Единый шаблон запроса: опциональный JOIN строится только если нужен.

    Args:
        conn               : Соединение с БД (row_factory не требуется снаружи).
        item_id            : Идентификатор предмета.
        granularity        : Тип временного среза.
        bucket_key         : Ключ среза.
        qlt                : Фильтр quality. None = без фильтра.
        ptn                : Фильтр potential. None = без фильтра.
        upgrade_level      : Фильтр upgrade_level. None = без фильтра.
        artifact_aggregate : True — агрегировать все ptn данного qlt (L2-агрегат
                             артефакта). Подавляет автоматический фильтр «ptn IS NULL»,
                             который иначе применяется при qlt без ptn (qlt_only-режим).
                             Игнорируется, если ptn задан явно.
        since              : Нижняя граница sold_at (включительно).
        until              : Верхняя граница sold_at (исключительно).

    Returns:
        Список sqlite3.Row: id, item_id, price, amount, price_per_unit, sold_at.
    """
    if granularity not in get_supported_granularities():
        raise ValueError(f"Неподдерживаемая гранулярность: {granularity!r}")

    original_factory = conn.row_factory
    conn.row_factory  = sqlite3.Row

    try:
        time_sql, params = _build_time_filter(granularity, bucket_key)
        params["item_id"] = item_id

        where_parts = ["s.item_id = :item_id", f"({time_sql})"]

        if since is not None:
            params["since"] = since
            where_parts.append("s.sold_at >= :since")
        if until is not None:
            params["until"] = until
            where_parts.append("s.sold_at < :until")

        # Строим JOIN и атрибутные условия
        attr_join  = ""
        attr_parts: list[str] = []

        need_attrs = qlt is not None or ptn is not None or upgrade_level is not None
        if need_attrs:
            attr_join = "JOIN sale_attrs a ON a.sale_id = s.id"
            if qlt is not None:
                params["_qlt"] = qlt
                attr_parts.append("a.qlt = :_qlt")
            if ptn is not None:
                params["_ptn"] = ptn
                attr_parts.append("a.ptn = :_ptn")
            elif qlt is not None and not artifact_aggregate:
                # qlt_only-режим: ptn должен быть NULL.
                # При artifact_aggregate=True фильтр подавляется — нужны все ptn
                # данного qlt (L2-агрегат артефакта).
                attr_parts.append("a.ptn IS NULL")
            if upgrade_level is not None:
                params["_ul"] = upgrade_level
                attr_parts.append("a.upgrade_level = :_ul")

        where_sql = " AND ".join(where_parts + attr_parts)

        query = f"""
            SELECT s.id, s.item_id, s.price, s.amount, s.price_per_unit, s.sold_at
            FROM sales s
            {attr_join}
            WHERE {where_sql}
            ORDER BY s.sold_at ASC
        """

        return conn.execute(query, params).fetchall()

    finally:
        conn.row_factory = original_factory


def get_distinct_attr_slices(
    conn: sqlite3.Connection,
    item_id: str,
    attr_type: str,
) -> list[dict]:
    """
    Возвращает все уникальные срезы атрибутов для предмета.

    Используется в recalculate_analytics_* чтобы определить,
    по каким комбинациям нужно считать аналитику.

    Returns:
        Список dict. Ключи зависят от attr_type:
          artifact: [{"qlt": 3, "ptn": 13}, ...]
          qlt_only: [{"qlt": 2}, ...]
          upgrade:  [{"upgrade_level": 7}, ...]
          none:     [] (обычному предмету не нужна разбивка)
    """
    if attr_type == ATTR_TYPE_ARTIFACT:
        rows = conn.execute(
            """
            SELECT DISTINCT a.qlt, a.ptn
            FROM sale_attrs a
            JOIN sales s ON s.id = a.sale_id
            WHERE s.item_id = ? AND a.qlt IS NOT NULL AND a.ptn IS NOT NULL
            ORDER BY a.qlt, a.ptn
            """,
            (item_id,),
        ).fetchall()
        return [{"qlt": r[0], "ptn": r[1]} for r in rows]

    if attr_type == ATTR_TYPE_QLT_ONLY:
        rows = conn.execute(
            """
            SELECT DISTINCT a.qlt
            FROM sale_attrs a
            JOIN sales s ON s.id = a.sale_id
            WHERE s.item_id = ? AND a.qlt IS NOT NULL AND a.ptn IS NULL
            ORDER BY a.qlt
            """,
            (item_id,),
        ).fetchall()
        return [{"qlt": r[0]} for r in rows]

    if attr_type == ATTR_TYPE_UPGRADE:
        rows = conn.execute(
            """
            SELECT DISTINCT a.upgrade_level
            FROM sale_attrs a
            JOIN sales s ON s.id = a.sale_id
            WHERE s.item_id = ? AND a.upgrade_level IS NOT NULL
            ORDER BY a.upgrade_level
            """,
            (item_id,),
        ).fetchall()
        return [{"upgrade_level": r[0]} for r in rows]

    return []
