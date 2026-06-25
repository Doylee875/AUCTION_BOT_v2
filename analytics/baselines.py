"""
analytics/baselines.py
======================
Пересчёт базовых линий по категориям и относительного объёма.
Также содержит get_best_slice() — запрос лучшего среза для UI.

Публичный интерфейс:
    recalculate_baselines(conn, granularities=None)
    apply_relative_volume(conn, granularities=None)
    get_best_slice(conn, item_id, granularity="daily") → dict | None
"""

import sqlite3
import statistics

from schema import ATTR_SENTINEL

import api.utils.logger

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

_BASELINE_GRANS      = frozenset({"window_weekday", "weekly", "monthly"})
_MIN_ITEMS_BASELINE  = 3


# ---------------------------------------------------------------------------
# Базовые линии
# ---------------------------------------------------------------------------

def recalculate_baselines(
    conn: sqlite3.Connection,
    granularities: list[str] | None = None,
) -> None:
    """
    Пересчитывает медианный объём по категориям.

    Читает total_amount из analytics_summary (только строки с ptn=-1 и
    upgrade_level=-1, чтобы не считать артефакты дважды), группирует по
    (granularity, bucket_key, category) и пишет медиану в analytics_baselines.

    Вызывать после recalculate_for_* для всех предметов.
    """
    if granularities is None:
        target = _BASELINE_GRANS
    else:
        target = frozenset(granularities) & _BASELINE_GRANS
        if not target:
            log.warning(
                "recalculate_baselines: ни одна гранулярность не входит в %s.",
                _BASELINE_GRANS,
            )
            return

    placeholders = ",".join("?" * len(target))

    rows = conn.execute(
        f"""
        SELECT
            a.granularity,
            a.bucket_key,
            i.category,
            a.total_amount
        FROM analytics_summary AS a
        JOIN items AS i ON i.item_id = a.item_id
        WHERE a.granularity IN ({placeholders})
          AND a.ptn          = {ATTR_SENTINEL}
          AND a.upgrade_level = {ATTR_SENTINEL}
          AND a.total_amount IS NOT NULL
        ORDER BY a.granularity, a.bucket_key, i.category
        """,
        tuple(target),
    ).fetchall()

    from collections import defaultdict
    groups: dict[tuple, list[float]] = defaultdict(list)
    for row in rows:
        groups[(row[0], row[1], row[2])].append(float(row[3]))

    inserted = skipped = 0
    for (gran, bk, cat), amounts in groups.items():
        if len(amounts) < _MIN_ITEMS_BASELINE:
            skipped += 1
            continue
        median_val = statistics.median(amounts)
        conn.execute(
            """
            INSERT OR REPLACE INTO analytics_baselines
                (granularity, bucket_key, category, median_amount, item_count, calculated_at)
            VALUES (?, ?, ?, ?, ?, strftime('%s', 'now'))
            """,
            (gran, bk, cat, median_val, len(amounts)),
        )
        inserted += 1

    conn.commit()
    log.info(
        "recalculate_baselines: %d базовых линий записано, %d пропущено (< %d предметов).",
        inserted, skipped, _MIN_ITEMS_BASELINE,
    )


# ---------------------------------------------------------------------------
# Относительный объём
# ---------------------------------------------------------------------------

def apply_relative_volume(
    conn: sqlite3.Connection,
    granularities: list[str] | None = None,
) -> None:
    """
    Обновляет relative_volume в analytics_summary на основе analytics_baselines.

    Формула: relative_volume = total_amount / median_amount_по_категории.
    Применяется только к строкам с ptn=-1 (агрегатные и обычные предметы).

    Вызывать после recalculate_baselines().
    """
    if granularities is None:
        target = _BASELINE_GRANS
    else:
        target = frozenset(granularities) & _BASELINE_GRANS

    placeholders = ",".join("?" * len(target))

    conn.execute(
        f"""
        UPDATE analytics_summary
        SET relative_volume = (
            SELECT
                CASE
                    WHEN b.median_amount > 0
                    THEN CAST(analytics_summary.total_amount AS REAL) / b.median_amount
                    ELSE NULL
                END
            FROM analytics_baselines AS b
            JOIN items AS i ON i.item_id = analytics_summary.item_id
            WHERE b.granularity = analytics_summary.granularity
              AND b.bucket_key  = analytics_summary.bucket_key
              AND b.category    = i.category
            LIMIT 1
        )
        WHERE granularity IN ({placeholders})
          AND ptn          = {ATTR_SENTINEL}
          AND total_amount IS NOT NULL
        """,
        tuple(target),
    )

    updated = conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    log.info("apply_relative_volume: обновлено %d строк.", updated)


# ---------------------------------------------------------------------------
# UI: лучший срез
# ---------------------------------------------------------------------------

def get_best_slice(
    conn: sqlite3.Connection,
    item_id: str,
    granularity: str = "daily",
) -> dict | None:
    """
    Возвращает наиболее ликвидный срез атрибутов для предмета.

    Для артефактов возвращает строку уровня 2 (qlt-агрегат, ptn=-1) с
    максимальным sales_per_day. Для обычных предметов — единственную строку.

    Критерий: MAX(AVG(sales_per_day)), тайбрейкер — MAX(SUM(total_amount)).

    Returns:
        Словарь с ключами: qlt, ptn, upgrade_level, liquidity, sales_per_day,
        avg_price, volatility, trend. None если данных нет.
    """
    original = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            f"""
            SELECT
                qlt, ptn, upgrade_level,
                AVG(liquidity)      AS liquidity,
                AVG(sales_per_day)  AS sales_per_day,
                AVG(avg_price)      AS avg_price,
                AVG(volatility)     AS volatility,
                AVG(trend)          AS trend
            FROM analytics_summary
            WHERE item_id    = :item_id
              AND granularity = :gran
              AND ptn         = {ATTR_SENTINEL}
              AND sales_per_day IS NOT NULL
            GROUP BY qlt, ptn, upgrade_level
            ORDER BY AVG(sales_per_day) DESC, SUM(total_amount) DESC
            LIMIT 1
            """,
            {"item_id": item_id, "gran": granularity},
        ).fetchone()
    finally:
        conn.row_factory = original

    return dict(row) if row is not None else None
