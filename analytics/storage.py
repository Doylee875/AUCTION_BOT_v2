"""
analytics/storage.py
====================
Чтение продаж из БД и запись метрик в analytics_summary.

Публичный интерфейс:
    save_metrics(conn, item_id, granularity, bucket_key, metrics, *, qlt, ptn, upgrade_level)
    fetch_all_sales_for_item(conn, item_id) → list[sqlite3.Row]
    group_by_bucket(rows, granularity)      → dict[str, list[sqlite3.Row]]
"""

import sqlite3
from collections import defaultdict

from analytics.bucket import calculate_bucket_key
from schema import ATTR_SENTINEL

import api.utils.logger

log = api.utils.logger.get_logger(__name__)


def save_metrics(
    conn: sqlite3.Connection,
    item_id: str,
    granularity: str,
    bucket_key: str,
    metrics: dict[str, float | None],
    *,
    qlt: int = ATTR_SENTINEL,
    ptn: int = ATTR_SENTINEL,
    upgrade_level: int = ATTR_SENTINEL,
) -> None:
    """
    Вставляет или заменяет одну строку в analytics_summary.

    Значения по умолчанию qlt=ptn=upgrade_level=ATTR_SENTINEL (-1) — для
    обычных предметов. Артефакты передают реальные значения.

    INSERT OR REPLACE корректен — sentinel -1 устраняет проблему NULL в PK.
    """
    conn.execute(
        """
        INSERT OR REPLACE INTO analytics_summary
            (item_id, granularity, bucket_key,
             qlt, ptn, upgrade_level,
             liquidity, sales_per_day, avg_price, volatility, trend,
             total_amount,
             amount_p50, price_single, price_bulk, bulk_share, vol_single,
             price_spread, amount_mode, spread_stable,
             low_sample, calculated_at)
        VALUES
            (:item_id, :granularity, :bucket_key,
             :qlt, :ptn, :upgrade_level,
             :liquidity, :sales_per_day, :avg_price, :volatility, :trend,
             :total_amount,
             :amount_p50, :price_single, :price_bulk, :bulk_share, :vol_single,
             :price_spread, :amount_mode, :spread_stable,
             :low_sample, strftime('%s', 'now'))
        """,
        {
            "item_id":       item_id,
            "granularity":   granularity,
            "bucket_key":    bucket_key,
            "qlt":           qlt,
            "ptn":           ptn,
            "upgrade_level": upgrade_level,
            "liquidity":     metrics.get("liquidity"),
            "sales_per_day": metrics.get("sales_per_day"),
            "avg_price":     metrics.get("avg_price"),
            "volatility":    metrics.get("volatility"),
            "trend":         metrics.get("trend"),
            "total_amount":  metrics.get("total_amount"),
            "amount_p50":    metrics.get("amount_p50"),
            "price_single":  metrics.get("price_single"),
            "price_bulk":    metrics.get("price_bulk"),
            "bulk_share":    metrics.get("bulk_share"),
            "vol_single":    metrics.get("vol_single"),
            "price_spread":  metrics.get("price_spread"),
            "amount_mode":   metrics.get("amount_mode"),
            "spread_stable": 1 if metrics.get("spread_stable") else 0,
            "low_sample":    1 if metrics.get("low_sample") else 0,
        },
    )


def fetch_all_sales_for_item(
    conn: sqlite3.Connection,
    item_id: str,
) -> list[sqlite3.Row]:
    """
    Читает ВСЕ продажи предмета одним запросом, включая атрибуты.

    Возвращает строки с полями:
        price_per_unit, amount, sold_at, qlt, ptn, upgrade_level.

    LEFT JOIN гарантирует, что продажи без атрибутов (обычные предметы,
    attr_type='none') не теряются — для них qlt/ptn/upgrade_level = NULL.

    Каждая recalculate_for_* вызывает эту функцию один раз, после чего
    фильтрует и группирует строки в памяти — вместо O(N_gran × N_buckets
    × N_slices) отдельных запросов.
    """
    original_factory = conn.row_factory
    conn.row_factory  = sqlite3.Row
    try:
        return conn.execute(
            """
            SELECT s.price_per_unit,
                   s.amount,
                   s.sold_at,
                   a.qlt,
                   a.ptn,
                   a.upgrade_level
            FROM   sales s
            LEFT JOIN sale_attrs a ON a.sale_id = s.id
            WHERE  s.item_id = ?
            ORDER  BY s.sold_at ASC
            """,
            (item_id,),
        ).fetchall()
    finally:
        conn.row_factory = original_factory


def group_by_bucket(
    rows: list[sqlite3.Row],
    granularity: str,
) -> dict[str, list[sqlite3.Row]]:
    """
    Группирует уже загруженные строки продаж по bucket_key.

    Вызывается на уже загруженном списке — никаких обращений к БД.
    Возвращает только непустые bucket'ы.
    """
    groups: dict[str, list] = defaultdict(list)
    for row in rows:
        bk = calculate_bucket_key(row["sold_at"], granularity)
        groups[bk].append(row)
    return dict(groups)
