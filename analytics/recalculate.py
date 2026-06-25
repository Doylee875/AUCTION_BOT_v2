"""
analytics/recalculate.py
========================
Пересчёт аналитики по типу предмета (attr_type).

Паттерн доступа к данным:
    Все recalculate_for_* читают ВСЕ продажи предмета ОДНИМ запросом
    (fetch_all_sales_for_item), затем группируют результат в памяти
    по (granularity → bucket_key → атрибутный срез) через group_by_bucket().
    Это O(1) запросов на предмет вместо O(N_gran × N_buckets × N_slices).

Сценарий А — двухуровневая иерархия для артефактов:
    Уровень 1 (точный срез):   qlt=3, ptn=13
    Уровень 2 (qlt-агрегат):   qlt=3, ptn=-1  (все ptn данного qlt)

Публичный интерфейс:
    recalculate_for_item(conn, item_id, granularities=None)
    recalculate_for_artifact(conn, item_id, granularities=None)
    recalculate_for_qlt_only(conn, item_id, granularities=None)
    recalculate_for_upgradeable(conn, item_id, granularities=None)
    recalculate_for_any(conn, item_id, attr_type, granularities=None)
"""

import sqlite3

from analytics.bucket import get_supported_granularities
from analytics.compute import compute_metrics
from analytics.storage import fetch_all_sales_for_item, group_by_bucket, save_metrics
from schema import (
    ATTR_SENTINEL,
    ATTR_TYPE_ARTIFACT,
    ATTR_TYPE_NONE,
    ATTR_TYPE_QLT_ONLY,
    ATTR_TYPE_UPGRADE,
)

import api.utils.logger

log = api.utils.logger.get_logger(__name__)


# ---------------------------------------------------------------------------
# Обычный предмет
# ---------------------------------------------------------------------------

def recalculate_for_item(
    conn: sqlite3.Connection,
    item_id: str,
    granularities: list[str] | None = None,
) -> None:
    """
    Пересчитывает аналитику для обычного предмета (attr_type='none').

    Пишет в analytics_summary с qlt=ptn=upgrade_level=-1.
    """
    if granularities is None:
        granularities = get_supported_granularities()

    all_rows = fetch_all_sales_for_item(conn, item_id)
    if not all_rows:
        log.debug("recalculate_for_item: нет данных для %s", item_id)
        return

    for gran in granularities:
        for bk, rows in group_by_bucket(all_rows, gran).items():
            try:
                metrics = compute_metrics(rows, gran, bk)
                if metrics["avg_price"] is None:
                    continue
                save_metrics(conn, item_id, gran, bk, metrics)
            except Exception as exc:
                log.warning("recalculate_for_item: %s %s %s: %s", item_id, gran, bk, exc)

    conn.commit()
    log.debug("recalculate_for_item: %s завершён.", item_id)


# ---------------------------------------------------------------------------
# Артефакты (Сценарий А — два уровня)
# ---------------------------------------------------------------------------

def recalculate_for_artifact(
    conn: sqlite3.Connection,
    item_id: str,
    granularities: list[str] | None = None,
) -> None:
    """
    Пересчитывает аналитику для артефакта (attr_type='artifact').

    Сценарий А — для каждого qlt пишет два уровня:

    Уровень 1 — точный срез (qlt, ptn):
        Строка analytics_summary с реальными qlt=3, ptn=13.

    Уровень 2 — qlt-агрегат (qlt, ptn=-1):
        Объединяет все продажи данного qlt независимо от ptn.
        Используется в UI как строка первого уровня при раскрытии.

    Итого строк: (N_qlt × N_ptn + N_qlt) × N_gran × N_buckets.
    """
    if granularities is None:
        granularities = get_supported_granularities()

    all_rows = fetch_all_sales_for_item(conn, item_id)
    if not all_rows:
        log.debug("recalculate_for_artifact: нет данных для %s", item_id)
        return

    artifact_rows = [
        r for r in all_rows
        if r["qlt"] is not None and r["ptn"] is not None
    ]
    if not artifact_rows:
        log.info("recalculate_for_artifact: нет атрибутных строк для %s", item_id)
        return

    seen_pairs: dict[tuple[int, int], None] = dict.fromkeys(
        (r["qlt"], r["ptn"]) for r in artifact_rows
    )
    seen_qlts: dict[int, None] = dict.fromkeys(r["qlt"] for r in artifact_rows)

    for gran in granularities:
        for bk, bk_rows in group_by_bucket(artifact_rows, gran).items():

            # Уровень 1: точные срезы (qlt, ptn)
            l1_groups: dict[tuple[int, int], list] = {}
            for r in bk_rows:
                key = (r["qlt"], r["ptn"])
                if key not in l1_groups:
                    l1_groups[key] = []
                l1_groups[key].append(r)

            for (qlt_val, ptn_val), rows in l1_groups.items():
                try:
                    metrics = compute_metrics(rows, gran, bk)
                    if metrics["avg_price"] is None:
                        continue
                    save_metrics(conn, item_id, gran, bk, metrics,
                                 qlt=qlt_val, ptn=ptn_val)
                except Exception as exc:
                    log.warning(
                        "recalculate_for_artifact L1: %s %s %s qlt=%s ptn=%s: %s",
                        item_id, gran, bk, qlt_val, ptn_val, exc,
                    )

            # Уровень 2: qlt-агрегаты (ptn=ATTR_SENTINEL)
            l2_groups: dict[int, list] = {}
            for r in bk_rows:
                qv = r["qlt"]
                if qv not in l2_groups:
                    l2_groups[qv] = []
                l2_groups[qv].append(r)

            for qlt_val, rows in l2_groups.items():
                try:
                    metrics = compute_metrics(rows, gran, bk)
                    if metrics["avg_price"] is None:
                        continue
                    save_metrics(conn, item_id, gran, bk, metrics,
                                 qlt=qlt_val, ptn=ATTR_SENTINEL)
                except Exception as exc:
                    log.warning(
                        "recalculate_for_artifact L2: %s %s %s qlt=%s: %s",
                        item_id, gran, bk, qlt_val, exc,
                    )

    conn.commit()
    log.debug(
        "recalculate_for_artifact: %s завершён (%d пар qlt×ptn, %d уник. qlt).",
        item_id, len(seen_pairs), len(seen_qlts),
    )


# ---------------------------------------------------------------------------
# qlt_only (ядра модулей и т.п.)
# ---------------------------------------------------------------------------

def recalculate_for_qlt_only(
    conn: sqlite3.Connection,
    item_id: str,
    granularities: list[str] | None = None,
) -> None:
    """
    Пересчитывает аналитику для qlt_only предмета (attr_type='qlt_only').

    Один уровень: qlt=N, ptn=-1, upgrade_level=-1.
    Фильтр: qlt IS NOT NULL AND ptn IS NULL.
    """
    if granularities is None:
        granularities = get_supported_granularities()

    all_rows = fetch_all_sales_for_item(conn, item_id)
    if not all_rows:
        log.debug("recalculate_for_qlt_only: нет данных для %s", item_id)
        return

    qlt_rows = [r for r in all_rows if r["qlt"] is not None and r["ptn"] is None]
    if not qlt_rows:
        log.debug("recalculate_for_qlt_only: нет qlt_only строк для %s", item_id)
        return

    unique_qlts = list(dict.fromkeys(r["qlt"] for r in qlt_rows))

    for gran in granularities:
        for bk, bk_rows in group_by_bucket(qlt_rows, gran).items():
            qlt_groups: dict[int, list] = {}
            for r in bk_rows:
                qv = r["qlt"]
                if qv not in qlt_groups:
                    qlt_groups[qv] = []
                qlt_groups[qv].append(r)

            for qlt_val, rows in qlt_groups.items():
                try:
                    metrics = compute_metrics(rows, gran, bk)
                    if metrics["avg_price"] is None:
                        continue
                    save_metrics(conn, item_id, gran, bk, metrics, qlt=qlt_val)
                except Exception as exc:
                    log.warning(
                        "recalculate_for_qlt_only: %s %s %s qlt=%s: %s",
                        item_id, gran, bk, qlt_val, exc,
                    )

    conn.commit()
    log.info("recalculate_for_qlt_only: %s завершён (%d qlt).", item_id, len(unique_qlts))


# ---------------------------------------------------------------------------
# Улучшаемые предметы
# ---------------------------------------------------------------------------

def recalculate_for_upgradeable(
    conn: sqlite3.Connection,
    item_id: str,
    granularities: list[str] | None = None,
) -> None:
    """
    Пересчитывает аналитику для улучшаемого предмета (attr_type='upgrade').

    Один уровень: qlt=-1, ptn=-1, upgrade_level=N.
    Фильтр: upgrade_level IS NOT NULL.
    """
    if granularities is None:
        granularities = get_supported_granularities()

    all_rows = fetch_all_sales_for_item(conn, item_id)
    if not all_rows:
        log.debug("recalculate_for_upgradeable: нет данных для %s", item_id)
        return

    upgrade_rows = [r for r in all_rows if r["upgrade_level"] is not None]
    if not upgrade_rows:
        log.debug("recalculate_for_upgradeable: нет upgrade строк для %s", item_id)
        return

    unique_levels = list(dict.fromkeys(r["upgrade_level"] for r in upgrade_rows))

    for gran in granularities:
        for bk, bk_rows in group_by_bucket(upgrade_rows, gran).items():
            lvl_groups: dict[int, list] = {}
            for r in bk_rows:
                lv = r["upgrade_level"]
                if lv not in lvl_groups:
                    lvl_groups[lv] = []
                lvl_groups[lv].append(r)

            for lvl, rows in lvl_groups.items():
                try:
                    metrics = compute_metrics(rows, gran, bk)
                    if metrics["avg_price"] is None:
                        continue
                    save_metrics(conn, item_id, gran, bk, metrics, upgrade_level=lvl)
                except Exception as exc:
                    log.warning(
                        "recalculate_for_upgradeable: %s %s %s lvl=%s: %s",
                        item_id, gran, bk, lvl, exc,
                    )

    conn.commit()
    log.info(
        "recalculate_for_upgradeable: %s завершён (%d уровней).",
        item_id, len(unique_levels),
    )


# ---------------------------------------------------------------------------
# Диспетчер
# ---------------------------------------------------------------------------

_RECALC_DISPATCH = {
    ATTR_TYPE_NONE:     recalculate_for_item,
    ATTR_TYPE_ARTIFACT: recalculate_for_artifact,
    ATTR_TYPE_QLT_ONLY: recalculate_for_qlt_only,
    ATTR_TYPE_UPGRADE:  recalculate_for_upgradeable,
}


def recalculate_for_any(
    conn: sqlite3.Connection,
    item_id: str,
    attr_type: str,
    granularities: list[str] | None = None,
) -> None:
    """
    Пересчитывает аналитику для предмета любого типа.

    Выбирает нужную recalculate_for_* по attr_type.
    Используется в fetcher_anal.py — не нужно знать тип заранее.
    """
    fn = _RECALC_DISPATCH.get(attr_type)
    if fn is None:
        log.warning("recalculate_for_any: неизвестный attr_type=%r для %s", attr_type, item_id)
        return
    fn(conn, item_id, granularities)
