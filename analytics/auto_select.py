"""
analytics/auto_select.py
========================
Архитектура: Базовая стратегия (S1/S2/S3) + Модификаторы (M_TEMPORAL, M_SUPPLY_SHOCK, M_LADDER).
"""

from __future__ import annotations
import sqlite3
from dataclasses import dataclass, field
from enum import Enum

import api.utils.logger
from schema import ATTR_SENTINEL
import math
log = api.utils.logger.get_logger(__name__)

# Настройки
MIN_SPREAD_PCT: float = 5.0
SALES_PER_DAY_CAP: float = 30.0  # выше — расходники/топ без арбитражного потенциала
TWO_WEEKS_SEC: int = 14 * 86400
MIN_VIABLE_SPD: float = 2.0      # ниже — штраф за неликвидность

# ---------------------------------------------------------------------------
# Типы
# ---------------------------------------------------------------------------

class BaseStrategy(str, Enum):
    S1_BULK_ASM  = "S1_BULK_ASM"    # Розница < Опт
    S2_BULK_SPLIT = "S2_BULK_SPLIT" # Опт < Розница
    S3_MEAN_REV  = "S3_MEAN_REV"    # Нет спреда (реверсия)

class Modifier(str, Enum):
    M_TEMPORAL    = "M_TEMPORAL"     # Временной арбитраж
    M_SUPPLY_SHOCK = "M_SUPPLY_SHOCK" # Дефицит предложения
    M_LADDER      = "M_LADDER"       # Лестница объёмов

@dataclass(slots=True)
class Candidate:
    item_id:       str
    name_ru:       str
    category:      str
    attr_type:     str
    base_strategy: BaseStrategy
    modifiers:     list[Modifier] = field(default_factory=list)
    score:         float = 0.0

    # Данные для скоринга и UI
    price_spread:    float = 0.0
    avg_spd:         float = 0.0
    weeks_covered:   int   = 0
    s1_demand_ratio: float | None = None  # Только для S1
    min_lot_price:   float | None = None  # Для M_TEMPORAL
    supply_ratio:    float | None = None  # Для M_SUPPLY_SHOCK (текущие лоты / исторические продажи)

    qlt:           int = ATTR_SENTINEL
    ptn:           int = ATTR_SENTINEL
    upgrade_level: int = ATTR_SENTINEL

    @property
    def tag(self) -> str:
        base = self.base_strategy.value
        mods = " + ".join(m.value for m in self.modifiers)
        return f"{base} + {mods}" if mods else base

# ---------------------------------------------------------------------------
# Логика классификации
# ---------------------------------------------------------------------------

def _assign_base(price_spread: float) -> BaseStrategy:
    if price_spread < -MIN_SPREAD_PCT:
        return BaseStrategy.S1_BULK_ASM
    if price_spread > MIN_SPREAD_PCT:
        return BaseStrategy.S2_BULK_SPLIT
    return BaseStrategy.S3_MEAN_REV


def _detect_modifiers(
    c: Candidate,
    hist: dict,
    snap: dict | None,
    windows: list[dict] | None,
) -> list[Modifier]:
    mods = []

    # --- M_TEMPORAL: текущая минимальная цена лота ниже среднего в самом дешёвом окне ---
    if c.min_lot_price and windows:
        cheapest_win_avg = min(
            (w["avg_price"] for w in windows if w.get("avg_price")), default=None
        )
        if cheapest_win_avg and c.min_lot_price < cheapest_win_avg * 0.95:
            mods.append(Modifier.M_TEMPORAL)

    # --- M_SUPPLY_SHOCK: текущих лотов аномально мало ---
    avg_spd = hist.get("avg_spd") or 0.0
    if snap and avg_spd > 0:
        expected_lots_weekly = avg_spd * 7
        ratio = snap.get("total_lots", 0) / expected_lots_weekly
        c.supply_ratio = ratio
        if ratio < 0.3:
            mods.append(Modifier.M_SUPPLY_SHOCK)

    # --- M_LADDER: есть прибыльная середина (для S2 и S3) ---
    if c.base_strategy in (BaseStrategy.S2_BULK_SPLIT, BaseStrategy.S3_MEAN_REV):
        mode = hist.get("amount_mode")
        p50  = hist.get("amount_p50")
        if mode and p50 and 1 < int(mode) < p50:
            mods.append(Modifier.M_LADDER)

    # --- Специфика S1: ratio спроса/предложения bulk (влияет на скоринг) ---
    if c.base_strategy == BaseStrategy.S1_BULK_ASM and snap:
        bulk_spd_weekly = (hist.get("bulk_share") or 0.0) * avg_spd * 7
        if bulk_spd_weekly > 0:
            c.s1_demand_ratio = snap.get("bulk_lots", 0) / bulk_spd_weekly

    return mods

# ---------------------------------------------------------------------------
# Скоринг (База + Бонусы за модификаторы)
# ---------------------------------------------------------------------------

def _score_candidate(c: Candidate) -> float:
    # Базовый скор от спреда (0..60 баллов)
    abs_spread = abs(c.price_spread)
    base_score = min(60.0, 20.0 + 40.0 * (1.0 - 1.0 / (1.0 + abs_spread / 15.0)))

    # Ликвидность (до +30)
    spd = c.avg_spd

    if spd < MIN_VIABLE_SPD:
        # Штраф: чем ниже спрос — тем сильнее срезаем итоговый скор
        liq_score = -20.0 * (1.0 - spd / MIN_VIABLE_SPD)
    elif spd <= SALES_PER_DAY_CAP:
        # Логарифм: рост быстрый в начале, плавный ближе к cap
        liq_score = 30.0 * math.log(1 + spd) / math.log(1 + SALES_PER_DAY_CAP)
    else:
        return 0.0
    
    # Специфичные штрафы/бонусы S1
    strategy_bonus = 0.0
    if c.base_strategy == BaseStrategy.S1_BULK_ASM and c.s1_demand_ratio is not None:
        if c.s1_demand_ratio > 1.5:
            return 0.0  # переизбыток предложения bulk
        if c.s1_demand_ratio < 0.5:
            strategy_bonus = 10.0  # дефицит bulk

    raw = base_score + liq_score + strategy_bonus

    # Бонусы модификаторов
    if Modifier.M_TEMPORAL    in c.modifiers:
        raw += 15.0
    if Modifier.M_SUPPLY_SHOCK in c.modifiers:
        raw += 20.0
    if Modifier.M_LADDER       in c.modifiers:
        raw += 10.0


    if c.weeks_covered < 3:
        raw *= 0.7   # -30% за недостаточную историю
    return round(max(0.0, min(100.0, raw)), 1)

# ---------------------------------------------------------------------------
# SQL: сбор данных для кандидатов
# ---------------------------------------------------------------------------

_GET_CANDIDATES_SQL = f"""
WITH
hist AS (
    SELECT
        a.item_id, a.qlt, a.ptn, a.upgrade_level,
        AVG(a.price_single)  AS price_single,
        AVG(a.price_bulk)    AS price_bulk,
        AVG(a.price_spread)  AS price_spread,
        AVG(a.amount_p50)    AS amount_p50,
        AVG(a.amount_mode)   AS amount_mode,
        AVG(a.bulk_share)    AS bulk_share,
        AVG(a.volatility)    AS volatility,
        AVG(a.sales_per_day) AS avg_spd,
        COUNT(*)             AS weeks_covered
    FROM analytics_summary a
    WHERE a.granularity = 'weekly'
      AND a.low_sample  = 0
      AND a.total_amount > 0
    GROUP BY a.item_id, a.qlt, a.ptn, a.upgrade_level
    HAVING weeks_covered >= 2
       AND AVG(a.sales_per_day) <= {SALES_PER_DAY_CAP}
       AND AVG(a.volatility) > 0.25
),
snap AS (
    SELECT * FROM lot_snapshots WHERE updated_at > (strftime('%s', 'now') - 3600)
)
SELECT
    h.item_id, COALESCE(i.name_ru, h.item_id) AS name_ru, i.category, i.attr_type,
    h.qlt, h.ptn, h.upgrade_level,
    h.weeks_covered,
    h.price_single, h.price_bulk, h.price_spread,
    h.amount_p50, h.amount_mode, h.bulk_share, h.volatility, h.avg_spd,
    s.total_lots, s.bulk_lots, s.min_price_pu
FROM hist h
JOIN items i ON i.item_id = h.item_id
LEFT JOIN snap s ON s.item_id = h.item_id AND s.qlt = h.qlt AND s.ptn = h.ptn
WHERE
    i.last_sale_at > (strftime('%s', 'now') - {TWO_WEEKS_SEC})
    AND i.category NOT IN ('bullet')
    AND (
        h.price_spread IS NOT NULL
        OR i.attr_type != 'none'
    )
    AND h.price_single IS NOT NULL AND h.price_bulk IS NOT NULL AND h.price_bulk > 0
    AND h.item_id NOT IN (SELECT item_id FROM ignored_items)
ORDER BY ABS(h.price_spread) DESC

"""

# ---------------------------------------------------------------------------
# Главный пайплайн
# ---------------------------------------------------------------------------

def find_candidates(conn: sqlite3.Connection) -> list[Candidate]:
    original_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(_GET_CANDIDATES_SQL).fetchall()
    finally:
        conn.row_factory = original_factory
    candidates = []

    for r in rows:
        spread = r["price_spread"] or 0.0
        c = Candidate(
            item_id=r["item_id"], name_ru=r["name_ru"], category=r["category"],
            weeks_covered=r["weeks_covered"],
            attr_type=r["attr_type"], qlt=r["qlt"], ptn=r["ptn"],
            upgrade_level=r["upgrade_level"],
            price_spread=spread, avg_spd=r["avg_spd"] or 0.0,
            min_lot_price=r["min_price_pu"],
            base_strategy=_assign_base(spread),
        )

        hist_data = {
            "avg_spd":      c.avg_spd,
            "bulk_share":   r["bulk_share"],
            "amount_mode":  r["amount_mode"],
            "amount_p50":   r["amount_p50"],
        }
        snap_data = (
            {"total_lots": r["total_lots"], "bulk_lots": r["bulk_lots"]}
            if r["total_lots"] is not None else None
        )

        c.modifiers = _detect_modifiers(c, hist_data, snap_data, windows=None)
        c.score = _score_candidate(c)

        if c.score > 0:
            candidates.append(c)

    candidates.sort(key=lambda x: x.score, reverse=True)
    return candidates


def sync_candidates_to_watched(
    conn: sqlite3.Connection,
    candidates: list[Candidate],
) -> tuple[int, int]:
    """Полностью перезаписывает watched_items списком кандидатов.

    Удаляет предметы, которые не прошли скоринг в этом прогоне.
    Добавляет новые. Предметы из ignored_items пропускаются автоматически
    (они не попадают в candidates через SQL-запрос).

    Args:
        conn:       Соединение с БД.
        candidates: Отсортированный по score список кандидатов.

    Returns:
        (added, total) — новых предметов и итоговый размер watched_items.
    """
    # Текущее состояние watched_items
    existing: set[tuple] = {
        (r[0], r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT item_id, qlt, ptn, upgrade_level FROM watched_items"
        ).fetchall()
    }

    # Новый желаемый список
    incoming: list[tuple] = [
        (c.item_id, c.qlt, c.ptn, c.upgrade_level)
        for c in candidates
    ]
    incoming_set: set[tuple] = set(incoming)

    to_add    = incoming_set - existing
    to_remove = existing - incoming_set

    cur = conn.cursor()

    for item_id, qlt, ptn, upgrade_level in to_remove:
        cur.execute(
            "DELETE FROM watched_items WHERE item_id=? AND qlt=? AND ptn=? AND upgrade_level=?",
            (item_id, qlt, ptn, upgrade_level),
        )
        log.info("Автоотбор: -%s [%s/%s/%s] исключён.", item_id, qlt, ptn, upgrade_level)

    for c in candidates:
        key = (c.item_id, c.qlt, c.ptn, c.upgrade_level)
        if key in to_add:
            cur.execute(
                "INSERT OR IGNORE INTO watched_items (item_id, qlt, ptn, upgrade_level)"
                " VALUES (?, ?, ?, ?)",
                key,
            )
            log.info(
                "Автоотбор: +%s [%s] score=%.1f (%s)",
                c.name_ru, c.item_id, c.score, c.tag,
            )

    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM watched_items").fetchone()[0]
    return len(to_add), total