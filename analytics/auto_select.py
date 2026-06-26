"""
analytics/auto_select.py
========================
Архитектура: Базовая стратегия (S1/S2/S3) + Модификаторы (M_TEMPORAL, M_SUPPLY_SHOCK, M_LADDER).

Изменения относительно предыдущей версии:
  - volatility > 0.25 убран из SQL (остался только как фильтр S3 в Python).
  - MIN_SPREAD_PCT адаптивный: max(5.0, volatility * 10) per item.
  - Порог M_TEMPORAL (0.95) масштабируется через volatility предмета.
  - Порог M_SUPPLY_SHOCK остаётся глобальным 0.3 (lot_snapshots_history отсутствует).
  - weeks_covered >= 4 смягчён для новых предметов (< 60 дней по updated_at).
  - MIN_VIABLE_SPD и SALES_PER_DAY_CAP берутся per-category из liquidity_baselines;
    глобальные константы используются только как fallback.
"""

from __future__ import annotations
import sqlite3
from dataclasses import dataclass, field
from enum import Enum

import api.utils.logger
from schema import ATTR_SENTINEL
import math

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Глобальные константы — используются как fallback, когда liquidity_baselines
# не содержит данных по категории предмета
# ---------------------------------------------------------------------------

_SPREAD_PCT_BASE: float  = 5.0    # нижняя граница адаптивного MIN_SPREAD_PCT
_SPREAD_VOL_MULT: float  = 10.0   # множитель volatility для MIN_SPREAD_PCT
SALES_PER_DAY_CAP: float = 30.0   # fallback cap (переопределяется per-category)
TWO_WEEKS_SEC: int       = 14 * 86400
MIN_VIABLE_SPD: float    = 2.0    # fallback (переопределяется per-category)

# Новый предмет: считается «новым» если updated_at < этого порога (в секундах)
_NEW_ITEM_AGE_SEC: int     = 60 * 86400   # 60 дней
_NEW_ITEM_MIN_WEEKS: int   = 2            # смягчённый порог для новых предметов

# Базовый порог покрытия истории (для «зрелых» предметов)
_MATURE_ITEM_MIN_WEEKS: int = 4

# Порог M_TEMPORAL: базовое значение, масштабируется через volatility
_TEMPORAL_THRESHOLD_BASE: float = 0.95

# ---------------------------------------------------------------------------
# Типы
# ---------------------------------------------------------------------------

class BaseStrategy(str, Enum):
    S1_BULK_ASM   = "S1_BULK_ASM"   # Розница < Опт
    S2_BULK_SPLIT = "S2_BULK_SPLIT" # Опт < Розница
    S3_MEAN_REV   = "S3_MEAN_REV"   # Нет спреда (реверсия)

class Modifier(str, Enum):
    M_TEMPORAL     = "M_TEMPORAL"     # Временной арбитраж
    M_SUPPLY_SHOCK = "M_SUPPLY_SHOCK" # Дефицит предложения
    M_LADDER       = "M_LADDER"       # Лестница объёмов

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
    volatility:      float = 0.0
    s1_demand_ratio: float | None = None  # Только для S1
    min_lot_price:   float | None = None  # Для M_TEMPORAL
    supply_ratio:    float | None = None  # Для M_SUPPLY_SHOCK

    # Per-category пороги из liquidity_baselines (None → fallback к глобальным)
    cat_min_viable_spd: float | None = None
    cat_spd_cap:        float | None = None

    qlt:           int = ATTR_SENTINEL
    ptn:           int = ATTR_SENTINEL
    upgrade_level: int = ATTR_SENTINEL

    @property
    def effective_min_viable_spd(self) -> float:
        return self.cat_min_viable_spd if self.cat_min_viable_spd is not None else MIN_VIABLE_SPD

    @property
    def effective_spd_cap(self) -> float:
        return self.cat_spd_cap if self.cat_spd_cap is not None else SALES_PER_DAY_CAP

    @property
    def tag(self) -> str:
        base = self.base_strategy.value
        mods = " + ".join(m.value for m in self.modifiers)
        return f"{base} + {mods}" if mods else base

# ---------------------------------------------------------------------------
# Логика классификации
# ---------------------------------------------------------------------------

def _adaptive_spread_pct(volatility: float) -> float:
    """
    Адаптивный порог MIN_SPREAD_PCT на основе volatility предмета.

    Формула: max(5.0, volatility * 10)

    Для волатильных предметов (vol=0.4) порог = 4.0% → берём floor 5.0%.
    Для очень волатильных (vol=0.8) порог = 8.0% — требуем больший спред.
    Это защищает от ложных S1/S2 сигналов на шумных ценовых рядах.
    """
    return max(_SPREAD_PCT_BASE, volatility * _SPREAD_VOL_MULT)


def _assign_base(price_spread: float, volatility: float) -> BaseStrategy:
    """Классифицирует стратегию с учётом адаптивного порога спреда."""
    threshold = _adaptive_spread_pct(volatility)
    if price_spread < -threshold:
        return BaseStrategy.S1_BULK_ASM
    if price_spread > threshold:
        return BaseStrategy.S2_BULK_SPLIT
    return BaseStrategy.S3_MEAN_REV


def _temporal_threshold(volatility: float) -> float:
    """
    Адаптивный порог для M_TEMPORAL.

    Базовое значение 0.95 для низковолатильных предметов.
    Для волатильных — расширяем зону: высокая volatility означает,
    что «дешёвая цена» — это нормальное состояние, а не аномалия.
    Формула: base - volatility * 0.1, clamped в [0.80, 0.95].

    Примеры:
        vol=0.0  → 0.95  (стабильный предмет, любое отклонение значимо)
        vol=0.25 → 0.925 (умеренная волатильность)
        vol=0.50 → 0.90  (высокая волатильность, нужен больший дисконт)
        vol=1.5  → 0.80  (floor — очень шумный рынок)
    """
    return max(0.80, _TEMPORAL_THRESHOLD_BASE - volatility * 0.10)


def _detect_modifiers(
    c: Candidate,
    hist: dict,
    snap: dict | None,
    windows: list[dict] | None,
) -> list[Modifier]:
    mods = []

    # --- M_TEMPORAL: текущая минимальная цена лота ниже среднего в самом дешёвом окне ---
    # Порог масштабируется через volatility предмета
    if c.min_lot_price and windows:
        cheapest_win_avg = min(
            (w["avg_price"] for w in windows if w.get("avg_price")), default=None
        )
        if cheapest_win_avg:
            threshold = _temporal_threshold(c.volatility)
            if c.min_lot_price < cheapest_win_avg * threshold:
                mods.append(Modifier.M_TEMPORAL)

    # --- M_SUPPLY_SHOCK: текущих лотов аномально мало ---
    # Порог 0.3 — глобальный фиксированный (lot_snapshots_history отсутствует,
    # per-item percentile недоступен)
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
    """
    Скоринг кандидата.

    Использует per-category пороги (effective_min_viable_spd / effective_spd_cap)
    вместо глобальных констант.

    S3 с высокой volatility (> 0.25) не фильтруется в SQL, но получает
    сниженный base_score (меньший abs_spread → меньше баллов), что естественно
    опускает его в рейтинге без явного отсечения.
    """
    min_spd = c.effective_min_viable_spd
    spd_cap = c.effective_spd_cap

    # Базовый скор от спреда (0..60 баллов)
    abs_spread = abs(c.price_spread)
    base_score = min(60.0, 20.0 + 40.0 * (1.0 - 1.0 / (1.0 + abs_spread / 15.0)))

    # Ликвидность (до +30)
    spd = c.avg_spd

    if spd < min_spd:
        # Штраф: чем ниже спрос — тем сильнее срезаем итоговый скор
        liq_score = -20.0 * (1.0 - spd / min_spd)
    elif spd <= spd_cap:
        # Логарифм: рост быстрый в начале, плавный ближе к cap
        liq_score = 30.0 * math.log(1 + spd) / math.log(1 + spd_cap)
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
    if Modifier.M_TEMPORAL     in c.modifiers:
        raw += 15.0
    if Modifier.M_SUPPLY_SHOCK in c.modifiers:
        raw += 20.0
    if Modifier.M_LADDER       in c.modifiers:
        raw += 10.0

    # Штраф за недостаточную историю
    if c.weeks_covered < _NEW_ITEM_MIN_WEEKS:
        raw *= 0.7   # -30% — совсем мало данных
    elif c.weeks_covered < _MATURE_ITEM_MIN_WEEKS:
        # Частичный штраф для предметов между 2 и 4 неделями
        # (применяется к «новым» предметам, прошедшим смягчённый SQL-фильтр)
        raw *= 0.85  # -15%

    return round(max(0.0, min(100.0, raw)), 1)

# ---------------------------------------------------------------------------
# SQL: сбор данных для кандидатов
# ---------------------------------------------------------------------------

# Примечания к SQL:
#
# 1. volatility > 0.25 убран из HAVING — волатильность теперь учитывается
#    только в Python (_assign_base, _temporal_threshold, _score_candidate).
#
# 2. MIN_VIABLE_SPD и SALES_PER_DAY_CAP в HAVING заменены на per-category
#    пороги из liquidity_baselines (liq_base CTE). Для предметов без записи
#    в liq_base используются глобальные fallback-значения через COALESCE.
#
# 3. weeks_covered >= 4 заменён на адаптивный порог:
#    - Новые предметы (updated_at < 60 дней): достаточно 2 недель.
#    - Зрелые предметы: требуется 4 недели.
#    Условие вынесено в WHERE (после JOIN с items) чтобы использовать
#    i.updated_at.
#
# 4. В SELECT добавлены: i.updated_at, lb.min_viable_spd, lb.spd_cap —
#    для передачи в Candidate и использования в скоринге.

_GET_CANDIDATES_SQL = f"""
WITH
liq_base AS (
    -- Per-category пороги ликвидности из recalculate_liquidity_baselines()
    SELECT category, min_viable_spd, spd_cap
    FROM liquidity_baselines
),
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
    HAVING SUM(CASE WHEN a.low_sample = 0 THEN 1 ELSE 0 END) >= 3
    -- volatility > 0.25 убран: теперь только в Python (_assign_base / _score_candidate)
),
snap AS (
    SELECT * FROM lot_snapshots WHERE updated_at > (strftime('%s', 'now') - 3600)
)
SELECT
    h.item_id,
    COALESCE(i.name_ru, h.item_id) AS name_ru,
    i.category,
    i.attr_type,
    i.updated_at                    AS item_updated_at,
    h.qlt, h.ptn, h.upgrade_level,
    h.weeks_covered,
    h.price_single, h.price_bulk, h.price_spread,
    h.amount_p50, h.amount_mode, h.bulk_share, h.volatility, h.avg_spd,
    s.total_lots, s.bulk_lots, s.min_price_pu,
    -- Per-category пороги (NULL если категория не в liq_base → Python использует fallback)
    lb.min_viable_spd AS cat_min_viable_spd,
    lb.spd_cap        AS cat_spd_cap
FROM hist h
JOIN items i ON i.item_id = h.item_id
LEFT JOIN snap s
       ON s.item_id = h.item_id AND s.qlt = h.qlt AND s.ptn = h.ptn
LEFT JOIN liq_base lb ON lb.category = i.category
WHERE
    i.last_sale_at > (strftime('%s', 'now') - {TWO_WEEKS_SEC})
    AND i.category NOT IN ('bullet')
    AND (
        h.price_spread IS NOT NULL
        OR i.attr_type != 'none'
    )
    AND h.price_single IS NOT NULL AND h.price_bulk IS NOT NULL AND h.price_bulk > 0
    AND h.item_id NOT IN (SELECT item_id FROM ignored_items)
    -- Адаптивный фильтр по avg_spd: per-category через liq_base, fallback — глобальный
    AND h.avg_spd >= COALESCE(lb.min_viable_spd, {MIN_VIABLE_SPD})
    AND h.avg_spd <= COALESCE(lb.spd_cap, {SALES_PER_DAY_CAP})
    -- Адаптивный weeks_covered: новые предметы (< {_NEW_ITEM_AGE_SEC // 86400} дней) — порог 2 нед.,
    -- зрелые — 4 нед.
    AND (
        (
            (strftime('%s', 'now') - i.updated_at) < {_NEW_ITEM_AGE_SEC}
            AND h.weeks_covered >= {_NEW_ITEM_MIN_WEEKS}
        )
        OR h.weeks_covered >= {_MATURE_ITEM_MIN_WEEKS}
    )
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
        spread     = r["price_spread"] or 0.0
        volatility = r["volatility"]   or 0.0

        c = Candidate(
            item_id       = r["item_id"],
            name_ru       = r["name_ru"],
            category      = r["category"],
            weeks_covered = r["weeks_covered"],
            attr_type     = r["attr_type"],
            qlt           = r["qlt"],
            ptn           = r["ptn"],
            upgrade_level = r["upgrade_level"],
            price_spread  = spread,
            avg_spd       = r["avg_spd"] or 0.0,
            volatility    = volatility,
            min_lot_price = r["min_price_pu"],
            # Per-category пороги — могут быть None (→ fallback в property)
            cat_min_viable_spd = r["cat_min_viable_spd"],
            cat_spd_cap        = r["cat_spd_cap"],
            # base_strategy теперь получает volatility для адаптивного порога
            base_strategy = _assign_base(spread, volatility),
        )

        hist_data = {
            "avg_spd":     c.avg_spd,
            "bulk_share":  r["bulk_share"],
            "amount_mode": r["amount_mode"],
            "amount_p50":  r["amount_p50"],
        }
        snap_data = (
            {"total_lots": r["total_lots"], "bulk_lots": r["bulk_lots"]}
            if r["total_lots"] is not None else None
        )

        c.modifiers = _detect_modifiers(c, hist_data, snap_data, windows=None)
        c.score     = _score_candidate(c)

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
    existing: set[tuple] = {
        (r[0], r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT item_id, qlt, ptn, upgrade_level FROM watched_items"
        ).fetchall()
    }

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
