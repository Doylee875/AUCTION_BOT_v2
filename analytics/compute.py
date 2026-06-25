"""
analytics/compute.py
====================
Ядро вычисления метрик из списка строк продаж.

Публичный интерфейс:
    compute_metrics(sales, granularity, bucket_key) → dict
"""

import sqlite3
import statistics
from collections import Counter
from datetime import datetime, timezone

from analytics.bucket import MSK_OFFSET_SEC, parse_bucket_key

import api.utils.logger

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Пороговые константы
# ---------------------------------------------------------------------------

_MIN_RELIABLE_SALES = 5

# Если медиана amount == 1 (розничный рынок), bulk threshold берётся как
# наименьший amount > 1 из данных. Если таких нет — этот дефолт.
_BULK_THRESHOLD_FALLBACK = 10


# ---------------------------------------------------------------------------
# Вспомогательные функции amount-сегментации
# ---------------------------------------------------------------------------

def _weighted_avg(pairs: list[tuple[int, int]]) -> float | None:
    """Взвешенное среднее price_per_unit по (price, amount) парам."""
    total_a = sum(a for _, a in pairs)
    if not total_a:
        return None
    return sum(p * a for p, a in pairs) / total_a


def _coeff_variation(prices: list[int]) -> float | None:
    """Коэффициент вариации (std / mean) по списку цен. None при < 2 точках."""
    if len(prices) < 2:
        return None
    mean_p = statistics.mean(prices)
    return (statistics.stdev(prices) / mean_p) if mean_p else None


def _compute_bulk_threshold(amounts: list[int]) -> int:
    """
    Адаптивный bulk threshold из медианы amount.

    Если медиана == 1 (розничный рынок), берём наименьший amount > 1
    из реальных данных. Если и таких нет — возвращаем _BULK_THRESHOLD_FALLBACK.
    """
    if not amounts:
        return _BULK_THRESHOLD_FALLBACK
    sorted_a = sorted(amounts)
    p50 = sorted_a[len(sorted_a) // 2]
    if p50 > 1:
        return p50
    above_one = [a for a in sorted_a if a > 1]
    return above_one[0] if above_one else _BULK_THRESHOLD_FALLBACK


def _mode(values: list[int]) -> int | None:
    """Мода распределения. При нескольких модах — наименьшее значение."""
    if not values:
        return None
    counts = Counter(values)
    max_count = max(counts.values())
    modes = [v for v, c in counts.items() if c == max_count]
    return min(modes)


# ---------------------------------------------------------------------------
# Публичный интерфейс
# ---------------------------------------------------------------------------

def compute_metrics(
    sales: list[sqlite3.Row],
    granularity: str,
    bucket_key: str,
) -> dict[str, float | None]:
    """
    Вычисляет аналитические метрики из готового списка строк продаж.

    Помимо базовых метрик (avg_price, volatility, trend, liquidity,
    sales_per_day, total_amount) вычисляет amount-сегментацию:

        amount_p50   — медиана amount; используется как адаптивный bulk threshold.
        price_single — взвеш. avg price_per_unit для сделок с amount = 1.
        price_bulk   — взвеш. avg price_per_unit для сделок с amount >= amount_p50.
        bulk_share   — доля объёма (SUM amount) от bulk-сделок в total_amount.
        vol_single   — CV цены только по розничным (amount=1) сделкам.

    NULL в полях сегментации означает отсутствие данных по данному сегменту
    (нет розничных или нет оптовых сделок в срезе) — не ошибку.

    Args:
        sales       : Список sqlite3.Row (поля: price_per_unit, amount, sold_at).
        granularity : Тип временного среза.
        bucket_key  : Ключ среза (для расчёта liquidity в weekly/monthly).

    Returns:
        Словарь со всеми метриками. relative_volume всегда None (заполняется позже).
    """
    _null: dict = {
        "liquidity": None, "sales_per_day": None, "avg_price": None,
        "volatility": None, "trend": None, "total_amount": None,
        "amount_p50": None, "price_single": None, "price_bulk": None,
        "bulk_share": None, "vol_single": None,
        "low_sample": False,
    }
    if not sales:
        return _null

    prices       = [row["price_per_unit"] for row in sales]
    amounts      = [row["amount"]         for row in sales]
    timestamps   = [row["sold_at"]        for row in sales]
    total_amount = sum(amounts)

    # Взвешенное среднее по количеству единиц — честнее невзвешенного,
    # т.к. оптовые сделки иначе занижали бы среднюю цену.
    avg_price = sum(p * a for p, a in zip(prices, amounts)) / total_amount if total_amount else None

    # --- Liquidity ---
    if granularity in ("window_weekday", "daily", "hourly"):
        liquidity = 1.0
    elif granularity in ("weekly", "monthly"):
        parsed = parse_bucket_key(bucket_key, granularity)
        start_date, end_date = parsed
        days_with_sales: set = set()
        for ts in timestamps:
            msk_dt = datetime.fromtimestamp(ts + MSK_OFFSET_SEC, tz=timezone.utc)
            days_with_sales.add(msk_dt.date())
        total_days = 7 if granularity == "weekly" else (end_date - start_date).days + 1
        liquidity = len(days_with_sales) / total_days if total_days > 0 else 0.0
    else:
        liquidity = None

    # --- Sales per day ---
    if granularity == "window_weekday":
        sales_per_day = float(len(sales))
    elif granularity == "daily":
        sales_per_day = float(len(sales))
    elif granularity == "hourly":
        sales_per_day = float(len(sales)) * 24.0
    elif granularity == "weekly":
        sales_per_day = len(sales) / 7.0
    elif granularity == "monthly":
        parsed = parse_bucket_key(bucket_key, granularity)
        start_date, end_date = parsed
        total_days = (end_date - start_date).days + 1
        sales_per_day = len(sales) / total_days if total_days > 0 else 0.0
    else:
        sales_per_day = None

    # --- Volatility ---
    volatility = _coeff_variation(prices)

    # --- Trend (наклон линейной регрессии) ---
    if len(prices) >= 2:
        min_ts  = min(timestamps)
        x_vals  = [ts - min_ts for ts in timestamps]
        y_vals  = prices
        n       = len(x_vals)
        mean_x  = sum(x_vals) / n
        mean_y  = sum(y_vals) / n
        cov_xy  = sum((x_vals[i] - mean_x) * (y_vals[i] - mean_y) for i in range(n)) / n
        var_x   = sum((x - mean_x) ** 2 for x in x_vals) / n
        trend   = cov_xy / var_x if var_x else 0.0
    else:
        trend = None

    # --- Amount-сегментация ---
    bulk_threshold = _compute_bulk_threshold(amounts)

    single_pairs = [(p, a) for p, a in zip(prices, amounts) if a == 1]
    bulk_pairs   = [(p, a) for p, a in zip(prices, amounts) if a >= bulk_threshold]

    price_single = _weighted_avg(single_pairs)
    price_bulk   = _weighted_avg(bulk_pairs)

    bulk_amount  = sum(a for _, a in bulk_pairs)
    bulk_share   = bulk_amount / total_amount if total_amount else None

    vol_single   = _coeff_variation([p for p, _ in single_pairs])

    # --- Арбитражные метрики ---
    if price_single is not None and price_bulk is not None and price_bulk > 0:
        price_spread = (price_single - price_bulk) / price_bulk * 100.0
    else:
        price_spread = None

    amount_mode   = _mode(amounts)
    spread_stable = 0   # заполняется отдельным проходом (auto_select)

    return {
        "liquidity":     liquidity,
        "sales_per_day": sales_per_day,
        "avg_price":     avg_price,
        "volatility":    volatility,
        "trend":         trend,
        "total_amount":  total_amount,
        "amount_p50":    bulk_threshold,
        "price_single":  price_single,
        "price_bulk":    price_bulk,
        "bulk_share":    bulk_share,
        "vol_single":    vol_single,
        "price_spread":  price_spread,
        "amount_mode":   amount_mode,
        "spread_stable": spread_stable,
        "low_sample":    len(sales) < _MIN_RELIABLE_SALES,
    }
