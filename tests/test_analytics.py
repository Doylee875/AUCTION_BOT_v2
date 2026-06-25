"""
tests/test_analytics.py
=======================
Тесты для каждой точки расчёта аналитики в analytics/metrics.py и analytics/bucket.py.

Покрытие:
  - compute_metrics: avg_price, volatility, trend, liquidity, sales_per_day
  - compute_metrics: amount-сегментация (price_single, price_bulk, bulk_share, vol_single)
  - _weighted_avg, _coeff_variation, _compute_bulk_threshold
  - calculate_bucket_key для всех гранулярностей
  - parse_bucket_key для weekly и monthly
  - _is_profitable / _effective_threshold / _resolve_ref_price (fetcher_lots)
  - LotAlert: свойства display_name, fmt_price, expires_dt, slice_label
  - NotifierDispatcher: дедупликация cooldown
"""

import sqlite3
import sys
import types
import unittest
from datetime import datetime, timezone, date
from analytics.metrics import (
    _coeff_variation,
    _compute_bulk_threshold,
    _weighted_avg,
    compute_metrics,
)
from analytics.bucket import (
    calculate_bucket_key,
    get_supported_granularities,
    parse_bucket_key,
)
from notifications.base import LotAlert
import os




# Stub aiohttp so imports don't fail in test environment without the library
_aiohttp_stub = types.ModuleType("aiohttp")
class _FakeClientSession:
    pass
class _FakeClientTimeout:
    def __init__(self, *a, **kw): pass
_aiohttp_stub.ClientSession = _FakeClientSession
_aiohttp_stub.ClientTimeout = _FakeClientTimeout
sys.modules.setdefault("aiohttp", _aiohttp_stub)

# Stub cachetools (used by api/client.py)
_cachetools_stub = types.ModuleType("cachetools")
_cachetools_stub.TTLCache = dict
sys.modules.setdefault("cachetools", _cachetools_stub)

# Stub aiolimiter (used by api/client.py)
_aiolimiter_stub = types.ModuleType("aiolimiter")
class _FakeAsyncLimiter:
    def __init__(self, *a, **kw): pass
_aiolimiter_stub.AsyncLimiter = _FakeAsyncLimiter
sys.modules.setdefault("aiolimiter", _aiolimiter_stub)

# Stub requests (used by api/github_client.py via fetcher_lots chain)
_requests_stub = types.ModuleType("requests")
sys.modules.setdefault("requests", _requests_stub)

# Stub dotenv (used by config.py)
_dotenv_stub = types.ModuleType("dotenv")
_dotenv_stub.load_dotenv = lambda: None
sys.modules.setdefault("dotenv", _dotenv_stub)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(price: int, amount: int, sold_at: int, qlt=None, ptn=None, upgrade_level=None):
    """Создаёт sqlite3.Row-совместимый объект через in-memory БД."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE t (price_per_unit INT, amount INT, sold_at INT, qlt, ptn, upgrade_level)"
    )
    conn.execute(
        "INSERT INTO t VALUES (?,?,?,?,?,?)", (price, amount, sold_at, qlt, ptn, upgrade_level)
    )
    return conn.execute("SELECT * FROM t").fetchone()


def _make_rows(data: list[tuple]) -> list:
    """data: list of (price_per_unit, amount, sold_at)"""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE t (price_per_unit INT, amount INT, sold_at INT, qlt, ptn, upgrade_level)"
    )
    for p, a, ts in data:
        conn.execute("INSERT INTO t VALUES (?,?,?,NULL,NULL,NULL)", (p, a, ts))
    return conn.execute("SELECT * FROM t").fetchall()


# Фиксированный timestamp: 2025-06-18 14:00 UTC → 2025-06-18 17:00 МСК (window 3, weekday)
_TS_WEEKDAY = int(datetime(2025, 6, 18, 14, 0, 0, tzinfo=timezone.utc).timestamp())
# 2025-06-21 10:00 UTC → суббота МСК (weekend, window 2)
_TS_WEEKEND = int(datetime(2025, 6, 21, 10, 0, 0, tzinfo=timezone.utc).timestamp())


# ===========================================================================
# Вспомогательные функции
# ===========================================================================

class TestWeightedAvg(unittest.TestCase):
    def test_single_pair(self):
        self.assertAlmostEqual(_weighted_avg([(100, 5)]), 100.0)

    def test_two_pairs_equal_amount(self):
        # (100, 2) и (200, 2): avg = (100*2 + 200*2) / 4 = 150
        self.assertAlmostEqual(_weighted_avg([(100, 2), (200, 2)]), 150.0)

    def test_two_pairs_unequal_amount(self):
        # (100, 1) и (200, 3): avg = (100 + 600) / 4 = 175
        self.assertAlmostEqual(_weighted_avg([(100, 1), (200, 3)]), 175.0)

    def test_empty_returns_none(self):
        self.assertIsNone(_weighted_avg([]))

    def test_zero_amount_returns_none(self):
        self.assertIsNone(_weighted_avg([(100, 0)]))


class TestCoeffVariation(unittest.TestCase):
    def test_single_price_returns_none(self):
        self.assertIsNone(_coeff_variation([100]))

    def test_empty_returns_none(self):
        self.assertIsNone(_coeff_variation([]))

    def test_identical_prices_zero_cv(self):
        self.assertAlmostEqual(_coeff_variation([100, 100, 100]), 0.0)

    def test_known_cv(self):
        # mean=150, std≈70.7, CV≈0.471
        cv = _coeff_variation([100, 200])
        self.assertGreater(cv, 0.4)
        self.assertLess(cv, 0.6)


class TestComputeBulkThreshold(unittest.TestCase):
    def test_empty_returns_fallback(self):
        from analytics.metrics import _BULK_THRESHOLD_FALLBACK
        self.assertEqual(_compute_bulk_threshold([]), _BULK_THRESHOLD_FALLBACK)

    def test_median_above_one(self):
        # median([1,5,10]) = 5
        self.assertEqual(_compute_bulk_threshold([1, 5, 10]), 5)

    def test_retail_market_smallest_above_one(self):
        # median([1,1,1]) = 1 → берём минимальный >1
        self.assertEqual(_compute_bulk_threshold([1, 1, 1, 3, 7]), 3)

    def test_all_ones_returns_fallback(self):
        from analytics.metrics import _BULK_THRESHOLD_FALLBACK
        self.assertEqual(_compute_bulk_threshold([1, 1, 1]), _BULK_THRESHOLD_FALLBACK)


# ===========================================================================
# compute_metrics
# ===========================================================================

class TestComputeMetricsEmpty(unittest.TestCase):
    def test_empty_sales_returns_nulls(self):
        result = compute_metrics([], "daily", "2025-06-18")
        self.assertIsNone(result["avg_price"])
        self.assertIsNone(result["volatility"])
        self.assertIsNone(result["sales_per_day"])
        self.assertIsNone(result["liquidity"])


class TestComputeMetricsAvgPrice(unittest.TestCase):
    def test_weighted_avg_price(self):
        # (1000, 1) + (2000, 3) → avg = (1000 + 6000) / 4 = 1750
        rows = _make_rows([(1000, 1, _TS_WEEKDAY), (2000, 3, _TS_WEEKDAY + 60)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertAlmostEqual(result["avg_price"], 1750.0)

    def test_total_amount(self):
        rows = _make_rows([(500, 2, _TS_WEEKDAY), (700, 3, _TS_WEEKDAY + 60)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertEqual(result["total_amount"], 5)


class TestComputeMetricsVolatility(unittest.TestCase):
    def test_identical_prices_zero_volatility(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY + i * 60) for i in range(5)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertAlmostEqual(result["volatility"], 0.0)

    def test_single_sale_volatility_none(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertIsNone(result["volatility"])


class TestComputeMetricsTrend(unittest.TestCase):
    def test_rising_prices_positive_trend(self):
        # Цены растут со временем → положительный тренд
        rows = _make_rows([
            (100, 1, _TS_WEEKDAY),
            (200, 1, _TS_WEEKDAY + 3600),
            (300, 1, _TS_WEEKDAY + 7200),
        ])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertGreater(result["trend"], 0)

    def test_falling_prices_negative_trend(self):
        rows = _make_rows([
            (300, 1, _TS_WEEKDAY),
            (200, 1, _TS_WEEKDAY + 3600),
            (100, 1, _TS_WEEKDAY + 7200),
        ])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertLess(result["trend"], 0)

    def test_single_sale_trend_none(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertIsNone(result["trend"])


class TestComputeMetricsLiquidity(unittest.TestCase):
    def test_window_weekday_liquidity_one(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "window_weekday", "3_weekday")
        self.assertEqual(result["liquidity"], 1.0)

    def test_daily_liquidity_one(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertEqual(result["liquidity"], 1.0)

    def test_hourly_liquidity_one(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "hourly", "2025-06-18T17")
        self.assertEqual(result["liquidity"], 1.0)

    def test_weekly_liquidity_one_day_of_seven(self):
        # Только один день продаж за неделю → 1/7
        rows = _make_rows([(1000, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "weekly", "2025-W25")
        self.assertAlmostEqual(result["liquidity"], 1 / 7, places=5)

    def test_weekly_liquidity_full_week(self):
        # Продажи в разные дни недели → liquidity = N_days / 7
        # W25 2025: пн 16 июня – вс 22 июня UTC
        days = [
            int(datetime(2025, 6, 16, 12, 0, tzinfo=timezone.utc).timestamp()),
            int(datetime(2025, 6, 17, 12, 0, tzinfo=timezone.utc).timestamp()),
            int(datetime(2025, 6, 18, 12, 0, tzinfo=timezone.utc).timestamp()),
        ]
        rows = _make_rows([(1000, 1, ts) for ts in days])
        result = compute_metrics(rows, "weekly", "2025-W25")
        # 3 уникальных МСК-дня из 7
        self.assertAlmostEqual(result["liquidity"], 3 / 7, places=5)

    def test_monthly_liquidity(self):
        # Июнь 2025 = 30 дней; 1 день с продажами → 1/30
        rows = _make_rows([(1000, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "monthly", "2025-06")
        self.assertAlmostEqual(result["liquidity"], 1 / 30, places=5)


class TestComputeMetricsSalesPerDay(unittest.TestCase):
    def test_daily(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY + i * 10) for i in range(5)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertAlmostEqual(result["sales_per_day"], 5.0)

    def test_hourly_multiplied_by_24(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY + i * 60) for i in range(3)])
        result = compute_metrics(rows, "hourly", "2025-06-18T17")
        self.assertAlmostEqual(result["sales_per_day"], 3 * 24.0)

    def test_weekly(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY + i * 3600) for i in range(7)])
        result = compute_metrics(rows, "weekly", "2025-W25")
        self.assertAlmostEqual(result["sales_per_day"], 7 / 7.0)

    def test_window_weekday(self):
        rows = _make_rows([(1000, 1, _TS_WEEKDAY + i * 300) for i in range(4)])
        result = compute_metrics(rows, "window_weekday", "3_weekday")
        self.assertAlmostEqual(result["sales_per_day"], 4.0)

    def test_monthly(self):
        # Июнь 2025 = 30 дней
        rows = _make_rows([(1000, 1, _TS_WEEKDAY + i * 3600) for i in range(30)])
        result = compute_metrics(rows, "monthly", "2025-06")
        self.assertAlmostEqual(result["sales_per_day"], 30 / 30.0)


class TestComputeMetricsAmountSegmentation(unittest.TestCase):
    """Тесты для price_single, price_bulk, bulk_share, vol_single."""

    def _mixed_rows(self):
        """Данные: розница (amount=1, price=100) + опт (amount=10, price=80)."""
        return _make_rows([
            (100, 1, _TS_WEEKDAY),
            (100, 1, _TS_WEEKDAY + 60),
            (80,  10, _TS_WEEKDAY + 120),
        ])

    def test_price_single_retail_avg(self):
        rows = self._mixed_rows()
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertAlmostEqual(result["price_single"], 100.0)

    def test_price_bulk_wholesale_avg(self):
        rows = self._mixed_rows()
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertAlmostEqual(result["price_bulk"], 80.0)

    def test_bulk_share_calculation(self):
        rows = self._mixed_rows()
        result = compute_metrics(rows, "daily", "2025-06-18")
        # total_amount = 1+1+10 = 12; bulk_amount = 10 → bulk_share ≈ 0.833
        self.assertAlmostEqual(result["bulk_share"], 10 / 12, places=5)

    def test_no_bulk_sales(self):
        rows = _make_rows([(100, 1, _TS_WEEKDAY + i * 60) for i in range(5)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        # Все amount=1, bulk_threshold=FALLBACK=10, нет bulk-сделок
        self.assertIsNone(result["price_bulk"])

    def test_vol_single_none_for_single_retail_price(self):
        # Единственная розничная сделка → CV = None
        rows = _make_rows([(100, 1, _TS_WEEKDAY)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertIsNone(result["vol_single"])

    def test_vol_single_zero_for_identical_retail_prices(self):
        rows = _make_rows([
            (100, 1, _TS_WEEKDAY),
            (100, 1, _TS_WEEKDAY + 60),
        ])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertAlmostEqual(result["vol_single"], 0.0)

    def test_amount_p50_bulk_threshold(self):
        rows = _make_rows([
            (100, 1, _TS_WEEKDAY),
            (90,  5, _TS_WEEKDAY + 60),
            (85,  10, _TS_WEEKDAY + 120),
        ])
        result = compute_metrics(rows, "daily", "2025-06-18")
        # median([1,5,10]) = 5 → bulk_threshold = 5
        self.assertEqual(result["amount_p50"], 5)

    def test_low_sample_flag(self):
        from analytics.metrics import _MIN_RELIABLE_SALES
        rows = _make_rows([(100, 1, _TS_WEEKDAY + i * 60) for i in range(_MIN_RELIABLE_SALES - 1)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertTrue(result["low_sample"])

    def test_not_low_sample(self):
        from analytics.metrics import _MIN_RELIABLE_SALES
        rows = _make_rows([(100, 1, _TS_WEEKDAY + i * 60) for i in range(_MIN_RELIABLE_SALES)])
        result = compute_metrics(rows, "daily", "2025-06-18")
        self.assertFalse(result["low_sample"])


# ===========================================================================
# calculate_bucket_key
# ===========================================================================

class TestCalculateBucketKey(unittest.TestCase):
    def test_daily(self):
        # _TS_WEEKDAY = 2025-06-18 14:00 UTC → МСК = 17:00 → дата 2025-06-18
        bk = calculate_bucket_key(_TS_WEEKDAY, "daily")
        self.assertEqual(bk, "2025-06-18")

    def test_monthly(self):
        bk = calculate_bucket_key(_TS_WEEKDAY, "monthly")
        self.assertEqual(bk, "2025-06")

    def test_weekly(self):
        # 2025-06-18 → ISO W25
        bk = calculate_bucket_key(_TS_WEEKDAY, "weekly")
        self.assertEqual(bk, "2025-W25")

    def test_hourly(self):
        # 14:00 UTC → 17:00 МСК → "2025-06-18T17"
        bk = calculate_bucket_key(_TS_WEEKDAY, "hourly")
        self.assertEqual(bk, "2025-06-18T17")

    def test_window_weekday_weekday(self):
        # 14:00 UTC = 17:00 МСК; (17*3600 - 2*3600) // (4*3600) = 15*3600//14400 = 3
        bk = calculate_bucket_key(_TS_WEEKDAY, "window_weekday")
        self.assertEqual(bk, "3_weekday")

    def test_window_weekday_weekend(self):
        bk = calculate_bucket_key(_TS_WEEKEND, "window_weekday")
        self.assertIn("weekend", bk)

    def test_unsupported_granularity_raises(self):
        with self.assertRaises(ValueError):
            calculate_bucket_key(_TS_WEEKDAY, "decade")

    def test_get_supported_granularities_complete(self):
        grans = get_supported_granularities()
        for g in ("window_weekday", "daily", "weekly", "monthly", "hourly"):
            self.assertIn(g, grans)


# ===========================================================================
# parse_bucket_key
# ===========================================================================

class TestParseBucketKey(unittest.TestCase):
    def test_weekly_start_end(self):
        start, end = parse_bucket_key("2025-W25", "weekly")
        self.assertEqual(start, date(2025, 6, 16))  # понедельник W25
        self.assertEqual(end,   date(2025, 6, 22))  # воскресенье W25

    def test_monthly_start_end(self):
        start, end = parse_bucket_key("2025-06", "monthly")
        self.assertEqual(start, date(2025, 6, 1))
        self.assertEqual(end,   date(2025, 6, 30))

    def test_monthly_february_leap(self):
        start, end = parse_bucket_key("2024-02", "monthly")
        self.assertEqual(end, date(2024, 2, 29))

    def test_daily(self):
        start, end = parse_bucket_key("2025-06-18", "daily")
        self.assertEqual(start, end)
        self.assertEqual(start, date(2025, 6, 18))

    def test_unsupported_raises(self):
        with self.assertRaises(ValueError):
            parse_bucket_key("2025-06", "decade")


# ===========================================================================
# LotAlert properties
# ===========================================================================

class TestLotAlertProperties(unittest.TestCase):
    def _alert(self, **kw) -> LotAlert:
        defaults = dict(
            item_id="ITEM_001", name_ru="Тёмный страж",
            lot_price=120_000, amount=1,
            avg_price=147_000, discount_pct=18.4,
        )
        defaults.update(kw)
        return LotAlert(**defaults)

    def test_display_name_no_slice(self):
        a = self._alert()
        self.assertEqual(a.display_name, "Тёмный страж")

    def test_display_name_with_qlt_ptn(self):
        a = self._alert(qlt=3, ptn=13)
        self.assertIn("Редкий", a.display_name)
        self.assertIn("+13", a.display_name)

    def test_display_name_with_upgrade_level(self):
        a = self._alert(upgrade_level=5)
        self.assertIn("ул.5", a.display_name)

    def test_lot_price_fmt_thousands(self):
        a = self._alert(lot_price=120_000)
        self.assertEqual(a.lot_price_fmt, "120K")

    def test_lot_price_fmt_millions(self):
        a = self._alert(lot_price=2_500_000)
        self.assertEqual(a.lot_price_fmt, "2.50M")

    def test_lot_price_fmt_small(self):
        a = self._alert(lot_price=500)
        self.assertEqual(a.lot_price_fmt, "500")

    def test_discount_str(self):
        a = self._alert(discount_pct=18.4)
        self.assertEqual(a.discount_str, "18.4%")

    def test_expires_dt_none(self):
        a = self._alert(expires_at=None)
        self.assertIsNone(a.expires_dt)

    def test_expires_dt_utc(self):
        ts = int(datetime(2026, 6, 22, 18, 30, tzinfo=timezone.utc).timestamp())
        a = self._alert(expires_at=ts)
        self.assertEqual(a.expires_dt.year, 2026)
        self.assertEqual(a.expires_dt.hour, 18)

    def test_ref_price_fmt(self):
        a = self._alert(avg_price=147_000, price_label="розн.")
        self.assertIn("147K", a.ref_price_fmt)
        self.assertIn("розн.", a.ref_price_fmt)

    def test_slice_label_artifact(self):
        a = self._alert(qlt=3, ptn=13)
        lbl = a.slice_label
        self.assertIn("Редкий", lbl)
        self.assertIn("+13", lbl)

    def test_slice_label_empty_sentinels(self):
        a = self._alert(qlt=-1, ptn=-1, upgrade_level=-1)
        self.assertEqual(a.slice_label, "")


# ===========================================================================
# Fetcher lots: _is_profitable / _effective_threshold / _resolve_ref_price
# ===========================================================================

class TestFetcherLotsLogic(unittest.TestCase):
    """Тесты для точек расчёта в api/fetcher_lots.py."""

    def setUp(self):
        from api.fetcher_lots import LotRaw, _effective_threshold, _is_profitable, _resolve_ref_price
        self.LotRaw = LotRaw
        self._effective_threshold = _effective_threshold
        self._is_profitable = _is_profitable
        self._resolve_ref_price = _resolve_ref_price

    def _metrics(self, avg_price=100_000, volatility=0.1, price_single=None,
                 price_bulk=None, amount_p50=None, bulk_share=None,
                 vol_single=None, liquidity=0.9, sales_per_day=3.0):
        return {
            "avg_price": avg_price,
            "volatility": volatility,
            "price_single": price_single,
            "price_bulk": price_bulk,
            "amount_p50": amount_p50,
            "bulk_share": bulk_share,
            "vol_single": vol_single,
            "liquidity": liquidity,
            "sales_per_day": sales_per_day,
        }

    def _lot(self, price, amount=1):
        lot = self.LotRaw(price=price, amount=amount)
        return lot

    # --- _effective_threshold ---

    def test_threshold_none_volatility_returns_default(self):
        from config import settings
        thr = self._effective_threshold(None)
        self.assertAlmostEqual(thr, settings.lots_discount_threshold)

    def test_threshold_low_volatility_returns_default(self):
        from config import settings
        thr = self._effective_threshold(0.01)
        self.assertAlmostEqual(thr, settings.lots_discount_threshold)

    def test_threshold_high_volatility_increases(self):
        from config import settings
        # Высокая волатильность → порог растёт
        thr_high = self._effective_threshold(1.0)
        self.assertGreater(thr_high, settings.lots_discount_threshold)

    def test_threshold_capped_at_2x(self):
        from config import settings
        # Очень высокая волатильность → max 2× порог
        thr = self._effective_threshold(999.0)
        self.assertAlmostEqual(thr, settings.lots_discount_threshold * 2.0)

    # --- _resolve_ref_price ---

    def test_resolve_retail_lot_uses_price_single(self):
        metrics = self._metrics(price_single=90_000, amount_p50=10)
        lot = self._lot(85_000, amount=1)
        ref, vol, label = self._resolve_ref_price(lot, metrics)
        self.assertEqual(ref, 90_000)
        self.assertEqual(label, "розн.")

    def test_resolve_bulk_lot_uses_price_bulk(self):
        metrics = self._metrics(price_bulk=70_000, amount_p50=10)
        lot = self._lot(65_000, amount=10)
        ref, vol, label = self._resolve_ref_price(lot, metrics)
        self.assertEqual(ref, 70_000)
        self.assertEqual(label, "опт.")

    def test_resolve_fallback_to_avg_price(self):
        # Нет price_single и price_bulk → fallback
        metrics = self._metrics(price_single=None, price_bulk=None, amount_p50=None)
        lot = self._lot(80_000, amount=1)
        ref, vol, label = self._resolve_ref_price(lot, metrics)
        self.assertEqual(ref, 100_000)
        self.assertEqual(label, "ср.")

    # --- _is_profitable ---

    def test_profitable_lot_below_threshold(self):
        # avg=100k, threshold=0.15 → cutoff=85k; lot price_per_unit=80k → profitable
        metrics = self._metrics(avg_price=100_000, volatility=0.1)
        lot = self._lot(80_000, amount=1)
        is_p, disc, label = self._is_profitable(lot, metrics)
        self.assertTrue(is_p)
        self.assertGreater(disc, 0)

    def test_not_profitable_lot_above_cutoff(self):
        metrics = self._metrics(avg_price=100_000, volatility=0.1)
        lot = self._lot(98_000, amount=1)
        is_p, disc, label = self._is_profitable(lot, metrics)
        self.assertFalse(is_p)
        self.assertAlmostEqual(disc, 0.0)

    def test_no_avg_price_returns_false(self):
        metrics = self._metrics(avg_price=None)
        lot = self._lot(50_000)
        is_p, disc, label = self._is_profitable(lot, metrics)
        self.assertFalse(is_p)

    def test_discount_pct_calculation(self):
        # avg=100k, lot=80k → discount = (100k-80k)/100k * 100 = 20%
        metrics = self._metrics(avg_price=100_000, volatility=None)
        lot = self._lot(80_000, amount=1)
        is_p, disc, label = self._is_profitable(lot, metrics)
        if is_p:
            self.assertAlmostEqual(disc, 20.0, places=1)

    def test_price_per_unit_uses_amount(self):
        # lot.price=200k, amount=2 → price_per_unit=100k → не выгодно (равно avg)
        metrics = self._metrics(avg_price=100_000, volatility=None)
        lot = self.LotRaw(price=200_000, amount=2)
        is_p, _, _ = self._is_profitable(lot, metrics)
        self.assertFalse(is_p)


# ===========================================================================
# NotifierDispatcher: дедупликация
# ===========================================================================

class TestNotifierDispatcher(unittest.TestCase):
    def _make_alert(self, item_id="ITEM_1", qlt=-1, ptn=-1, ul=-1) -> LotAlert:
        return LotAlert(
            item_id=item_id, name_ru="Тест",
            lot_price=100, amount=1,
            avg_price=200, discount_pct=50.0,
            qlt=qlt, ptn=ptn, upgrade_level=ul,
        )

    def test_first_send_not_duplicate(self):
        from notifications.dispatcher import NotifierDispatcher
        d = NotifierDispatcher([], cooldown_sec=600)
        alert = self._make_alert()
        self.assertFalse(d.is_duplicate(alert))

    def test_after_send_is_duplicate(self):
        import time
        from notifications.dispatcher import NotifierDispatcher, _alert_key

        d = NotifierDispatcher([], cooldown_sec=600)
        alert = self._make_alert()
        # Записываем время как это делает send() после успешного gather
        d._last_sent[_alert_key(alert)] = time.time()
        self.assertTrue(d.is_duplicate(alert))

    def test_different_slice_not_duplicate(self):
        import time
        from notifications.dispatcher import NotifierDispatcher, _alert_key

        d = NotifierDispatcher([], cooldown_sec=600)
        alert1 = self._make_alert(qlt=3, ptn=13)
        alert2 = self._make_alert(qlt=3, ptn=14)
        d._last_sent[_alert_key(alert1)] = time.time()
        self.assertFalse(d.is_duplicate(alert2))

    def test_clear_cooldowns_resets_state(self):
        import time
        from notifications.dispatcher import NotifierDispatcher, _alert_key

        d = NotifierDispatcher([], cooldown_sec=600)
        alert = self._make_alert()
        d._last_sent[_alert_key(alert)] = time.time()
        d.clear_cooldowns()
        self.assertFalse(d.is_duplicate(alert))

    def test_expired_cooldown_not_duplicate(self):
        import time
        from notifications.dispatcher import NotifierDispatcher, _alert_key

        d = NotifierDispatcher([], cooldown_sec=1)
        alert = self._make_alert()
        # Записываем время 10 секунд назад — cooldown=1с → уже истёк
        d._last_sent[_alert_key(alert)] = time.time() - 10
        self.assertFalse(d.is_duplicate(alert))


# ===========================================================================
# TelegramNotifier: _format
# ===========================================================================

class TestTelegramNotifierFormat(unittest.TestCase):
    def _alert(self, **kw) -> LotAlert:
        defaults = dict(
            item_id="ITEM_001", name_ru="Тёмный страж",
            lot_price=120_000, amount=1,
            avg_price=147_000, discount_pct=18.4,
            price_label="розн.",
        )
        defaults.update(kw)
        return LotAlert(**defaults)

    def setUp(self):
        from notifications.telegram import TelegramNotifier
        self.notifier = TelegramNotifier("stub_token", "stub_chat")

    def test_format_contains_name(self):
        msg = self.notifier._format(self._alert())
        self.assertIn("Тёмный страж", msg)

    def test_format_contains_price(self):
        msg = self.notifier._format(self._alert())
        self.assertIn("120K", msg)

    def test_format_contains_discount(self):
        msg = self.notifier._format(self._alert())
        # MarkdownV2 escapes '.', so "18.4%" becomes "18\.4%"
        self.assertTrue("18" in msg and "4%" in msg)

    def test_format_contains_expires(self):
        ts = int(datetime(2026, 6, 22, 18, 30, tzinfo=timezone.utc).timestamp())
        msg = self.notifier._format(self._alert(expires_at=ts))
        self.assertIn("Истекает", msg)

    def test_format_bulk_line_shown_above_threshold(self):
        msg = self.notifier._format(self._alert(bulk_share=0.7))
        self.assertIn("Оптовый рынок", msg)

    def test_format_bulk_line_hidden_below_threshold(self):
        msg = self.notifier._format(self._alert(bulk_share=0.3))
        self.assertNotIn("Оптовый рынок", msg)


# ===========================================================================
# DiscordNotifier: _build_payload
# ===========================================================================

class TestDiscordNotifierPayload(unittest.TestCase):
    def _alert(self, **kw) -> LotAlert:
        defaults = dict(
            item_id="ITEM_001", name_ru="Тёмный страж",
            lot_price=120_000, amount=1,
            avg_price=147_000, discount_pct=18.4,
            price_label="розн.", color="rare",
        )
        defaults.update(kw)
        return LotAlert(**defaults)

    def setUp(self):
        from notifications.discord import DiscordNotifier
        self.notifier = DiscordNotifier("https://discord.com/api/webhooks/stub")

    def test_payload_has_embeds(self):
        payload = self.notifier._build_payload(self._alert())
        self.assertIn("embeds", payload)
        self.assertEqual(len(payload["embeds"]), 1)

    def test_embed_color_rare(self):
        payload = self.notifier._build_payload(self._alert(color="rare"))
        self.assertEqual(payload["embeds"][0]["color"], 0x5BC0DE)

    def test_embed_color_unknown_default(self):
        payload = self.notifier._build_payload(self._alert(color="unknown_rarity"))
        self.assertEqual(payload["embeds"][0]["color"], 0xAAAAAA)

    def test_embed_title_contains_name(self):
        payload = self.notifier._build_payload(self._alert())
        self.assertIn("Тёмный страж", payload["embeds"][0]["title"])

    def test_embed_fields_contain_price(self):
        payload = self.notifier._build_payload(self._alert())
        field_names = [f["name"] for f in payload["embeds"][0]["fields"]]
        self.assertTrue(any("Цена" in n for n in field_names))

    def test_seller_field_present(self):
        payload = self.notifier._build_payload(self._alert(seller="PlayerOne"))
        field_names = [f["name"] for f in payload["embeds"][0]["fields"]]
        self.assertTrue(any("Продавец" in n for n in field_names))

    def test_seller_field_absent_when_empty(self):
        payload = self.notifier._build_payload(self._alert(seller=""))
        field_names = [f["name"] for f in payload["embeds"][0]["fields"]]
        self.assertFalse(any("Продавец" in n for n in field_names))

    def test_bulk_field_shown_above_threshold(self):
        payload = self.notifier._build_payload(self._alert(bulk_share=0.6))
        field_names = [f["name"] for f in payload["embeds"][0]["fields"]]
        self.assertTrue(any("Опт" in n for n in field_names))

    def test_footer_contains_realm(self):
        payload = self.notifier._build_payload(self._alert(realm="EU"))
        footer = payload["embeds"][0]["footer"]["text"]
        self.assertIn("EU", footer)


if __name__ == "__main__":
    unittest.main(verbosity=2)
