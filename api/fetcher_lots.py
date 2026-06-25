"""
api/fetcher_lots.py
===================
Опрос активных лотов аукциона и отправка уведомлений о выгодных предложениях.

Архитектура:
  - Бесконечный цикл с интервалом LOTS_POLL_INTERVAL секунд
  - Один проход = запрос лотов для всех watched_items параллельно (через пул)
  - Каждый лот сравнивается с историческими метриками из analytics_summary
  - Выгодный лот → немедленная отправка в NotifierDispatcher (TG + Discord)

Логика «выгодности»:
  lot_price < avg_price × (1 − LOTS_DISCOUNT_THRESHOLD)

  Порог адаптируется к волатильности предмета:
    если volatility > VOLATILITY_SCALE_THRESHOLD — порог смягчается,
    чтобы не генерировать ложные срабатывания на нестабильных предметах.

Фильтрация лотов по атрибутам:
  WatchFilter.matches_lot() проверяет совпадение qlt/ptn/upgrade_level.
  Sentinel -1 в фильтре = «любое значение».

Эндпоинт API:
  GET /{region}/auction/{item_id}/lots
  Ожидаемый ответ: { "total": N, "items": [ { "price", "amount", ... } ] }
  (структура аналогична /history — уточнить по документации STALCRAFT API)
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import api.utils.logger
from api.pool import StalcraftClientPool, build_pool
from config import settings
from db.connection import get_connection, open_connection
from notifications.base import LotAlert
from notifications.dispatcher import NotifierDispatcher, build_dispatcher
from schema import ATTR_SENTINEL
from watched_items import WatchFilter, load_watched_filters

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

LOTS_POLL_INTERVAL       : int   = settings.lots_poll_interval
LOTS_DISCOUNT_THRESHOLD  : float = settings.lots_discount_threshold
LOTS_ALERT_COOLDOWN_SEC  : int   = settings.lots_alert_cooldown_sec
LOTS_VOLATILITY_SCALE_THR: float = settings.lots_volatility_scale_thr

# Максимум одновременных HTTP-запросов на один клиент пула.
# Итоговый семафор = len(pool.clients) * LOTS_CONCURRENT_PER_CLIENT.
# Значение 4 — разумный баланс между скоростью обхода и нагрузкой на API;
# увеличивай только если API явно поддерживает более высокий rate-limit.
LOTS_CONCURRENT_PER_CLIENT: int = 4

LOTS_ENDPOINT = "/{region}/auction/{item_id}/lots"


# ---------------------------------------------------------------------------
# Вспомогательные типы
# ---------------------------------------------------------------------------

@dataclass
class LotRaw:
    """Сырые данные одного активного лота от API."""
    price:      float
    amount:     int
    seller:     str        = ""
    expires_at: int | None = None
    # Атрибуты среза (None если не применимо)
    qlt:           int | None = None
    ptn:           int | None = None
    upgrade_level: int | None = None

    @property
    def price_per_unit(self) -> float:
        return self.price / max(self.amount, 1)


# ---------------------------------------------------------------------------
# Парсинг ответа API
# ---------------------------------------------------------------------------

def _parse_lots(data: dict[str, Any]) -> list[LotRaw]:
    """
    Парсит ответ эндпоинта /lots в список LotRaw.

    Структура ответа уточняется по документации STALCRAFT API.
    Текущая реализация ожидает формат аналогичный /history:
      {
        "total": N,
        "items": [
          {
            "amount": 1,
            "price": 1500000,
            "time": "2026-06-16T12:17:33Z",
            "additional": {
              "bonus_properties": [
                "HEALTH_BONUS",
                "BLEEDING_PROTECTION"
              ],
              "it_transf_count": 8,
              "qlt": 2,
              "ptn": 13,
              "upgrade_bonus": 0.022799999,
              "spawn_time": 1760628913325
            }
          },
          ...
        ]
      }

    При изменении структуры API — адаптировать только эту функцию.
    """
    lots = []
    for raw in data.get("items", []):
        # Атрибуты
        qlt = ptn = ul = None
        for attr in raw.get("additional", []):
            key = attr.get("key", "")
            val = attr.get("value")
            try:
                if key == "ItemQuality":
                    qlt = int(val)
                elif key == "ItemPotential":
                    ptn = int(val)
                elif key == "ItemUpgradeLevel":
                    ul  = int(val)
            except (TypeError, ValueError):
                pass

        # Время истечения
        expires_at = None
        time_str = raw.get("time") or raw.get("expires_at")
        if time_str:
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                expires_at = int(dt.timestamp())
            except (ValueError, AttributeError):
                pass

        try:
            lots.append(LotRaw(
                price      = float(raw["price"]),
                amount     = int(raw.get("amount", 1)),
                seller     = raw.get("seller", ""),
                expires_at = expires_at,
                qlt        = qlt,
                ptn        = ptn,
                upgrade_level = ul,
            ))
        except (KeyError, TypeError, ValueError) as exc:
            log.debug("Пропуск лота с ошибкой парсинга: %s | %s", exc, raw)

    return lots


# ---------------------------------------------------------------------------
# Загрузка метрик из БД
# ---------------------------------------------------------------------------

def _load_metrics(
    conn: sqlite3.Connection,
    item_id: str,
    qlt: int,
    ptn: int,
    upgrade_level: int,
    granularity: str = "weekly",
) -> dict[str, float | None]:
    """
    Загружает метрики из analytics_summary для заданного среза.

    Возвращает только строки с low_sample=0 (надёжная аналитика).
    Если точный срез не найден — пробует агрегат (ptn=-1 для артефактов),
    затем полный sentinel-агрегат.

    Новые поля amount-сегментации (price_single, price_bulk, bulk_share,
    vol_single, amount_p50) могут быть None — fetcher обрабатывает это
    через fallback на avg_price / volatility.
    """
    conn.row_factory = sqlite3.Row

    def _query(q: int, p: int, ul: int) -> dict | None:
        row = conn.execute(
            """
            SELECT AVG(avg_price)    AS avg_price,
                   AVG(volatility)   AS volatility,
                   AVG(liquidity)    AS liquidity,
                   AVG(sales_per_day) AS sales_per_day,
                   AVG(price_single) AS price_single,
                   AVG(price_bulk)   AS price_bulk,
                   AVG(bulk_share)   AS bulk_share,
                   AVG(vol_single)   AS vol_single,
                   AVG(amount_p50)   AS amount_p50,
                   MAX(low_sample)   AS low_sample
            FROM analytics_summary
            WHERE item_id    = ?
              AND granularity = ?
              AND qlt         = ?
              AND ptn         = ?
              AND upgrade_level = ?
              AND low_sample  = 0
            """,
            (item_id, granularity, q, p, ul),
        ).fetchone()
        if row and row["avg_price"] is not None:
            return dict(row)
        return None

    result = _query(qlt, ptn, upgrade_level)
    if result:
        return result

    if ptn != ATTR_SENTINEL:
        result = _query(qlt, ATTR_SENTINEL, upgrade_level)
        if result:
            return result

    result = _query(ATTR_SENTINEL, ATTR_SENTINEL, ATTR_SENTINEL)
    return result or {
        "avg_price": None, "volatility": None,
        "liquidity": None, "sales_per_day": None,
        "price_single": None, "price_bulk": None,
        "bulk_share": None, "vol_single": None,
        "amount_p50": None,
    }


# ---------------------------------------------------------------------------
# Логика оценки выгодности
# ---------------------------------------------------------------------------

def _effective_threshold(volatility: float | None) -> float:
    """
    Адаптивный порог скидки на основе волатильности.

    Высокая волатильность → порог смягчается (требуем бо́льшую скидку),
    чтобы не срабатывать на случайных колебаниях нестабильных предметов.
    """
    if volatility is None:
        return LOTS_DISCOUNT_THRESHOLD
    if volatility > LOTS_VOLATILITY_SCALE_THR:
        scale = min(volatility / LOTS_VOLATILITY_SCALE_THR, 2.0)
        return LOTS_DISCOUNT_THRESHOLD * scale
    return LOTS_DISCOUNT_THRESHOLD


def _resolve_ref_price(
    lot: "LotRaw",
    metrics: dict[str, float | None],
) -> tuple[float | None, float | None, str]:
    """
    Выбирает эталонную цену и волатильность для лота с учётом его amount.

    Логика:
      - amount == 1  → price_single + vol_single (розничный эталон)
      - amount >= p50 → price_bulk   + volatility (оптовый эталон)
      - иначе        → avg_price    + volatility  (fallback)

    Возвращает (ref_price, ref_volatility, label) где label —
    читаемое обозначение сегмента для уведомления.
    """
    amount    = lot.amount
    p50       = metrics.get("amount_p50")
    bulk_thr  = int(p50) if p50 is not None else None

    if amount == 1:
        ref   = metrics.get("price_single")
        vol   = metrics.get("vol_single")
        label = "розн."
        if ref is not None:
            return ref, vol, label

    if bulk_thr is not None and amount >= bulk_thr:
        ref   = metrics.get("price_bulk")
        vol   = metrics.get("volatility")
        label = "опт."
        if ref is not None:
            return ref, vol, label

    # Fallback — смешанная средняя
    return metrics.get("avg_price"), metrics.get("volatility"), "ср."


def _is_profitable(
    lot: "LotRaw",
    metrics: dict[str, float | None],
) -> tuple[bool, float, str]:
    """
    Проверяет, выгоден ли лот относительно правильного ценового сегмента.

    Returns:
        (is_profitable, discount_pct, price_label)
        discount_pct  — насколько лот дешевле эталона (%)
        price_label   — «розн.» / «опт.» / «ср.» — какой эталон использован
    """
    ref_price, ref_vol, label = _resolve_ref_price(lot, metrics)

    if ref_price is None or ref_price <= 0:
        return False, 0.0, label

    threshold = _effective_threshold(ref_vol)
    cutoff    = ref_price * (1.0 - threshold)

    if lot.price_per_unit < cutoff:
        discount_pct = (ref_price - lot.price_per_unit) / ref_price * 100.0
        return True, discount_pct, label

    return False, 0.0, label


# ---------------------------------------------------------------------------
# Один предмет: запрос лотов + анализ
# ---------------------------------------------------------------------------

async def _process_item(
    pool:       StalcraftClientPool,
    conn:       sqlite3.Connection,
    wf:         WatchFilter,
    dispatcher: NotifierDispatcher,
    realm:      str,
    worker_id:  int = 0,
    semaphore:  asyncio.Semaphore | None = None,
) -> int:
    """
    Запрашивает активные лоты для одного WatchFilter, сравнивает с аналитикой,
    отправляет алерты для выгодных.

    Args:
        worker_id:  Индекс задачи в текущем проходе. Передаётся в
                    pool.client_for_worker(), чтобы нагрузка равномерно
                    распределялась по всем клиентам пула, а не падала
                    только на clients[0].
        semaphore:  Ограничивает количество одновременно активных HTTP-запросов.
                    Без него asyncio.gather запускает все len(watched_items)
                    запросов разом — нет back-pressure на API.

    Returns:
        Количество отправленных алертов.
    """
    client   = pool.client_for_worker(worker_id)
    endpoint = LOTS_ENDPOINT.format(region=realm.upper(), item_id=wf.item_id)

    _sem = semaphore or asyncio.Semaphore(1)   # fallback: по одному
    async with _sem:
        try:
            data = await client.get(endpoint)
        except Exception as exc:
            log.warning("Ошибка запроса лотов %s: %s", wf.item_id, exc)
            return 0

    lots = _parse_lots(data)
    if not lots:
        log.debug("%s: лотов нет", wf.item_id)
        return 0

    # Данные предмета из БД
    conn.row_factory = sqlite3.Row
    item_row = conn.execute(
        "SELECT name_ru, name_en, color, attr_type FROM items WHERE item_id=?",
        (wf.item_id,),
    ).fetchone()
    name_ru  = item_row["name_ru"]  if item_row else wf.item_id
    name_en  = item_row["name_en"]  if item_row else ""
    color    = item_row["color"]    if item_row else ""

    alerts_sent = 0
    for lot in lots:
        # Фильтр по атрибутам
        if not wf.matches_lot(lot.qlt, lot.ptn, lot.upgrade_level):
            continue

        # Сентинел-нормализация для запроса метрик
        q  = lot.qlt           if lot.qlt           is not None else ATTR_SENTINEL
        p  = lot.ptn           if lot.ptn           is not None else ATTR_SENTINEL
        ul = lot.upgrade_level if lot.upgrade_level is not None else ATTR_SENTINEL

        metrics    = _load_metrics(conn, wf.item_id, q, p, ul)
        profitable, discount_pct, price_label = _is_profitable(lot, metrics)

        if not profitable:
            continue

        # ref_price для уведомления: тот же сегмент что выбрал _is_profitable
        ref_price, _, _ = _resolve_ref_price(lot, metrics)

        alert = LotAlert(
            item_id       = wf.item_id,
            name_ru       = name_ru or wf.item_id,
            name_en       = name_en or "",
            color         = color   or "",
            qlt           = q,
            ptn           = p,
            upgrade_level = ul,
            lot_price     = lot.price_per_unit,
            amount        = lot.amount,
            seller        = lot.seller,
            expires_at    = lot.expires_at,
            avg_price     = ref_price,
            volatility    = metrics.get("volatility"),
            liquidity     = metrics.get("liquidity"),
            sales_per_day = metrics.get("sales_per_day"),
            discount_pct  = discount_pct,
            price_label   = price_label,
            bulk_share    = metrics.get("bulk_share"),
        )

        await dispatcher.send(alert)
        alerts_sent += 1

    return alerts_sent


# ---------------------------------------------------------------------------
# Один проход по всем watched_items
# ---------------------------------------------------------------------------

async def poll_lots_once(
    pool:       StalcraftClientPool,
    conn:       sqlite3.Connection,
    dispatcher: NotifierDispatcher,
    realm:      str,
) -> None:
    """
    Один полный проход: загружает watched_filters, опрашивает лоты для каждого,
    отправляет алерты для выгодных.

    Конкурентность ограничена семафором: не более
    ``len(pool.clients) * LOTS_CONCURRENT_PER_CLIENT`` одновременных HTTP-запросов.
    Каждая задача получает свой worker_id, по которому pool.client_for_worker()
    выбирает клиента — нагрузка распределяется по всем клиентам пула.
    """
    filters = load_watched_filters(conn)
    if not filters:
        log.debug("watched_items пуст, пропускаем опрос лотов.")
        return

    log.info("Опрос лотов: %d предметов для проверки.", len(filters))

    # Семафор = количество слотов параллельных запросов.
    # LOTS_CONCURRENT_PER_CLIENT запросов на каждый клиент пула — разумный
    # back-pressure: не перегружаем API, не простаиваем.
    concurrency = len(pool.clients) * LOTS_CONCURRENT_PER_CLIENT
    semaphore   = asyncio.Semaphore(concurrency)

    tasks = [
        _process_item(pool, conn, wf, dispatcher, realm,
                      worker_id=i, semaphore=semaphore)
        for i, wf in enumerate(filters)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_alerts = 0
    errors       = 0
    for res in results:
        if isinstance(res, Exception):
            log.error("Ошибка при обработке предмета: %s", res)
            errors += 1
        else:
            total_alerts += res

    log.info(
        "Опрос завершён: %d алертов отправлено, %d ошибок.",
        total_alerts, errors,
    )


# ---------------------------------------------------------------------------
# Бесконечный цикл
# ---------------------------------------------------------------------------

async def run_lots_watcher(
    pool:       StalcraftClientPool | None = None,
    conn:       sqlite3.Connection  | None = None,
    dispatcher: NotifierDispatcher  | None = None,
    realm:      str | None                 = None,
    db_lock:    asyncio.Lock        | None = None,
) -> None:
    """
    Бесконечный цикл опроса активных лотов.

    Вызывается из main.py через asyncio.gather вместе с остальными задачами.
    Все аргументы опциональны — при None создаются из настроек.

    Args:
        pool:       Пул API-клиентов (переиспользуется из fetcher_anal).
        conn:       Соединение с БД. При совместном запуске с setup_analysis
                    ОБЯЗАТЕЛЬНО передавать то же соединение, что и в
                    setup_analysis, иначе два объекта Connection конкурируют
                    на WAL-чекпоинте и могут получить OperationalError:
                    database is locked.
        dispatcher: Диспетчер уведомлений.
        realm:      Регион («RU» / «EU»).
        db_lock:    Общий asyncio.Lock для сериализации записей в conn.
                    При None — создаётся локальный лок (только чтение,
                    лок практически не нужен, но сохраняем сигнатуру единой).
    """
    realm = realm or settings.region.value

    if pool is None:
        pool = build_pool()

    # Флаг: соединение создано здесь → мы его и закрываем
    _owns_conn = conn is None
    if _owns_conn:
        conn = open_connection(settings.db_path)

    if db_lock is None:
        db_lock = asyncio.Lock()

    if dispatcher is None:
        dispatcher = build_dispatcher(cooldown_sec=LOTS_ALERT_COOLDOWN_SEC)

    log.info(
        "Lots watcher запущен: realm=%s, интервал=%ds, порог=%.0f%%",
        realm, LOTS_POLL_INTERVAL, LOTS_DISCOUNT_THRESHOLD * 100,
    )

    await pool.open()
    try:
      while True:
        try:
            await poll_lots_once(pool, conn, dispatcher, realm)
        except Exception as exc:
            log.error("Необработанная ошибка в poll_lots_once: %s", exc, exc_info=True)

        await asyncio.sleep(LOTS_POLL_INTERVAL)
    finally:
        if _owns_conn:
            conn.close()
        await pool.close()
