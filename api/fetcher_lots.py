"""
api/fetcher_lots.py
===================
Опрос активных лотов аукциона и отправка уведомлений о выгодных предложениях.

Архитектура:
  Бесконечный цикл → один проход = poll_lots_once().

  poll_lots_once использует ту же модель воркеров, что и fetcher_anal:
    - asyncio.Queue с watched-фильтрами
    - N воркеров (_worker), каждый привязан к clients[worker_id]
    - Каждый воркер последовательно берёт фильтры из очереди
      и обрабатывает их через _process_item
    - Это даёт строгий back-pressure (воркер не берёт следующий
      фильтр пока не завершит текущий) и равномерную нагрузку
      по всем клиентам пула без лишних корутин в памяти.

  Конкурентность: len(pool.clients) × LOTS_CONCURRENT_PER_CLIENT
  запросов одновременно — столько воркеров в очереди.

Логика «выгодности»:
  lot_price < avg_price × (1 − LOTS_DISCOUNT_THRESHOLD)

  Порог адаптируется к волатильности: высокая волатильность →
  требуем бо́льшую скидку, чтобы не давать ложных срабатываний.

Фильтрация лотов по атрибутам:
  WatchFilter.matches_lot() проверяет qlt/ptn/upgrade_level.
  Sentinel -1 в фильтре = «любое значение».

Эндпоинт API:
  GET /{region}/auction/{item_id}/lots
  Ожидаемый ответ: { "total": N, "lots": [ { "price", "amount", ... } ] }
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
from db.connection import open_connection
from notifications.base import LotAlert
from notifications.dispatcher import NotifierDispatcher, build_dispatcher
from schema import ATTR_SENTINEL
from watched_items import WatchFilter, load_watched_filters

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

LOTS_POLL_INTERVAL        : int   = settings.lots_poll_interval
LOTS_DISCOUNT_THRESHOLD   : float = settings.lots_discount_threshold
LOTS_ALERT_COOLDOWN_SEC   : int   = settings.lots_alert_cooldown_sec
LOTS_VOLATILITY_SCALE_THR : float = settings.lots_volatility_scale_thr

# Воркеров на клиент пула: итоговый параллелизм = len(pool.clients) × N.
# Значение 4 — разумный баланс между скоростью обхода и нагрузкой на API.
LOTS_CONCURRENT_PER_CLIENT: int = 4

LOTS_ENDPOINT = "/{region}/auction/{item_id}/lots"


# ---------------------------------------------------------------------------
# Вспомогательные типы
# ---------------------------------------------------------------------------

@dataclass
class LotRaw:
    """Сырые данные одного активного лота от API."""
    price:         float
    amount:        int
    seller:        str        = ""
    expires_at:    int | None = None
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

    Реальный формат ответа API:
      {
        "total": N,
        "lots": [
          {
            "itemId":     "okm20",
            "amount":     1,
            "startPrice": 350000,
            "buyoutPrice": 450000,   ← цена мгновенной покупки
            "startTime":  "2026-06-25T20:56:14Z",
            "endTime":    "2026-06-27T20:56:14Z",  ← время истечения
            "additional": {          ← dict с произвольными ключами (не qlt/ptn)
              "buyer": "PlayerName",
              ...
            }
          }, ...
        ]
      }

    Цена: buyoutPrice (цена мгновенной покупки). Если отсутствует — startPrice.
    Время: endTime (дата истечения лота).
    additional: dict с произвольными метаданными (не содержит qlt/ptn/upgrade_level для обычных предметов).
    """
    lots = []
    for raw in data.get("lots", data.get("items", [])):
        additional = raw.get("additional") or {}
        qlt = ptn = ul = None

        if isinstance(additional, dict):
            # Обычный формат: dict с произвльными ключами (читаем qlt/ptn/upgrade_level если есть)
            try:
                if "qlt" in additional:
                    qlt = int(additional["qlt"])
                if "ptn" in additional:
                    ptn = int(additional["ptn"])
                if "upgrade_level" in additional:
                    ul = int(additional["upgrade_level"])
            except (TypeError, ValueError):
                pass

        elif isinstance(additional, list):
            # Старый формат: [{"key": "ItemQuality", "value": 2}, ...]
            for attr in additional:
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

        # Время истечения: endTime > time > expires_at
        expires_at = None
        time_str = raw.get("endTime") or raw.get("time") or raw.get("expires_at")
        if time_str:
            try:
                dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                expires_at = int(dt.timestamp())
            except (ValueError, AttributeError):
                pass

        # Цена: buyoutPrice (мгновенная покупка) > price > startPrice
        price_raw = raw.get("buyoutPrice") or raw.get("price") or raw.get("startPrice")
        if price_raw is None:
            log.debug("Пропуск лота — нет цены: %s", raw)
            continue

        try:
            lots.append(LotRaw(
                price         = float(price_raw),
                amount        = int(raw.get("amount", 1)),
                seller        = raw.get("seller", ""),
                expires_at    = expires_at,
                qlt           = qlt,
                ptn           = ptn,
                upgrade_level = ul,
            ))
        except (KeyError, TypeError, ValueError) as exc:
            log.debug("Пропуск лота с ошибкой парсинга: %s | %s", exc, raw)

    return lots
def _load_metrics(
    conn:          sqlite3.Connection,
    item_id:       str,
    qlt:           int,
    ptn:           int,
    upgrade_level: int,
    granularity:   str = "weekly",
) -> dict[str, float | None]:
    """
    Загружает метрики из analytics_summary для заданного среза.

    Возвращает только строки с low_sample=0 (надёжная аналитика).
    Если точный срез не найден — пробует qlt-агрегат (ptn=-1),
    затем полный sentinel-агрегат.

    conn.row_factory сбрасывается в исходное состояние после запроса —
    избегаем утечки состояния между конкурентными корутинами.
    """
    original_factory = conn.row_factory
    conn.row_factory  = sqlite3.Row

    def _query(q: int, p: int, ul: int) -> dict | None:
        row = conn.execute(
            """
            SELECT AVG(avg_price)     AS avg_price,
                   AVG(volatility)    AS volatility,
                   AVG(liquidity)     AS liquidity,
                   AVG(sales_per_day) AS sales_per_day,
                   AVG(price_single)  AS price_single,
                   AVG(price_bulk)    AS price_bulk,
                   AVG(bulk_share)    AS bulk_share,
                   AVG(vol_single)    AS vol_single,
                   AVG(amount_p50)    AS amount_p50
            FROM analytics_summary
            WHERE item_id       = ?
              AND granularity   = ?
              AND qlt           = ?
              AND ptn           = ?
              AND upgrade_level = ?
              AND low_sample    = 0
            """,
            (item_id, granularity, q, p, ul),
        ).fetchone()
        if row and row["avg_price"] is not None:
            return dict(row)
        return None

    try:
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
    finally:
        conn.row_factory = original_factory


# ---------------------------------------------------------------------------
# Логика оценки выгодности
# ---------------------------------------------------------------------------

def _effective_threshold(volatility: float | None) -> float:
    """
    Адаптивный порог скидки.

    Высокая волатильность → требуем бо́льшую скидку, иначе слишком
    много ложных срабатываний на нестабильных предметах.
    """
    if volatility is None:
        return LOTS_DISCOUNT_THRESHOLD
    if volatility > LOTS_VOLATILITY_SCALE_THR:
        scale = min(volatility / LOTS_VOLATILITY_SCALE_THR, 2.0)
        return LOTS_DISCOUNT_THRESHOLD * scale
    return LOTS_DISCOUNT_THRESHOLD


def _resolve_ref_price(
    lot:     LotRaw,
    metrics: dict[str, float | None],
) -> tuple[float | None, float | None, str]:
    """
    Выбирает эталонную цену для лота с учётом его amount.

      amount == 1        → price_single + vol_single  (розн.)
      amount >= p50      → price_bulk   + volatility  (опт.)
      иначе              → avg_price    + volatility  (ср.)

    Returns: (ref_price, ref_volatility, label)
    """
    p50      = metrics.get("amount_p50")
    bulk_thr = int(p50) if p50 is not None else None

    if lot.amount == 1:
        ref = metrics.get("price_single")
        vol = metrics.get("vol_single")
        if ref is not None:
            return ref, vol, "розн."

    if bulk_thr is not None and lot.amount >= bulk_thr:
        ref = metrics.get("price_bulk")
        if ref is not None:
            return ref, metrics.get("volatility"), "опт."

    return metrics.get("avg_price"), metrics.get("volatility"), "ср."


def _is_profitable(
    lot:     LotRaw,
    metrics: dict[str, float | None],
) -> tuple[bool, float, str]:
    """
    Проверяет, выгоден ли лот.

    Returns: (is_profitable, discount_pct, price_label)
    """
    ref_price, ref_vol, label = _resolve_ref_price(lot, metrics)

    if ref_price is None or ref_price <= 0:
        return False, 0.0, label

    cutoff = ref_price * (1.0 - _effective_threshold(ref_vol))
    if lot.price_per_unit < cutoff:
        discount_pct = (ref_price - lot.price_per_unit) / ref_price * 100.0
        return True, discount_pct, label

    return False, 0.0, label


# ---------------------------------------------------------------------------
# Один предмет: запрос лотов + анализ
# ---------------------------------------------------------------------------

async def _process_item(
    client:     Any,                  # StalcraftClient
    conn:       sqlite3.Connection,
    wf:         WatchFilter,
    dispatcher: NotifierDispatcher,
    realm:      str,
    semaphore:  asyncio.Semaphore | None = None,
    db_lock:    asyncio.Lock      | None = None,
) -> int:
    """
    Запрашивает активные лоты для одного WatchFilter, сравнивает с аналитикой,
    отправляет алерты для выгодных.

    Принимает конкретный client (не pool) — воркер сам передаёт свой клиент,
    что гарантирует равномерное распределение нагрузки по пулу.

    semaphore ограничивает число одновременных HTTP-запросов внутри одного
    воркера (LOTS_CONCURRENT_PER_CLIENT слотов). Воркеров ровно n_clients,
    каждый держит до LOTS_CONCURRENT_PER_CLIENT параллельных запросов —
    итоговый параллелизм: n_clients × LOTS_CONCURRENT_PER_CLIENT.

    db_lock сериализует все обращения к conn — SQLite не поддерживает
    конкурентную запись/чтение из одного Connection объекта в asyncio.

    Returns: количество отправленных алертов.
    """
    endpoint = LOTS_ENDPOINT.format(region=realm.upper(), item_id=wf.item_id)
    _sem  = semaphore or asyncio.Semaphore(1)
    _lock = db_lock   or asyncio.Lock()

    # ── HTTP-запрос (под семафором, без db_lock) ──────────────────────────
    async with _sem:
        try:
            data = await client.get(endpoint)
        except Exception as exc:
            log.warning("Ошибка запроса лотов %s: %s", wf.item_id, exc)
            return 0

    log.debug("%s: raw API total=%s items=%s keys=%s",
              wf.item_id,
              data.get('total') if isinstance(data, dict) else 'NOT_DICT',
              len(data.get('lots', data.get('items', []))) if isinstance(data, dict) else '?',
              list(data.keys())[:6] if isinstance(data, dict) else type(data).__name__)

    lots = _parse_lots(data)
    if not lots:
        log.debug("%s: лотов нет (total=%s в ответе)",
                  wf.item_id, data.get('total') if isinstance(data, dict) else '?')
        return 0

    log.debug("%s: получено %d лотов", wf.item_id, len(lots))

    # ── Чтение данных предмета из БД (под db_lock) ───────────────────────
    async with _lock:
        original_factory = conn.row_factory
        conn.row_factory  = sqlite3.Row
        try:
            item_row = conn.execute(
                "SELECT name_ru, name_en, color FROM items WHERE item_id = ?",
                (wf.item_id,),
            ).fetchone()
        finally:
            conn.row_factory = original_factory

    name_ru = item_row["name_ru"] if item_row else wf.item_id
    name_en = item_row["name_en"] if item_row else ""
    color   = item_row["color"]   if item_row else ""

    alerts_sent = 0
    for lot in lots:
        if not wf.matches_lot(lot.qlt, lot.ptn, lot.upgrade_level):
            log.debug(
                "%s: лот пропущен — не совпадает фильтр (lot qlt=%s ptn=%s ul=%s, filter qlt=%s ptn=%s ul=%s)",
                wf.item_id, lot.qlt, lot.ptn, lot.upgrade_level,
                wf.qlt, wf.ptn, wf.upgrade_level,
            )
            continue

        q  = lot.qlt           if lot.qlt           is not None else ATTR_SENTINEL
        p  = lot.ptn           if lot.ptn           is not None else ATTR_SENTINEL
        ul = lot.upgrade_level if lot.upgrade_level is not None else ATTR_SENTINEL

        # ── Чтение метрик (под db_lock) ───────────────────────────────────
        async with _lock:
            metrics = _load_metrics(conn, wf.item_id, q, p, ul)

        avg = metrics.get("avg_price")
        log.debug(
            "%s: лот цена=%.0f avg_price=%s vol=%s порог=%.0f%%",
            wf.item_id, lot.price_per_unit,
            f"{avg:.0f}" if avg else "None",
            f"{metrics.get('volatility'):.3f}" if metrics.get("volatility") is not None else "None",
            _effective_threshold(metrics.get("volatility")) * 100,
        )

        profitable, discount_pct, label = _is_profitable(lot, metrics)
        if not profitable:
            log.debug(
                "%s: лот не выгоден (цена=%.0f, порог=%.0f, скидка=%.1f%%)",
                wf.item_id, lot.price_per_unit,
                avg * (1 - _effective_threshold(metrics.get("volatility"))) if avg else 0,
                discount_pct,
            )
            continue

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
            price_label   = label,
            bulk_share    = metrics.get("bulk_share"),
        )
        log.info(
            "Выгодный лот найден: %s — цена %.0f (−%.1f%% от %.0f %s), "
            "дубль=%s",
            alert.display_name, lot.price_per_unit, discount_pct,
            ref_price or 0, label,
            dispatcher.is_duplicate(alert),
        )
        # ── Отправка уведомления (вне db_lock — сетевой вызов) ────────────
        await dispatcher.send(alert)
        alerts_sent += 1

    return alerts_sent


# ---------------------------------------------------------------------------
# Воркер очереди (аналог _worker из fetcher_anal)
# ---------------------------------------------------------------------------

async def _worker(
    worker_id:  int,
    client:     Any,                  # StalcraftClient
    queue:      asyncio.Queue,
    conn:       sqlite3.Connection,
    dispatcher: NotifierDispatcher,
    realm:      str,
    semaphore:  asyncio.Semaphore | None = None,
    db_lock:    asyncio.Lock      | None = None,
) -> None:
    """
    Воркер берёт WatchFilter из очереди и обрабатывает его.
    Sentinel None → завершение.

    Воркеров ровно n_clients (по одному на клиент пула). Каждый воркер
    передаёт семафор в _process_item, позволяя держать до
    LOTS_CONCURRENT_PER_CLIENT параллельных HTTP-запросов одновременно.
    Итоговый параллелизм: n_clients × LOTS_CONCURRENT_PER_CLIENT.

    db_lock пробрасывается в _process_item для сериализации доступа к conn.
    """
    request_delay = settings.request_delay
    total_alerts = 0
    while True:
        item = await queue.get()
        try:
            if item is None:
                return total_alerts
            idx, total, wf = item
            log.debug(
                "worker=%d client=%s %d/%d %s",
                worker_id, client.name, idx, total, wf.item_id,
            )
            sent = await _process_item(client, conn, wf, dispatcher, realm, semaphore, db_lock)
            total_alerts += sent
            await asyncio.sleep(request_delay)
        except Exception as exc:
            item_id = wf.item_id if item else "?"
            log.error("worker=%d item=%s: %s", worker_id, item_id, exc, exc_info=True)
        finally:
            queue.task_done()


# ---------------------------------------------------------------------------
# Один проход по всем watched_items
# ---------------------------------------------------------------------------

async def poll_lots_once(
    pool:       StalcraftClientPool,
    conn:       sqlite3.Connection,
    dispatcher: NotifierDispatcher,
    realm:      str,
    db_lock:    asyncio.Lock | None = None,
) -> None:
    """
    Один полный проход: загружает watched_filters и опрашивает лоты для каждого.

    Модель — Queue + воркеры, как в fetcher_anal:
      - Воркеров ровно n_clients (по одному на клиент пула)
      - Каждый воркер держит до LOTS_CONCURRENT_PER_CLIENT параллельных
        HTTP-запросов через общий семафор (итого: n_clients × N слотов)
      - Back-pressure: воркер берёт следующий фильтр только после того,
        как семафор освободил слот для текущего запроса
      - db_lock сериализует все обращения к conn между воркерами
    """
    _lock = db_lock or asyncio.Lock()

    async with _lock:
        filters = load_watched_filters(conn)
    if not filters:
        log.debug("watched_items пуст, пропускаем опрос лотов.")
        return

    total     = len(filters)
    n_clients = len(pool.clients)
    n_workers = n_clients
    concurrency = n_clients * LOTS_CONCURRENT_PER_CLIENT
    semaphore   = asyncio.Semaphore(concurrency)
    log.info(
        "Опрос лотов: %d предметов, %d воркеров, семафор=%d.",
        total, n_workers, concurrency,
    )

    queue: asyncio.Queue = asyncio.Queue()
    for idx, wf in enumerate(filters, start=1):
        queue.put_nowait((idx, total, wf))
    for _ in range(n_workers):
        queue.put_nowait(None)   # sentinel для каждого воркера

    workers = [
        asyncio.create_task(
            _worker(
                worker_id  = i,
                client     = pool.client_for_worker(i),
                queue      = queue,
                conn       = conn,
                dispatcher = dispatcher,
                realm      = realm,
                semaphore  = semaphore,
                db_lock    = _lock,
            )
        )
        for i in range(n_workers)
    ]

    await queue.join()
    results = await asyncio.gather(*workers, return_exceptions=True)

    errors       = sum(1 for r in results if isinstance(r, Exception))
    alerts_total = sum(r for r in results if isinstance(r, int))

    if errors:
        log.warning("poll_lots_once: %d воркеров завершились с ошибкой.", errors)

    log.info(
        "Опрос завершён (%d предметов, алертов отправлено: %d).",
        total, alerts_total,
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

    Вызывается из main._run_main_pipeline после завершения setup_analysis.
    Все аргументы опциональны — при None создаются из настроек.

    Args:
        pool:       Пул API-клиентов. При None — создаётся и закрывается здесь.
        conn:       Соединение с БД. При совместном запуске с setup_analysis
                    передавать то же соединение во избежание WAL-конфликтов.
        dispatcher: Диспетчер уведомлений.
        realm:      Регион («RU» / «EU»).
        db_lock:    Не используется в текущей реализации (fetcher_lots только
                    читает из БД), но принимается для единообразия сигнатуры
                    с fetcher_anal.
    """
    realm = realm or settings.region.value

    _owns_pool = pool is None
    if _owns_pool:
        pool = build_pool()

    _owns_conn = conn is None
    if _owns_conn:
        conn = open_connection(settings.db_path)

    if dispatcher is None:
        dispatcher = build_dispatcher(cooldown_sec=LOTS_ALERT_COOLDOWN_SEC)

    log.info(
        "Lots watcher запущен: realm=%s, интервал=%ds, порог=%.0f%%.",
        realm, LOTS_POLL_INTERVAL, LOTS_DISCOUNT_THRESHOLD * 100,
    )

    if _owns_pool:
        await pool.open()
    try:
        while True:
            try:
                await poll_lots_once(pool, conn, dispatcher, realm, db_lock)
            except Exception as exc:
                log.error("Необработанная ошибка в poll_lots_once: %s", exc, exc_info=True)
            await asyncio.sleep(LOTS_POLL_INTERVAL)
    finally:
        if _owns_conn:
            conn.close()
        if _owns_pool:
            await pool.close()