# """
# tracker_server.py
# =================

# Локальный HTTP-сервер: мост между auction_tracker.html и SQLite.

# FREAK.db — read-only (база бота: items, analytics_summary)
# tracker.db — read-write (покупки, продажи, история трекера)

# Запуск:
#     python tracker_server.py
#     # или с явными путями:
#     DB_FREAK=data/FREAK.db DB_TRACKER=data/tracker.db python tracker_server.py

# Порт: 7734 (задаётся PORT=…)

# API:
#     GET  /ping
#     GET  /items?q=<query>   — autocomplete по FREAK.db
#     GET  /item?id=<item_id> — полная запись одного предмета
#     GET  /state             — всё состояние трекера
#     POST /buy               — зафиксировать закупку
#     POST /sell              — зафиксировать продажу

# CORS разрешён для localhost (нужен для открытого HTML-файла).
# """

# import html
# import json
# import os
# import sqlite3
# import threading          # FIX #1: нужен для _db_lock
# import traceback
# from http.server import BaseHTTPRequestHandler, HTTPServer
# from urllib.parse import parse_qs, urlparse

# from api.utils.logger import get_logger

# log = get_logger(__name__)

# # ─── CONFIG ───────────────────────────────────────────────────────────────────

# PORT         = int(os.getenv("PORT", "7734"))
# FREAK_PATH   = os.getenv("DB_FREAK",    "db/FREAK.db")
# TRACKER_PATH = os.getenv("DB_TRACKER",  "db/tracker.db")
# ANALYTICS_GRAN = "weekly"

# # ─── TRACKER DB SCHEMA ────────────────────────────────────────────────────────

# TRACKER_SCHEMA = """
# PRAGMA journal_mode=WAL;
# PRAGMA synchronous=NORMAL;
# PRAGMA foreign_keys=ON;

# CREATE TABLE IF NOT EXISTS purchases (
#     id            INTEGER PRIMARY KEY AUTOINCREMENT,
#     deal_group_id TEXT    NOT NULL,
#     item_id       TEXT    NOT NULL,
#     name          TEXT    NOT NULL,
#     icon_path     TEXT    NOT NULL DEFAULT '',
#     qty           INTEGER NOT NULL,
#     price         INTEGER NOT NULL,   -- цена лота целиком
#     ppu           INTEGER GENERATED ALWAYS AS (price / qty) VIRTUAL,
#     ts            INTEGER NOT NULL DEFAULT (strftime('%s','now'))
# );
# CREATE INDEX IF NOT EXISTS idx_pur_item ON purchases (item_id);
# CREATE INDEX IF NOT EXISTS idx_pur_dg   ON purchases (deal_group_id);

# CREATE TABLE IF NOT EXISTS sell_events (
#     id            INTEGER PRIMARY KEY AUTOINCREMENT,
#     deal_group_id TEXT,               -- NULL если куплено вне трекера
#     item_id       TEXT    NOT NULL,
#     qty           INTEGER NOT NULL,
#     price         INTEGER NOT NULL,   -- цена за штуку
#     revenue       INTEGER GENERATED ALWAYS AS (qty * price) VIRTUAL,
#     ts            INTEGER NOT NULL DEFAULT (strftime('%s','now'))
# );
# CREATE INDEX IF NOT EXISTS idx_sell_item ON sell_events (item_id);
# CREATE INDEX IF NOT EXISTS idx_sell_dg   ON sell_events (deal_group_id);

# -- deal_groups: одна закупка = одна сделка
# CREATE TABLE IF NOT EXISTS deal_groups (
#     id          TEXT    PRIMARY KEY,
#     item_id     TEXT    NOT NULL,
#     bought_qty  INTEGER NOT NULL DEFAULT 0,
#     sold_qty    INTEGER NOT NULL DEFAULT 0,
#     closed      INTEGER NOT NULL DEFAULT 0,  -- 1 = все продано
#     ts          INTEGER NOT NULL DEFAULT (strftime('%s','now'))
# );
# CREATE INDEX IF NOT EXISTS idx_dg_item ON deal_groups (item_id);
# """

# # ─── DB HELPERS ───────────────────────────────────────────────────────────────

# def _connect(path: str, readonly: bool = False) -> sqlite3.Connection:
#     if readonly:
#         uri  = f"file:{path}?mode=ro"
#         conn = sqlite3.connect(uri, uri=True)
#     else:
#         os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
#         conn = sqlite3.connect(path)
#     conn.row_factory = sqlite3.Row
#     conn.execute("PRAGMA journal_mode=WAL")
#     conn.execute("PRAGMA foreign_keys=ON")
#     return conn


# # ─── FIX #1: потокобезопасный доступ к _tracker_conn ─────────────────────────
# #
# # HTTPServer по умолчанию использует threading (каждый запрос — новый поток).
# # Глобальная переменная _tracker_conn читается и пишется из разных потоков
# # одновременно, что приводит к:
# #   • sqlite3.ProgrammingError (объект создан в другом потоке)
# #   • тихой порче данных при конкурентных commit-ах
# #
# # Решение: вместо одного глобального соединения каждый поток открывает
# # своё собственное соединение через threading.local().  Это полностью
# # устраняет гонку без блокировок на уровне Python-кода — sqlite3 (WAL-режим)
# # сам сериализует конкурентные записи на уровне файловых блокировок.
# #
# # Альтернатива (единое соединение + threading.Lock) хуже: она превращает
# # параллельный HTTP-сервер в однопоточный по чтению тоже.
# # ─────────────────────────────────────────────────────────────────────────────

# _local = threading.local()   # каждый поток хранит своё conn здесь
# _tracker_path_holder: list[str] = []  # mutable контейнер, чтобы можно было обновить из run_server


# def _get_thread_conn() -> sqlite3.Connection:
#     """Возвращает соединение tracker.db для текущего потока.

#     При первом вызове в данном потоке открывает новое соединение и
#     кэширует его в threading.local.  WAL-режим позволяет нескольким
#     читателям и одному писателю работать параллельно без блокировок
#     на уровне Python.
#     """
#     if not _tracker_path_holder:
#         raise RuntimeError(
#             "tracker connection не инициализировано; вызови run_server() первым"
#         )
#     conn = getattr(_local, "conn", None)
#     if conn is None:
#         path = _tracker_path_holder[0]
#         conn = sqlite3.connect(path)
#         conn.row_factory = sqlite3.Row
#         conn.execute("PRAGMA journal_mode=WAL")
#         conn.execute("PRAGMA foreign_keys=ON")
#         _local.conn = conn
#     return conn


# def get_tracker() -> sqlite3.Connection:
#     """Публичный accessor — всегда возвращает соединение текущего потока."""
#     return _get_thread_conn()


# def get_freak() -> sqlite3.Connection | None:
#     if not os.path.exists(FREAK_PATH):
#         return None
#     try:
#         return _connect(FREAK_PATH, readonly=True)
#     except Exception:
#         return None

# # ─── STATE BUILDER ────────────────────────────────────────────────────────────

# def build_state() -> dict:
#     """
#     Строит state аналогичный frontend-у:
#         items:      {item_id: {name, id, iconPath, qty, totalSpent, recQty, median, lots:[]}}
#         deals:      [{type, dealGroupId, itemId, name, qty, price, ppu?, revenue?, ts}]
#         dealGroups: {id: {itemId, boughtQty, soldQty, closed, ts}}
#     """
#     conn = get_tracker()

#     dg_rows = conn.execute("SELECT * FROM deal_groups ORDER BY ts DESC").fetchall()
#     dealGroups = {
#         r["id"]: {
#             "itemId":     r["item_id"],
#             "boughtQty":  r["bought_qty"],
#             "soldQty":    r["sold_qty"],
#             "closed":     bool(r["closed"]),
#             "ts":         r["ts"] * 1000,
#         }
#         for r in dg_rows
#     }

#     pur_rows = conn.execute("SELECT * FROM purchases ORDER BY ts ASC").fetchall()
#     items: dict = {}
#     for r in pur_rows:
#         iid, qty, price = r["item_id"], r["qty"], r["price"]
#         if iid not in items:
#             items[iid] = {
#                 "name":       html.escape(r["name"]),
#                 "id":         iid,
#                 "iconPath":   r["icon_path"],
#                 "qty":        0,
#                 "totalSpent": 0,
#                 "recQty":     0,
#                 "median":     0,
#                 "lots":       [],
#             }
#         items[iid]["qty"]        += qty
#         items[iid]["totalSpent"] += price
#         items[iid]["lots"].append({
#             "qty":   qty,
#             "price": price,
#             "ppu":   price // max(qty, 1),
#             "ts":    r["ts"] * 1000,
#         })

#     # ─── FIX #2: корректный пересчёт totalSpent при продаже ──────────────────
#     #
#     # ПРОБЛЕМА (оригинал):
#     #   share = sold_qty / qty_ПОСЛЕ_предыдущих_вычитаний
#     #   totalSpent *= (1 - share)
#     #
#     #   При 2+ продажах делитель каждый раз уменьшается, что даёт
#     #   экспоненциальное занижение: продали 1 из 10 → *0.9;
#     #   затем 1 из 9 → *0.889 вместо правильного *0.889 от ОРИГИНАЛА.
#     #   В итоге после N частичных продаж totalSpent стремится к нулю.
#     #
#     # ПРАВИЛЬНАЯ ЛОГИКА (FIFO/средняя себестоимость):
#     #   avg_cost_per_unit = totalSpent_ДО / qty_ДО
#     #   totalSpent -= avg_cost_per_unit * sold_qty
#     #
#     #   Эквивалентно: totalSpent *= (qty_ДО - sold_qty) / qty_ДО
#     #   где qty_ДО — количество ДО этой конкретной продажи.
#     #   Делитель берётся от qty на момент данной продажи, а не текущего.
#     # ─────────────────────────────────────────────────────────────────────────

#     sell_rows = conn.execute("SELECT * FROM sell_events ORDER BY ts ASC").fetchall()
#     for r in sell_rows:
#         iid      = r["item_id"]
#         if iid not in items:
#             continue
#         sold_qty   = r["qty"]
#         qty_before = items[iid]["qty"]          # qty ДО этой продажи
#         if qty_before > 0:
#             # Средняя себестоимость единицы × проданное количество
#             avg_cost = items[iid]["totalSpent"] / qty_before
#             items[iid]["totalSpent"] = max(0, round(items[iid]["totalSpent"] - avg_cost * sold_qty))
#         items[iid]["qty"] = max(0, qty_before - sold_qty)

#     freak = get_freak()
#     if freak:
#         for iid, item in items.items():
#             _enrich_from_freak(freak, iid, item)
#         freak.close()

#     buys_raw = [
#         (r["ts"] * 1000, {
#             "type":        "buy",
#             "dealGroupId": r["deal_group_id"],
#             "itemId":      r["item_id"],
#             "name":        html.escape(r["name"]),
#             "qty":         r["qty"],
#             "price":       r["price"],
#             "ppu":         r["price"] // max(r["qty"], 1),
#             "ts":          r["ts"] * 1000,
#         })
#         for r in pur_rows
#     ]
#     sells_raw = [
#         (r["ts"] * 1000, {
#             "type":        "sell",
#             "dealGroupId": r["deal_group_id"],
#             "itemId":      r["item_id"],
#             "name":        items.get(r["item_id"], {}).get("name", r["item_id"]),
#             "qty":         r["qty"],
#             "price":       r["price"],
#             "revenue":     r["qty"] * r["price"],
#             "ts":          r["ts"] * 1000,
#         })
#         for r in sell_rows
#     ]
#     deals = [d for _, d in sorted(buys_raw + sells_raw, key=lambda x: -x[0])]

#     return {"items": items, "deals": deals, "dealGroups": dealGroups}


# def _enrich_from_freak(freak: sqlite3.Connection, item_id: str, item: dict) -> None:
#     row = freak.execute(
#         "SELECT icon_path FROM items WHERE item_id = ?", (item_id,)
#     ).fetchone()
#     if row and row["icon_path"]:
#         item["iconPath"] = row["icon_path"]

#     an = freak.execute("""
#         SELECT avg_price, amount_p50
#         FROM   analytics_summary
#         WHERE  item_id     = ?
#           AND  granularity = 'weekly'
#           AND  qlt = -1 AND ptn = -1 AND upgrade_level = -1
#         ORDER  BY bucket_key DESC
#         LIMIT  1
#     """, (item_id,)).fetchone()
#     if an:
#         if an["avg_price"]:
#             item["median"] = round(an["avg_price"])
#         if an["amount_p50"]:
#             item["recQty"] = an["amount_p50"]

# # ─── FREAK AUTOCOMPLETE ───────────────────────────────────────────────────────

# def search_items(q: str, limit: int = 12) -> list[dict]:
#     freak = get_freak()
#     if not freak:
#         return []
#     try:
#         pattern = f"%{q}%"
#         rows = freak.execute("""
#             SELECT DISTINCT i.item_id, i.name_ru, i.name_en, i.icon_path,
#                             an.avg_price, an.amount_p50
#             FROM   items i
#             LEFT JOIN analytics_summary an
#                    ON an.item_id     = i.item_id
#                   AND an.granularity = 'weekly'
#                   AND an.qlt = -1 AND an.ptn = -1 AND an.upgrade_level = -1
#                   AND an.bucket_key  = (
#                           SELECT MAX(bucket_key) FROM analytics_summary
#                           WHERE  item_id     = i.item_id
#                             AND  granularity = 'weekly'
#                             AND  qlt = -1 AND ptn = -1 AND upgrade_level = -1
#                       )
#             WHERE  i.name_ru LIKE ? OR i.name_en LIKE ? OR i.item_id LIKE ?
#             LIMIT  ?
#         """, (pattern, pattern, pattern, limit)).fetchall()
#         return [{
#             "item_id":  r["item_id"],
#             "name_ru":  r["name_ru"]  or "",
#             "name_en":  r["name_en"]  or "",
#             "icon_path": r["icon_path"] or "",
#             "median":   round(r["avg_price"]) if r["avg_price"] else None,
#             "rec_qty":  r["amount_p50"] if r["amount_p50"] else None,
#         } for r in rows]
#     finally:
#         freak.close()


# def get_item(item_id: str) -> dict | None:
#     results = search_items(item_id, limit=1)
#     if results and results[0]["item_id"] == item_id:
#         return results[0]
#     return None

# # ─── WRITE OPERATIONS ─────────────────────────────────────────────────────────

# def record_buy(body: dict) -> None:
#     """body: {deal_group_id, item_id, name, icon_path, qty, price}"""
#     dgid  = body["deal_group_id"]
#     iid   = body["item_id"]
#     name  = html.escape(str(body.get("name", iid)))
#     icon  = body.get("icon_path", "")
#     qty   = int(body["qty"])
#     price = int(body["price"])
#     if qty <= 0 or price <= 0:
#         raise ValueError("qty and price must be positive")

#     conn = get_tracker()
#     conn.execute("""
#         INSERT INTO deal_groups (id, item_id, bought_qty)
#         VALUES (?, ?, ?)
#         ON CONFLICT(id) DO UPDATE SET bought_qty = bought_qty + excluded.bought_qty
#     """, (dgid, iid, qty))
#     conn.execute("""
#         INSERT INTO purchases (deal_group_id, item_id, name, icon_path, qty, price)
#         VALUES (?, ?, ?, ?, ?, ?)
#     """, (dgid, iid, name, icon, qty, price))
#     conn.commit()


# def record_sell(body: dict) -> None:
#     """body: {deal_group_id, item_id, qty, price}"""
#     dgid  = body.get("deal_group_id")
#     iid   = body["item_id"]
#     qty   = int(body["qty"])
#     price = int(body["price"])
#     if qty <= 0 or price <= 0:
#         raise ValueError("qty and price must be positive")

#     conn = get_tracker()
#     conn.execute("""
#         INSERT INTO sell_events (deal_group_id, item_id, qty, price)
#         VALUES (?, ?, ?, ?)
#     """, (dgid, iid, qty, price))
#     if dgid:
#         conn.execute("""
#             UPDATE deal_groups
#             SET sold_qty = sold_qty + ?,
#                 closed   = CASE WHEN sold_qty + ? >= bought_qty THEN 1 ELSE 0 END
#             WHERE id = ?
#         """, (qty, qty, dgid))
#     conn.commit()

# # ─── HTTP HANDLER ─────────────────────────────────────────────────────────────

# CORS_HEADERS = {
#     "Access-Control-Allow-Origin":  "*",
#     "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
#     "Access-Control-Allow-Headers": "Content-Type",
# }


# class Handler(BaseHTTPRequestHandler):

#     def log_message(self, fmt, *args):
#         log.debug(fmt, *args)

#     def send_json(self, data, status=200):
#         body = json.dumps(data, ensure_ascii=False).encode()
#         self.send_response(status)
#         for k, v in CORS_HEADERS.items():
#             self.send_header(k, v)
#         self.send_header("Content-Type",   "application/json; charset=utf-8")
#         self.send_header("Content-Length", len(body))
#         self.end_headers()
#         self.wfile.write(body)

#     def send_err(self, msg, status=400):
#         self.send_json({"error": msg}, status)

#     def do_OPTIONS(self):
#         self.send_response(204)
#         for k, v in CORS_HEADERS.items():
#             self.send_header(k, v)
#         self.end_headers()

#     def do_GET(self):
#         parsed = urlparse(self.path)
#         qs     = parse_qs(parsed.query)
#         try:
#             if parsed.path == "/ping":
#                 freak_ok = os.path.exists(FREAK_PATH)
#                 self.send_json({"ok": True, "freak_ok": freak_ok, "tracker_path": TRACKER_PATH})
#             elif parsed.path == "/items":
#                 self.send_json(search_items(qs.get("q", [""])[0]))
#             elif parsed.path == "/item":
#                 item = get_item(qs.get("id", [""])[0])
#                 self.send_json(item) if item else self.send_err("not found", 404)
#             elif parsed.path == "/state":
#                 self.send_json(build_state())
#             else:
#                 self.send_err("not found", 404)
#         except Exception:
#             log.exception("GET %s failed", self.path)
#             self.send_err("internal error", 500)

#     def do_POST(self):
#         parsed = urlparse(self.path)
#         length = int(self.headers.get("Content-Length", 0))
#         body   = json.loads(self.rfile.read(length) or b"{}") if length else {}
#         try:
#             if parsed.path == "/buy":
#                 record_buy(body)
#                 self.send_json({"ok": True})
#             elif parsed.path == "/sell":
#                 record_sell(body)
#                 self.send_json({"ok": True})
#             else:
#                 self.send_err("not found", 404)
#         except KeyError as e:
#             self.send_err(f"missing field: {e}", 400)
#         except ValueError as e:
#             self.send_err(str(e), 400)
#         except Exception:
#             log.exception("POST %s failed", self.path)
#             self.send_err("internal error", 500)


# def run_server() -> None:
#     """Запустить сервер в текущем потоке (блокирующий вызов).

#     FIX #1 (thread-safety):
#         Больше не держим единое глобальное соединение.  Вместо этого
#         инициализируем _tracker_path_holder (путь к БД) и применяем схему
#         через одноразовое соединение в главном потоке.  Каждый рабочий поток
#         HTTPServer сам открывает своё соединение через get_tracker() →
#         _get_thread_conn(), которое кэшируется в threading.local().

#         WAL-режим (уже задан в TRACKER_SCHEMA) позволяет нескольким
#         конкурентным писателям работать без блокировок на уровне Python.
#     """
#     # Инициализируем путь для threading.local-соединений
#     _tracker_path_holder.clear()
#     _tracker_path_holder.append(TRACKER_PATH)

#     # Применяем схему через временное соединение в текущем потоке
#     os.makedirs(os.path.dirname(os.path.abspath(TRACKER_PATH)), exist_ok=True)
#     init_conn = sqlite3.connect(TRACKER_PATH)
#     try:
#         init_conn.executescript(TRACKER_SCHEMA)
#         init_conn.commit()
#     finally:
#         init_conn.close()

#     freak_status = "found" if os.path.exists(FREAK_PATH) else "NOT FOUND"
#     log.info(
#         "Auction Tracker Server | port=%d | freak=%s (%s) | tracker=%s",
#         PORT, FREAK_PATH, freak_status, TRACKER_PATH,
#     )

#     server = HTTPServer(("127.0.0.1", PORT), Handler)
#     try:
#         server.serve_forever()
#     finally:
#         server.server_close()
#         # WAL-checkpoint: сбрасываем журнал в основной файл при остановке
#         try:
#             fin_conn = sqlite3.connect(TRACKER_PATH)
#             fin_conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
#             fin_conn.commit()
#             fin_conn.close()
#         except Exception:
#             pass
#         _tracker_path_holder.clear()
#         log.info("Tracker server остановлен.")


# if __name__ == "__main__":
#     run_server()