"""
db
==
Слой доступа к данным. Три модуля по ответственности:

  connection.py — get_connection(): единая точка создания sqlite3.Connection.
  catalog.py    — каталог предметов: upsert_item, detect_attr_type, log_sync.
  sales.py      — история продаж: dispatch_sale, get_sales, fetch-состояние.

Импортируй напрямую из нужного подмодуля:

    from db.connection import get_connection
    from db.catalog import upsert_item, log_sync
    from db.sales import dispatch_sales_batch, get_sales
"""
