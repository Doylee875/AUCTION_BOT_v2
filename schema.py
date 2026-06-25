"""
schema.py
=========
Единая схема базы данных. Единственное место, где описаны таблицы и индексы.

Принципы схемы:
  - Один тип продажи — одна таблица атрибутов (sale_attrs).
  - Тип атрибутов предмета зафиксирован в items.attr_type.
  - Состояние загрузки истории хранится в items (fetch_*).
  - Аналитика — одна таблица analytics_summary для всех типов.
    Для обычных предметов qlt=ptn=upgrade_level=-1 (sentinel «не применимо»).
    Sentinel -1 вместо NULL в PK: SQLite NULL != NULL, INSERT OR REPLACE
    с NULL в PK тихо создаёт дубли, а -1 работает корректно.

Константы ATTR_TYPE_* используются везде вместо магических строк.
"""

import sqlite3

import api.utils.logger

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Константы типов атрибутов
# ---------------------------------------------------------------------------

ATTR_TYPE_NONE     = "none"      # обычный предмет, нет доп. атрибутов
ATTR_TYPE_ARTIFACT = "artifact"  # qlt + ptn
ATTR_TYPE_UPGRADE  = "upgrade"   # upgrade_level
ATTR_TYPE_QLT_ONLY = "qlt_only"  # только qlt (ядра модулей и т.п.)

# Sentinel: «атрибут не применим» — используется в PK analytics_summary
ATTR_SENTINEL = -1


# ---------------------------------------------------------------------------
# Инициализация
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection) -> None:
    """
    Создаёт все таблицы и индексы. Идемпотентна (IF NOT EXISTS).
    Вызывать при каждом старте приложения.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")
    cur.execute("PRAGMA journal_mode = WAL")
    cur.execute("PRAGMA synchronous = NORMAL")

    # -----------------------------------------------------------------------
    # items — каталог предметов
    # -----------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS items (
            item_id         TEXT    NOT NULL PRIMARY KEY,
            category        TEXT    NOT NULL,
            subcategory     TEXT    NOT NULL DEFAULT '',
            -- Тип атрибутов: 'none' | 'artifact' | 'upgrade' | 'qlt_only'
            attr_type       TEXT    NOT NULL DEFAULT 'none',
            color           TEXT,
            name_ru         TEXT,
            name_en         TEXT,
            raw_json        TEXT    NOT NULL,
            icon_path       TEXT,
            updated_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),

            -- Состояние загрузки истории продаж
            fetch_total     INTEGER NOT NULL DEFAULT 0,
            fetch_time      INTEGER,
            fetch_offset    INTEGER NOT NULL DEFAULT 0,
            last_sale_at    INTEGER
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_category    ON items (category)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_subcategory ON items (subcategory)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_color       ON items (color)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_attr_type   ON items (attr_type)")

    # Мультиязычные имена для поиска
    cur.execute("""
        CREATE TABLE IF NOT EXISTS items_names (
            item_id TEXT NOT NULL,
            lang    TEXT NOT NULL,
            name    TEXT NOT NULL,
            PRIMARY KEY (item_id, lang)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_items_names_name ON items_names (name)")

    # История изменений каталога
    cur.execute("""
        CREATE TABLE IF NOT EXISTS items_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id     TEXT    NOT NULL,
            category    TEXT    NOT NULL,
            subcategory TEXT    NOT NULL DEFAULT '',
            attr_type   TEXT    NOT NULL DEFAULT 'none',
            color       TEXT,
            name_ru     TEXT,
            name_en     TEXT,
            raw_json    TEXT    NOT NULL,
            icon_path   TEXT,
            updated_at  INTEGER NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_history_item ON items_history (item_id)")

    # Лог синхронизаций каталога
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            run_at         INTEGER PRIMARY KEY,
            items_inserted INTEGER NOT NULL,
            items_updated  INTEGER NOT NULL
        )
    """)

    # Список отслеживаемых предметов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS watched_items (
            item_id       TEXT    NOT NULL,
            qlt           INTEGER NOT NULL DEFAULT -1,
            ptn           INTEGER NOT NULL DEFAULT -1,
            upgrade_level INTEGER NOT NULL DEFAULT -1,
            added_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (item_id, qlt, ptn, upgrade_level)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_watched_added   ON watched_items (added_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_watched_item_id ON watched_items (item_id)")

    # -----------------------------------------------------------------------
    # sales — история продаж
    # -----------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id        TEXT    NOT NULL,
            price          INTEGER NOT NULL,
            amount         INTEGER NOT NULL,
            price_per_unit INTEGER GENERATED ALWAYS AS (price / amount) VIRTUAL,
            sold_at        INTEGER NOT NULL,

            UNIQUE  (item_id, sold_at, price, amount),
            FOREIGN KEY (item_id) REFERENCES items (item_id)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sales_item_time
        ON sales (item_id, sold_at DESC)
    """)

    # -----------------------------------------------------------------------
    # sale_attrs — единая таблица атрибутов для всех типов предметов
    # -----------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sale_attrs (
            sale_id       INTEGER PRIMARY KEY,
            qlt           INTEGER,
            ptn           INTEGER CHECK (ptn IS NULL OR ptn BETWEEN 0 AND 15),
            upgrade_level INTEGER CHECK (upgrade_level IS NULL OR upgrade_level BETWEEN 0 AND 15),
            FOREIGN KEY (sale_id) REFERENCES sales (id) ON DELETE CASCADE
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sale_attrs_artifact
        ON sale_attrs (qlt, ptn)
        WHERE qlt IS NOT NULL AND ptn IS NOT NULL
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sale_attrs_upgrade
        ON sale_attrs (upgrade_level)
        WHERE upgrade_level IS NOT NULL
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_sale_attrs_qlt_only
        ON sale_attrs (qlt)
        WHERE qlt IS NOT NULL AND ptn IS NULL
    """)

    # -----------------------------------------------------------------------
    # analytics_summary — единая таблица аналитики для всех типов
    # -----------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics_summary (
            item_id       TEXT    NOT NULL,
            granularity   TEXT    NOT NULL,
            bucket_key    TEXT    NOT NULL,
            qlt           INTEGER NOT NULL DEFAULT -1,
            ptn           INTEGER NOT NULL DEFAULT -1,
            upgrade_level INTEGER NOT NULL DEFAULT -1,

            liquidity     REAL,
            sales_per_day REAL,
            avg_price     REAL,
            volatility    REAL,
            trend         REAL,
            total_amount  INTEGER,

            amount_p50    INTEGER,
            price_single  REAL,
            price_bulk    REAL,
            bulk_share    REAL,
            vol_single    REAL,

            price_spread  REAL,
            amount_mode   INTEGER,
            spread_stable INTEGER NOT NULL DEFAULT 0,

            relative_volume REAL,

            low_sample    INTEGER NOT NULL DEFAULT 0,
            calculated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),

            PRIMARY KEY (item_id, granularity, bucket_key, qlt, ptn, upgrade_level)
        )
    """)

    # -----------------------------------------------------------------------
    # lot_snapshots — актуальное состояние лотов
    # -----------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lot_snapshots (
            item_id       TEXT    NOT NULL,
            qlt           INTEGER NOT NULL DEFAULT -1,
            ptn           INTEGER NOT NULL DEFAULT -1,
            upgrade_level INTEGER NOT NULL DEFAULT -1,

            total_lots    INTEGER NOT NULL DEFAULT 0,
            total_amount  INTEGER NOT NULL DEFAULT 0,
            single_lots   INTEGER NOT NULL DEFAULT 0,
            single_amount INTEGER NOT NULL DEFAULT 0,
            bulk_lots     INTEGER NOT NULL DEFAULT 0,
            bulk_amount   INTEGER NOT NULL DEFAULT 0,

            min_price_pu  REAL,
            avg_price_pu  REAL,

            updated_at    INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (item_id, qlt, ptn, upgrade_level)
        )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_analytics_item      ON analytics_summary (item_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_analytics_item_gran ON analytics_summary (item_id, granularity)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_analytics_slice     ON analytics_summary (item_id, granularity, bucket_key)")
    # Индекс для UI: артефакты (ptn != -1) — наиболее частый запрос «лучшего среза»
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_analytics_ui_best
        ON analytics_summary (item_id, granularity, qlt, ptn, upgrade_level)
        WHERE ptn != -1
    """)

    # -----------------------------------------------------------------------
    # analytics_baselines — медианный объём по категориям
    # -----------------------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics_baselines (
            granularity   TEXT    NOT NULL,
            bucket_key    TEXT    NOT NULL,
            category      TEXT    NOT NULL,
            median_amount REAL    NOT NULL,
            item_count    INTEGER NOT NULL,
            calculated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (granularity, bucket_key, category)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_baselines_gran_bucket
        ON analytics_baselines (granularity, bucket_key)
    """)

    # Минус-лист: предметы, исключённые из загрузки истории продаж
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ignored_items (
            item_id  TEXT    NOT NULL PRIMARY KEY,
            added_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
        )
    """)

    conn.commit()
    log.info("БД инициализирована.")
