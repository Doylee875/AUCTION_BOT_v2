"""
watched_items.py
================
Слой данных для таблицы watched_items.

Схема (v2):
    watched_items (
        item_id       TEXT    NOT NULL,
        qlt           INTEGER NOT NULL DEFAULT -1,   -- ATTR_SENTINEL = любой
        ptn           INTEGER NOT NULL DEFAULT -1,   -- ATTR_SENTINEL = любой
        upgrade_level INTEGER NOT NULL DEFAULT -1,   -- ATTR_SENTINEL = любой
        added_at      INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
        PRIMARY KEY (item_id, qlt, ptn, upgrade_level)
    )

Sentinel -1 в атрибутах означает «не фильтровать»:
  (item_id, -1, -1, -1)  — отслеживать предмет без фильтра (как раньше)
  (item_id,  3, 13, -1)  — отслеживать только «Редкий +13»

Обратная совместимость:
  load_watched_item_ids() — возвращает set[str], работает как раньше для UI
  save_watched_items()    — сохраняет item_id с sentinel-атрибутами
  load_watched_filters()  — новая функция для fetcher_lots
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Collection


# ---------------------------------------------------------------------------
# Датакласс фильтра
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WatchFilter:
    """
    Запись об отслеживаемом предмете с опциональным фильтром по атрибутам.

    Sentinel -1 (ATTR_SENTINEL) в любом поле означает «не фильтровать».
    Например:
        WatchFilter("arm_artifact_42", qlt=3, ptn=13)   → только Редкий +13
        WatchFilter("arm_artifact_42")                   → любой вариант
    """
    item_id:       str
    qlt:           int = -1
    ptn:           int = -1
    upgrade_level: int = -1
    added_at:      int | None = None

    @property
    def has_attr_filter(self) -> bool:
        """True если хотя бы один атрибут задан явно (не sentinel)."""
        return self.qlt != -1 or self.ptn != -1 or self.upgrade_level != -1

    def matches_lot(self, lot_qlt: int | None, lot_ptn: int | None, lot_ul: int | None) -> bool:
        """
        Проверяет, подходит ли лот с заданными атрибутами под этот фильтр.
        Sentinel -1 в фильтре = совпадает с любым значением лота.
        """
        if self.qlt != -1 and lot_qlt != self.qlt:
            return False
        if self.ptn != -1 and lot_ptn != self.ptn:
            return False
        if self.upgrade_level != -1 and lot_ul != self.upgrade_level:
            return False
        return True


# ---------------------------------------------------------------------------
# Запись
# ---------------------------------------------------------------------------

def save_watched_items(
    conn: sqlite3.Connection,
    item_ids: Collection[str],
) -> tuple[int, int]:
    """
    Синхронизирует watched_items с переданным набором item_ids.
    Все записи сохраняются с sentinel-атрибутами (-1,-1,-1).

    Добавляет отсутствующие, удаляет лишние.
    Не трогает added_at у существующих записей.

    Returns:
        (saved, removed)
    """
    desired = set(item_ids)
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT item_id FROM watched_items WHERE qlt=-1 AND ptn=-1 AND upgrade_level=-1"
        ).fetchall()
    }

    to_add    = desired - existing
    to_remove = existing - desired

    cur = conn.cursor()
    if to_add:
        cur.executemany(
            "INSERT OR IGNORE INTO watched_items (item_id) VALUES (?)",
            [(iid,) for iid in to_add],
        )
    if to_remove:
        cur.executemany(
            "DELETE FROM watched_items WHERE item_id=? AND qlt=-1 AND ptn=-1 AND upgrade_level=-1",
            [(iid,) for iid in to_remove],
        )
    conn.commit()
    return len(to_add), len(to_remove)


def add_watched_item(
    conn: sqlite3.Connection,
    item_id: str,
    qlt: int = -1,
    ptn: int = -1,
    upgrade_level: int = -1,
) -> bool:
    """
    Добавляет запись отслеживания с опциональным фильтром атрибутов.

    Returns:
        True если запись добавлена, False если уже существовала.
    """
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO watched_items (item_id, qlt, ptn, upgrade_level)
        VALUES (?, ?, ?, ?)
        """,
        (item_id, qlt, ptn, upgrade_level),
    )
    conn.commit()
    return cur.rowcount > 0


def remove_watched_item(
    conn: sqlite3.Connection,
    item_id: str,
    qlt: int = -1,
    ptn: int = -1,
    upgrade_level: int = -1,
) -> bool:
    """
    Удаляет конкретную запись отслеживания (по полному PK).

    Returns:
        True если запись удалена, False если не существовала.
    """
    cur = conn.execute(
        "DELETE FROM watched_items WHERE item_id=? AND qlt=? AND ptn=? AND upgrade_level=?",
        (item_id, qlt, ptn, upgrade_level),
    )
    conn.commit()
    return cur.rowcount > 0


def remove_watched_item_all(conn: sqlite3.Connection, item_id: str) -> int:
    """
    Удаляет все записи отслеживания для item_id (все варианты атрибутов).

    Returns:
        Количество удалённых записей.
    """
    cur = conn.execute("DELETE FROM watched_items WHERE item_id=?", (item_id,))
    conn.commit()
    return cur.rowcount


# ---------------------------------------------------------------------------
# Чтение
# ---------------------------------------------------------------------------

def load_watched_item_ids(conn: sqlite3.Connection) -> set[str]:
    """
    Возвращает множество всех отслеживаемых item_id.
    Обратная совместимость: UI использует эту функцию для отображения чекбоксов.
    """
    rows = conn.execute("SELECT DISTINCT item_id FROM watched_items").fetchall()
    return {row[0] for row in rows}


def load_watched_filters(conn: sqlite3.Connection) -> list[WatchFilter]:
    """
    Возвращает все записи отслеживания как список WatchFilter.
    Используется fetcher_lots для сопоставления лотов с фильтрами.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT item_id, qlt, ptn, upgrade_level, added_at FROM watched_items ORDER BY added_at DESC"
    ).fetchall()
    conn.row_factory = None
    return [
        WatchFilter(
            item_id=r["item_id"],
            qlt=r["qlt"],
            ptn=r["ptn"],
            upgrade_level=r["upgrade_level"],
            added_at=r["added_at"],
        )
        for r in rows
    ]


def load_watched_items(conn: sqlite3.Connection) -> list[dict]:
    """
    Возвращает полные строки watched_items как список словарей.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT item_id, qlt, ptn, upgrade_level, added_at FROM watched_items ORDER BY added_at DESC"
    ).fetchall()
    conn.row_factory = None
    return [dict(row) for row in rows]


def is_watched(
    conn: sqlite3.Connection,
    item_id: str,
    qlt: int = -1,
    ptn: int = -1,
    upgrade_level: int = -1,
) -> bool:
    """Проверяет, отслеживается ли конкретный предмет с заданными атрибутами."""
    row = conn.execute(
        "SELECT 1 FROM watched_items WHERE item_id=? AND qlt=? AND ptn=? AND upgrade_level=?",
        (item_id, qlt, ptn, upgrade_level),
    ).fetchone()
    return row is not None


def count_watched(conn: sqlite3.Connection) -> int:
    """Возвращает общее количество записей отслеживания."""
    row = conn.execute("SELECT COUNT(*) FROM watched_items").fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Минус-лист (ignored_items)
# ---------------------------------------------------------------------------

def add_ignored(conn: sqlite3.Connection, item_id: str) -> bool:
    """Добавляет предмет в минус-лист. True если добавлен, False если уже был."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO ignored_items (item_id) VALUES (?)", (item_id,)
    )
    conn.commit()
    return cur.rowcount > 0


def remove_ignored(conn: sqlite3.Connection, item_id: str) -> bool:
    """Убирает предмет из минус-листа. True если удалён, False если не было."""
    cur = conn.execute("DELETE FROM ignored_items WHERE item_id=?", (item_id,))
    conn.commit()
    return cur.rowcount > 0


def load_ignored_ids(conn: sqlite3.Connection) -> set[str]:
    """Возвращает множество всех item_id из минус-листа."""
    return {row[0] for row in conn.execute("SELECT item_id FROM ignored_items").fetchall()}
