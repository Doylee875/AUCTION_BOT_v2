"""
db/catalog.py
=============
Операции с каталогом предметов: вставка, обновление, история, синхронизация.

Определяет attr_type предмета из raw_json при upsert — один раз, на уровне
каталога, чтобы все остальные слои не повторяли эту логику.
"""

import json
import sqlite3

import api.utils.logger

log = api.utils.logger.get_logger(__name__)

INSERTED  = "inserted"
UPDATED   = "updated"
UNCHANGED = "unchanged"


# ---------------------------------------------------------------------------
# Определение типа атрибутов из raw_json
# ---------------------------------------------------------------------------

# def detect_attr_type(category: str, subcategory: str, item_id: str = "") -> str:
#     """
#     Определяет тип атрибутов предмета.
#     TODO:Подлежит переработке.
#     """
#     from db.domain_rules import ATTR_TYPE_ITEM_LISTS, ATTR_TYPE_RULES
#     if item_id:
#         for attr_type, item_ids in ATTR_TYPE_ITEM_LISTS.items():
#             if item_id in item_ids:
#                 return attr_type
#     for rule_cat, rule_sub, attr_type in ATTR_TYPE_RULES:
#         if category == rule_cat and (rule_sub == "" or subcategory == rule_sub):
#             return attr_type
#     return ATTR_TYPE_NONE


def _extract_names(item_data: dict) -> tuple[str, str]:
    """Извлекает (name_ru, name_en) из вложенной структуры JSON предмета."""
    name_obj = item_data.get("name", {})
    if name_obj.get("type") == "translation":
        lines = name_obj.get("lines", {})
        return lines.get("ru", ""), lines.get("en", "")
    if name_obj.get("type") == "text":
        text = name_obj.get("text", "")
        return text, text
    return "", ""


# ---------------------------------------------------------------------------
# Вставка / обновление предмета
# ---------------------------------------------------------------------------

def upsert_item(
    conn: sqlite3.Connection,
    category: str,
    subcategory: str,
    item_data: dict,
) -> str:
    """
    Вставляет или обновляет запись предмета в БД.

    При изменении raw_json копирует старую версию в items_history.
    Определяет attr_type из item_data и сохраняет его в items.

    Returns:
        INSERTED | UPDATED | UNCHANGED
    """
    item_id          = item_data.get("id", "")
    color            = item_data.get("color", "")
    name_ru, name_en = _extract_names(item_data)
    raw_json_str     = json.dumps(item_data, ensure_ascii=False)
    icon_path        = item_data.get("icon_path", "")

    cur = conn.cursor()

    existing = cur.execute(
        "SELECT raw_json FROM items WHERE item_id = ?",
        (item_id,),
    ).fetchone()

    if existing is None:
        cur.execute("""
            INSERT INTO items
                (item_id, category, subcategory,
                 color, name_ru, name_en, raw_json, icon_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (item_id, category, subcategory,
              color, name_ru, name_en, raw_json_str, icon_path))
        status = INSERTED

    elif existing[0] != raw_json_str:
        cur.execute("""
            INSERT INTO items_history
                (item_id, category, subcategory, attr_type,
                 color, name_ru, name_en, raw_json, icon_path, updated_at)
            SELECT item_id, category, subcategory, attr_type,
                   color, name_ru, name_en, raw_json, icon_path, updated_at
            FROM items
            WHERE item_id = ?
        """, (item_id,))

        cur.execute("""
            UPDATE items SET
                category    = ?,
                subcategory = ?,
                color       = ?,
                name_ru     = ?,
                name_en     = ?,
                raw_json    = ?,
                icon_path   = ?,
                updated_at  = strftime('%s', 'now')
            WHERE item_id = ?
        """, (category, subcategory,
              color, name_ru, name_en, raw_json_str, icon_path,
              item_id))
        status = UPDATED

    # else:
    #     # raw_json не изменился, attr_type пересчитывается (производный от category/subcategory)
    #     cur.execute(
    #         "UPDATE items SET attr_type = ? WHERE item_id = ?",
    #         (attr_type, item_id),
    #     )
    #     status = UNCHANGED

    for lang, name in (("ru", name_ru), ("en", name_en)):
        if name:
            cur.execute("""
                INSERT INTO items_names (item_id, lang, name) VALUES (?, ?, ?)
                ON CONFLICT (item_id, lang) DO UPDATE SET name = excluded.name
            """, (item_id, lang, name))

    return status


# ---------------------------------------------------------------------------
# Лог синхронизации
# ---------------------------------------------------------------------------

def log_sync(conn: sqlite3.Connection, items_inserted: int, items_updated: int) -> None:
    """Записывает итоги одного прогона синхронизации каталога."""
    conn.execute("""
        INSERT INTO sync_log (run_at, items_inserted, items_updated)
        VALUES (strftime('%s', 'now'), ?, ?)
    """, (items_inserted, items_updated))
    conn.commit()
    log.info("Sync log: +%d новых, ~%d обновлённых.", items_inserted, items_updated)
