"""
ui_item_selector.py
===================
UI выбора предметов для отслеживания.

Поддерживает все типы предметов:
  - Обычные предметы: метрики из analytics_summary (qlt=ptn=ul=-1)
  - Артефакты: строка в списке — qlt-агрегат лучшего qlt (ptn=-1);
    при раскрытии (ПКМ) — таблица точных срезов (qlt, ptn)
  - qlt_only / upgrade: аналогично артефактам, один уровень

Структура окна:
    ┌──────────────────────────────────────────────────────┐
    │  Toolbar: поиск, сортировка, кнопки выбора           │
    ├──────────────────┬───────────────────────────────────┤
    │  Дерево категорий│  Таблица предметов (справа)       │
    │                  │  Для артефактов — раскрываемые    │
    │                  │  строки qlt → ptn (по ПКМ)        │
    └──────────────────┴───────────────────────────────────┘
    │  Статусбар                                           │
    └──────────────────────────────────────────────────────┘

Фильтрация предметов:
  - fetch_total = 0 → не показывается (ни одной продажи в API)
  - last_sale_at < 30 дней назад → помечается как «неактивный»,
    всегда в конце списка, отображается серым
  - Категории без видимых предметов не показываются
"""

from __future__ import annotations

import argparse
import sqlite3
import tkinter as tk
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from tkinter import messagebox, ttk

import api.utils.logger
from analytics.metrics import get_best_slice
from config import settings
from db.connection import get_connection, open_connection
from schema import ATTR_SENTINEL, ATTR_TYPE_NONE, ATTR_TYPE_ARTIFACT, init_db
from watched_items import (
    WatchFilter,
    add_watched_item,
    load_watched_item_ids,
    save_watched_items,
)

log = api.utils.logger.get_logger(__name__)

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

DEFAULT_GRANULARITY = "weekly"

# Предмет считается «неактивным» если последняя продажа была старше этого порога
INACTIVE_THRESHOLD = timedelta(days=30)

SORT_METRICS: list[dict] = [
    {"key": "name_ru",       "label": "Имя (А–Я)",       "reverse": False},
    {"key": "liquidity",     "label": "Ликвидность ↓",    "reverse": True},
    {"key": "sales_per_day", "label": "Продаж/день ↓",    "reverse": True},
    {"key": "avg_price",     "label": "Ср. цена ↓",       "reverse": True},
    {"key": "volatility",    "label": "Волатильность ↓",  "reverse": True},
    {"key": "trend",         "label": "Тренд ↓",          "reverse": True},
]

# Колонки таблицы: (column_id, заголовок, минимальная ширина px)
TABLE_COLUMNS: list[tuple[str, str, int]] = [
    ("name_ru",       "Название",       220),
    ("color",         "Цвет",            70),
    ("best_slice",    "Лучший срез",    110),
    ("avg_price",     "Ср. цена",       110),
    ("liquidity",     "Ликвидность",    100),
    ("sales_per_day", "Продаж/нед.",    100),
    ("volatility",    "Волатильность",  100),
    ("trend",         "Тренд",           80),
]

WINDOW_WIDTH  = 1200
WINDOW_HEIGHT = 740

# Контекстное меню — действия ПКМ
_CTX_EXPAND   = "expand"
_CTX_COLLAPSE = "collapse"


# ---------------------------------------------------------------------------
# Слой данных
# ---------------------------------------------------------------------------

@dataclass
class ItemRow:
    """Предмет для отображения в основном списке."""
    item_id:     str
    realm:       str
    category:    str
    subcategory: str
    name_ru:     str
    name_en:     str
    color:       str
    attr_type:   str = ATTR_TYPE_NONE

    # Временные метаданные
    last_sale_at: int | None = None   # unix timestamp последней продажи

    # Метрики «лучшего среза»
    liquidity:     float | None = None
    sales_per_day: float | None = None
    avg_price:     float | None = None
    volatility:    float | None = None
    trend:         float | None = None

    # Атрибуты лучшего среза (None для обычных предметов)
    best_qlt:           int | None = None
    best_ptn:           int | None = None
    best_upgrade_level: int | None = None

    @property
    def is_artifact(self) -> bool:
        return self.attr_type != ATTR_TYPE_NONE

    # Флаг ненадёжной аналитики (< MIN_RELIABLE_SALES продаж в срезе)
    low_sample: bool = False

    @property
    def is_inactive(self) -> bool:
        """True если предмет не торговался дольше INACTIVE_THRESHOLD."""
        if self.last_sale_at is None:
            return True
        last_dt = datetime.fromtimestamp(self.last_sale_at, tz=timezone.utc)
        return (datetime.now(timezone.utc) - last_dt) > INACTIVE_THRESHOLD


def _fmt_price(val: float | None) -> str:
    if val is None:
        return "—"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M"
    if val >= 1_000:
        return f"{val / 1_000:.0f}K"
    return str(int(val))


def _fmt_float(val: float | None, precision: int = 2) -> str:
    return f"{val:.{precision}f}" if val is not None else "—"


def _fmt_best_slice(item: ItemRow) -> str:
    """Читаемое обозначение лучшего среза для артефакта."""
    if not item.is_artifact:
        return "—"
    if item.best_qlt is not None and item.best_qlt != ATTR_SENTINEL:
        qlt_labels = {0: "Обыч", 1: "Необыч", 2: "Особый", 3: "Редкий", 4: "Искл.", 5: "Легенд."}
        return qlt_labels.get(item.best_qlt, f"q{item.best_qlt}")
    if item.best_upgrade_level is not None and item.best_upgrade_level != ATTR_SENTINEL:
        return f"+{item.best_upgrade_level}"
    return "—"


def load_items_with_metrics(conn: sqlite3.Connection, realm: str) -> list[ItemRow]:
    """
    Загружает предметы realm + метрики лучшего среза.

    Исключает предметы с fetch_total = 0 (ни одной продажи в API).
    Для обычных предметов читает аналитику из analytics_summary (qlt=ptn=ul=-1).
    Для артефактов вызывает get_best_slice() — qlt-агрегат с max sales_per_day.
    """
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        f"""
        SELECT
            i.item_id,
            i.realm,
            i.category,
            i.subcategory,
            i.name_ru,
            i.name_en,
            i.color,
            i.attr_type,
            i.last_sale_at,
            AVG(a.liquidity)      AS liquidity,
            AVG(a.sales_per_day)  AS sales_per_day,
            AVG(a.avg_price)      AS avg_price,
            AVG(a.volatility)     AS volatility,
            AVG(a.trend)          AS trend
        FROM items i
        LEFT JOIN analytics_summary a
               ON a.item_id       = i.item_id
              AND a.granularity   = :gran
              AND a.qlt           = {ATTR_SENTINEL}
              AND a.ptn           = {ATTR_SENTINEL}
              AND a.upgrade_level = {ATTR_SENTINEL}
              AND a.low_sample    = 0
        WHERE LOWER(i.realm) = LOWER(:realm)
          AND i.fetch_total > 0
        GROUP BY i.item_id
        ORDER BY i.category, i.subcategory, i.name_ru
        """,
        {"realm": realm, "gran": DEFAULT_GRANULARITY},
    ).fetchall()

    items: list[ItemRow] = []
    for r in rows:
        attr_type = r["attr_type"] if r["attr_type"] else ATTR_TYPE_NONE

        if attr_type != ATTR_TYPE_NONE:
            best = get_best_slice(conn, r["item_id"], granularity=DEFAULT_GRANULARITY)
            if best:
                bq  = best.get("qlt",           ATTR_SENTINEL)
                bp  = best.get("ptn",            ATTR_SENTINEL)
                bul = best.get("upgrade_level",  ATTR_SENTINEL)
                item = ItemRow(
                    item_id            = r["item_id"],
                    realm              = r["realm"],
                    category           = r["category"],
                    subcategory        = r["subcategory"],
                    name_ru            = r["name_ru"] or r["item_id"],
                    name_en            = r["name_en"] or "",
                    color              = r["color"]   or "",
                    attr_type          = attr_type,
                    last_sale_at       = r["last_sale_at"],
                    liquidity          = best.get("liquidity"),
                    sales_per_day      = best.get("sales_per_day"),
                    avg_price          = best.get("avg_price"),
                    volatility         = best.get("volatility"),
                    trend              = best.get("trend"),
                    best_qlt           = bq  if bq  != ATTR_SENTINEL else None,
                    best_ptn           = bp  if bp  != ATTR_SENTINEL else None,
                    best_upgrade_level = bul if bul != ATTR_SENTINEL else None,
                )
            else:
                # Аналитики нет, но тип предмета сохраняем —
                # иначе is_artifact вернёт False и ПКМ не покажет раскрытие
                item = ItemRow(
                    item_id      = r["item_id"],
                    realm        = r["realm"],
                    category     = r["category"],
                    subcategory  = r["subcategory"],
                    name_ru      = r["name_ru"] or r["item_id"],
                    name_en      = r["name_en"] or "",
                    color        = r["color"]   or "",
                    attr_type    = attr_type,
                    last_sale_at = r["last_sale_at"],
                )
        else:
            item = ItemRow(
                item_id       = r["item_id"],
                realm         = r["realm"],
                category      = r["category"],
                subcategory   = r["subcategory"],
                name_ru       = r["name_ru"] or r["item_id"],
                name_en       = r["name_en"] or "",
                color         = r["color"]   or "",
                attr_type     = attr_type,
                last_sale_at  = r["last_sale_at"],
                liquidity     = r["liquidity"],
                sales_per_day = r["sales_per_day"],
                avg_price     = r["avg_price"],
                volatility    = r["volatility"],
                trend         = r["trend"],
            )
        items.append(item)

    return items


def load_artifact_detail(
    conn: sqlite3.Connection,
    item_id: str,
    granularity: str = DEFAULT_GRANULARITY,
) -> list[dict]:
    """
    Загружает точные срезы (qlt, ptn) для артефакта из analytics_summary.

    Возвращает только строки уровня 1 (ptn != ATTR_SENTINEL),
    отсортированные по qlt DESC, ptn ASC.
    """
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        f"""
        SELECT
            qlt, ptn,
            AVG(liquidity)      AS liquidity,
            AVG(sales_per_day)  AS sales_per_day,
            AVG(avg_price)      AS avg_price,
            AVG(volatility)     AS volatility,
            AVG(trend)          AS trend,
            SUM(total_amount)   AS total_amount
        FROM analytics_summary
        WHERE item_id    = :item_id
          AND granularity = :gran
          AND ptn        != {ATTR_SENTINEL}
          AND upgrade_level = {ATTR_SENTINEL}
          AND avg_price IS NOT NULL
        GROUP BY qlt, ptn
        ORDER BY qlt DESC, ptn ASC
        """,
        {"item_id": item_id, "gran": granularity},
    ).fetchall()
    return [dict(r) for r in rows]


def group_by_category(items: list[ItemRow]) -> dict[str, dict[str, list[ItemRow]]]:
    """Группирует предметы по категории/подкатегории. Пустые категории не включаются."""
    result: dict[str, dict[str, list[ItemRow]]] = {}
    for item in items:
        cat = item.category    or "Без категории"
        sub = item.subcategory or ""
        result.setdefault(cat, {}).setdefault(sub, []).append(item)
    return result


def is_watched_filter(
    conn: sqlite3.Connection | None,
    item_id: str,
    qlt: int = -1,
    ptn: int = -1,
    upgrade_level: int = -1,
) -> bool:
    """Проверяет, отслеживается ли конкретный срез предмета."""
    if conn is None:
        return False
    row = conn.execute(
        "SELECT 1 FROM watched_items WHERE item_id=? AND qlt=? AND ptn=? AND upgrade_level=?",
        (item_id, qlt, ptn, upgrade_level),
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Главное окно
# ---------------------------------------------------------------------------

class ItemSelectorApp:

    def __init__(self, root: tk.Tk, db_path: str, realm: str) -> None:
        self.root    = root
        self.db_path = db_path
        self.realm   = realm

        self.root.title(f"STALCRAFT — Выбор предметов  [{realm}]")
        self.root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
        self.root.minsize(800, 500)

        self._all_items:    list[ItemRow]    = []
        self._grouped:      dict             = {}
        self._checked:      dict[str, bool]  = {}
        self._current_cat:  str | None       = None
        self._current_sub:  str | None       = None
        self._sort_key:     str              = SORT_METRICS[0]["key"]
        self._sort_reverse: bool             = SORT_METRICS[0]["reverse"]
        self._filter_text:  str              = ""
        self._db_conn:      sqlite3.Connection | None = None

        # Множество item_id с раскрытыми срезами
        self._expanded: set[str] = set()

        self._build_ui()
        self._load_data()

    # -----------------------------------------------------------------------
    # UI
    # -----------------------------------------------------------------------

    def _build_ui(self) -> None:
        self._build_toolbar()
        self._build_main_pane()
        self._build_statusbar()

    def _build_toolbar(self) -> None:
        tb = tk.Frame(self.root, bd=1, relief=tk.RAISED, pady=4, padx=6)
        tb.pack(side=tk.TOP, fill=tk.X)

        tk.Label(tb, text="Поиск:").pack(side=tk.LEFT)
        self._search_var = tk.StringVar()
        self._search_var.trace_add("write", lambda *_: self._on_filter_change())
        tk.Entry(tb, textvariable=self._search_var, width=22).pack(side=tk.LEFT, padx=(2, 10))

        tk.Label(tb, text="Сортировка:").pack(side=tk.LEFT)
        self._sort_var = tk.StringVar(value=SORT_METRICS[0]["label"])
        sort_cb = ttk.Combobox(
            tb, textvariable=self._sort_var,
            values=[m["label"] for m in SORT_METRICS],
            state="readonly", width=20,
        )
        sort_cb.pack(side=tk.LEFT, padx=(2, 10))
        sort_cb.bind("<<ComboboxSelected>>", lambda _: self._on_sort_change())

        tk.Button(tb, text="Выбрать всё", command=self._select_all).pack(side=tk.LEFT, padx=2)
        tk.Button(tb, text="Снять всё",   command=self._deselect_all).pack(side=tk.LEFT, padx=2)

        tk.Button(
            tb, text="💾 Сохранить",
            command=self._save_selection,
            bg="#4CAF50", fg="white", font=("", 9, "bold"),
        ).pack(side=tk.RIGHT, padx=6)

        self._selected_count_var = tk.StringVar(value="Выбрано: 0")
        tk.Label(tb, textvariable=self._selected_count_var, fg="#555").pack(side=tk.RIGHT, padx=10)

    def _build_main_pane(self) -> None:
        pane = tk.PanedWindow(self.root, orient=tk.HORIZONTAL, sashwidth=5, sashrelief=tk.RAISED)
        pane.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        left = tk.Frame(pane, width=220)
        pane.add(left, minsize=160)
        self._build_category_tree(left)

        right = tk.Frame(pane)
        pane.add(right, minsize=500)
        self._build_items_table(right)

    def _build_category_tree(self, parent: tk.Frame) -> None:
        tk.Label(parent, text="Категории", font=("", 9, "bold")).pack(anchor=tk.W, padx=4, pady=(4, 0))
        frame = tk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True)
        vsb = ttk.Scrollbar(frame, orient=tk.VERTICAL)
        self._cat_tree = ttk.Treeview(
            frame, selectmode="browse",
            yscrollcommand=vsb.set, show="tree",
        )
        vsb.config(command=self._cat_tree.yview)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self._cat_tree.pack(fill=tk.BOTH, expand=True)
        self._cat_tree.bind("<<TreeviewSelect>>", self._on_category_select)

    def _build_items_table(self, parent: tk.Frame) -> None:
        tk.Label(parent, text="Предметы", font=("", 9, "bold")).pack(anchor=tk.W, padx=4, pady=(4, 0))

        # Легенда активности
        legend = tk.Frame(parent)
        legend.pack(anchor=tk.W, padx=4, pady=(0, 2))
        tk.Label(legend, text="● активный", fg="#1a1a1a", font=("", 8)).pack(side=tk.LEFT, padx=(0, 8))
        tk.Label(legend, text="● неактивный (>30 дней без продаж)", fg="#aaaaaa", font=("", 8)).pack(side=tk.LEFT, padx=(0, 8))
        tk.Label(legend, text="italic = мало данных (<5 продаж)", fg="#555555", font=("", 8, "italic")).pack(side=tk.LEFT)

        frame = tk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True)

        columns = ["check"] + [col_id for col_id, _, _ in TABLE_COLUMNS]
        self._items_tree = ttk.Treeview(
            frame, columns=columns, show="headings", selectmode="extended",
        )
        self._items_tree.heading("check", text="✓", command=self._toggle_visible)
        self._items_tree.column("check", width=30, minwidth=30, stretch=False, anchor=tk.CENTER)

        for col_id, header, min_w in TABLE_COLUMNS:
            self._items_tree.heading(
                col_id, text=header,
                command=lambda c=col_id: self._sort_by_column(c),
            )
            self._items_tree.column(col_id, width=min_w, minwidth=min_w)

        vsb = ttk.Scrollbar(frame, orient=tk.VERTICAL,   command=self._items_tree.yview)
        hsb = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=self._items_tree.xview)
        self._items_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT,  fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self._items_tree.pack(fill=tk.BOTH, expand=True)

        # Теги цветов редкости — активные предметы
        self._items_tree.tag_configure("common",    background="#F5F5F5")
        self._items_tree.tag_configure("uncommon",  background="#DFF5D1")
        self._items_tree.tag_configure("rare",      background="#D1E8FF")
        self._items_tree.tag_configure("epic",      background="#ECD9FF")
        self._items_tree.tag_configure("legendary", background="#FFE8B0")
        self._items_tree.tag_configure("mythical",  background="#FFDADA")
        # Неактивные предметы — серый текст поверх цвета редкости
        self._items_tree.tag_configure("inactive",   foreground="#aaaaaa")
        # Ненадёжная аналитика (< 5 продаж в срезе) — курсив
        self._items_tree.tag_configure("low_sample", font=("", 9, "italic"))
        # Чекбокс выбранных
        self._items_tree.tag_configure("checked",   font=("", 9, "bold"))
        # Строки-детали артефактов (развёрнутые срезы)
        self._items_tree.tag_configure("detail",    foreground="#888888")

        # Одиночный клик — только чекбокс
        self._items_tree.bind("<ButtonRelease-1>", self._on_item_click)
        # ПКМ — контекстное меню (раскрытие срезов артефактов)
        self._items_tree.bind("<Button-3>", self._on_right_click)

        # Контекстное меню
        self._ctx_menu = tk.Menu(self.root, tearoff=0)

    def _build_statusbar(self) -> None:
        self._status_var = tk.StringVar(value="Загрузка...")
        tk.Label(
            self.root, textvariable=self._status_var,
            bd=1, relief=tk.SUNKEN, anchor=tk.W, padx=4,
        ).pack(side=tk.BOTTOM, fill=tk.X)

    # -----------------------------------------------------------------------
    # Данные
    # -----------------------------------------------------------------------

    def _load_data(self) -> None:
        self._status_var.set("Загрузка предметов из БД…")
        self.root.update_idletasks()
        try:
            self._db_conn = open_connection(self.db_path)
            init_db(self._db_conn)
            self._all_items = load_items_with_metrics(self._db_conn, self.realm)
            watched_ids     = load_watched_item_ids(self._db_conn)
        except sqlite3.OperationalError as e:
            messagebox.showerror("Ошибка БД", str(e))
            self._status_var.set(f"Ошибка: {e}")
            return

        self._checked = {item.item_id: (item.item_id in watched_ids) for item in self._all_items}
        self._grouped = group_by_category(self._all_items)
        self._populate_category_tree()
        self._refresh_selected_count()

        total    = len(self._all_items)
        inactive = sum(1 for i in self._all_items if i.is_inactive)
        self._status_var.set(
            f"Загружено {total} предметов в {len(self._grouped)} категориях "
            f"({inactive} неактивных)."
        )

    # -----------------------------------------------------------------------
    # Дерево категорий — только непустые категории
    # -----------------------------------------------------------------------

    def _populate_category_tree(self) -> None:
        self._cat_tree.delete(*self._cat_tree.get_children())
        for cat, subcats in self._grouped.items():
            # Считаем только предметы, которые реально видны (fetch_total > 0
            # уже отфильтрован на уровне БД, grouped содержит только их)
            cat_checked = sum(
                1 for sub_items in subcats.values()
                for item in sub_items if self._checked.get(item.item_id)
            )
            cat_total = sum(len(v) for v in subcats.values())
            cat_node  = self._cat_tree.insert(
                "", tk.END, iid=f"cat:{cat}",
                text=f"{cat}  ({cat_checked}/{cat_total})", open=False,
            )
            if list(subcats.keys()) == [""]:
                self._cat_tree.insert(cat_node, tk.END, iid=f"sub:{cat}:", text="")
            else:
                for sub, sub_items in subcats.items():
                    sub_checked = sum(1 for i in sub_items if self._checked.get(i.item_id))
                    sub_label   = f"  {sub}  ({sub_checked}/{len(sub_items)})" if sub \
                                  else f"  —  ({sub_checked}/{len(sub_items)})"
                    self._cat_tree.insert(cat_node, tk.END, iid=f"sub:{cat}:{sub}", text=sub_label)

        children = self._cat_tree.get_children()
        if children:
            first = self._cat_tree.get_children(children[0])
            sel   = first[0] if first else children[0]
            self._cat_tree.selection_set(sel)
            self._cat_tree.see(sel)

    def _on_category_select(self, _event: tk.Event) -> None:
        sel = self._cat_tree.selection()
        if not sel:
            return
        node_id = sel[0]
        if node_id.startswith("sub:"):
            parts = node_id[4:].split(":", 1)
            self._current_cat = parts[0]
            self._current_sub = parts[1] if len(parts) > 1 else None
        else:
            self._current_cat = node_id[4:]
            self._current_sub = None
        self._expanded.clear()
        self._refresh_items_table()

    # -----------------------------------------------------------------------
    # Таблица предметов
    # -----------------------------------------------------------------------

    def _get_visible_items(self) -> list[ItemRow]:
        if self._current_cat is None:
            return []
        items: list[ItemRow] = []
        subcats = self._grouped.get(self._current_cat, {})
        if self._current_sub is not None:
            items = subcats.get(self._current_sub, [])
        else:
            for sub_items in subcats.values():
                items.extend(sub_items)

        q = self._filter_text.lower()
        if q:
            items = [
                i for i in items
                if q in i.name_ru.lower() or q in i.name_en.lower() or q in i.item_id.lower()
            ]

        def sort_key(item: ItemRow):
            # Неактивные всегда в конце
            inactive_flag = 1 if item.is_inactive else 0
            val = getattr(item, self._sort_key, None)
            if val is None:
                metric = (1, 0)
            elif self._sort_reverse and isinstance(val, (int, float)):
                metric = (0, -val)
            else:
                metric = (0, val)
            return (inactive_flag,) + metric

        return sorted(items, key=sort_key)

    def _item_values(self, item: ItemRow, check_mark: str) -> list:
        return [
            check_mark,
            item.name_ru,
            item.color or "",
            _fmt_best_slice(item),
            _fmt_price(item.avg_price),
            _fmt_float(item.liquidity),
            _fmt_float(item.sales_per_day, 1),
            _fmt_float(item.volatility),
            _fmt_float(item.trend),
        ]

    def _item_tags(self, item: ItemRow, checked: bool) -> list[str]:
        tags = [(item.color or "common").lower()]
        if checked:
            tags.append("checked")
        if item.is_inactive:
            tags.append("inactive")
        if item.low_sample:
            tags.append("low_sample")
        return tags

    def _refresh_items_table(self) -> None:
        self._items_tree.delete(*self._items_tree.get_children())
        items = self._get_visible_items()

        for item in items:
            checked    = self._checked.get(item.item_id, False)
            check_mark = "☑" if checked else "☐"
            tags       = self._item_tags(item, checked)

            self._items_tree.insert(
                "", tk.END, iid=item.item_id,
                values=self._item_values(item, check_mark),
                tags=tags,
            )

            # Если предмет был раскрыт — восстанавливаем срезы
            if item.item_id in self._expanded and item.is_artifact:
                self._insert_detail_rows(item)

    # -----------------------------------------------------------------------
    # Одиночный клик — только чекбокс
    # -----------------------------------------------------------------------

    def _on_item_click(self, event: tk.Event) -> None:
        region = self._items_tree.identify_region(event.x, event.y)
        if region not in ("cell", "tree"):
            return
        row_id = self._items_tree.identify_row(event.y)
        if not row_id or row_id.startswith("detail:"):
            return

        self._checked[row_id] = not self._checked.get(row_id, False)
        checked    = self._checked[row_id]
        check_mark = "☑" if checked else "☐"

        vals    = list(self._items_tree.item(row_id, "values"))
        vals[0] = check_mark
        self._items_tree.item(row_id, values=vals)

        tags = list(self._items_tree.item(row_id, "tags"))
        if checked and "checked" not in tags:
            tags.append("checked")
        elif not checked and "checked" in tags:
            tags.remove("checked")
        self._items_tree.item(row_id, tags=tags)

        self._refresh_selected_count()
        self._update_category_counts()

    # -----------------------------------------------------------------------
    # ПКМ — контекстное меню с раскрытием/сворачиванием срезов артефактов
    # -----------------------------------------------------------------------

    def _on_right_click(self, event: tk.Event) -> None:
        row_id = self._items_tree.identify_row(event.y)
        if not row_id:
            return

        self._ctx_menu.delete(0, tk.END)

        # ПКМ по строке-детали (раскрытый срез артефакта)
        if row_id.startswith("detail:"):
            self._build_ctx_detail(row_id)
        else:
            self._build_ctx_item(row_id)

        try:
            self._ctx_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self._ctx_menu.grab_release()

    def _build_ctx_item(self, row_id: str) -> None:
        """Контекстное меню для основной строки предмета."""
        item = next((i for i in self._all_items if i.item_id == row_id), None)
        if item is None:
            return

        # Раскрытие срезов только для артефактов (qlt + ptn)
        if item.attr_type == ATTR_TYPE_ARTIFACT:
            if row_id in self._expanded:
                self._ctx_menu.add_command(
                    label="▲ Свернуть срезы",
                    command=lambda: self._collapse_artifact(row_id),
                )
            else:
                self._ctx_menu.add_command(
                    label="▼ Раскрыть срезы (редкость × патч)",
                    command=lambda: self._expand_artifact(row_id, item),
                )
            self._ctx_menu.add_separator()

        checked = self._checked.get(row_id, False)
        label   = "☐ Снять выбор" if checked else "☑ Выбрать"
        self._ctx_menu.add_command(
            label=label,
            command=lambda: self._toggle_item(row_id),
        )

    def _build_ctx_detail(self, row_id: str) -> None:
        """
        Контекстное меню для строки-детали (раскрытый срез qlt × ptn).
        row_id формат: "detail:{item_id}:{qlt}_{ptn}"
        """
        # Парсим item_id, qlt, ptn из row_id
        try:
            # row_id = "detail:ITEM_ID:QLT_PTN"
            after_prefix  = row_id[len("detail:"):]
            # item_id может содержать ":", ищем последнее ":"
            last_colon    = after_prefix.rfind(":")
            item_id       = after_prefix[:last_colon]
            qlt_ptn       = after_prefix[last_colon + 1:]
            qlt_str, ptn_str = qlt_ptn.split("_", 1)
            qlt = int(qlt_str)
            ptn = int(ptn_str)
        except (ValueError, IndexError):
            return

        qlt_labels = {0: "Обыч", 1: "Необыч", 2: "Особый", 3: "Редкий", 4: "Искл.", 5: "Легенд."}
        label_str  = f"{qlt_labels.get(qlt, f'q{qlt}')} +{ptn}"
        watched    = is_watched_filter(self._db_conn, item_id, qlt=qlt, ptn=ptn)                      if self._db_conn else False

        if watched:
            self._ctx_menu.add_command(
                label=f"★ Уже отслеживается: {label_str}",
                state=tk.DISABLED,
            )
            self._ctx_menu.add_command(
                label=f"✕ Снять отслеживание {label_str}",
                command=lambda: self._unwatch_slice(item_id, qlt, ptn),
            )
        else:
            self._ctx_menu.add_command(
                label=f"☆ Отслеживать {label_str}",
                command=lambda: self._watch_slice(item_id, qlt, ptn),
            )

    def _expand_artifact(self, row_id: str, item: ItemRow) -> None:
        if self._db_conn is None:
            return
        slices = load_artifact_detail(self._db_conn, row_id)
        if not slices:
            self._status_var.set(f"{item.name_ru}: детальных данных нет.")
            return
        self._expanded.add(row_id)
        self._insert_detail_rows(item, slices)
        self._status_var.set(f"{item.name_ru}: раскрыто {len(slices)} срезов qlt×ptn.")

    def _collapse_artifact(self, row_id: str) -> None:
        self._expanded.discard(row_id)
        detail_prefix = f"detail:{row_id}:"
        for child_id in list(self._items_tree.get_children()):
            if child_id.startswith(detail_prefix):
                self._items_tree.delete(child_id)
        self._status_var.set("Срезы свёрнуты.")

    def _insert_detail_rows(
        self,
        item: ItemRow,
        slices: list[dict] | None = None,
    ) -> None:
        """Вставляет строки-детали сразу после строки предмета."""
        if self._db_conn is None:
            return
        if slices is None:
            slices = load_artifact_detail(self._db_conn, item.item_id)
        if not slices:
            return

        parent_idx    = self._items_tree.index(item.item_id)
        detail_prefix = f"detail:{item.item_id}:"
        qlt_labels    = {0: "Обыч", 1: "Необыч", 2: "Особый", 3: "Редкий", 4: "Искл.", 5: "Легенд."}

        for i, sl in enumerate(slices):
            qlt_str  = qlt_labels.get(sl["qlt"], f"q{sl['qlt']}")
            ptn_str  = f"+{sl['ptn']}"
            slice_id = f"{detail_prefix}{sl['qlt']}_{sl['ptn']}"

            # Не дублируем если уже есть (восстановление после смены категории)
            if self._items_tree.exists(slice_id):
                continue

            values = [
                "",
                f"  └ {qlt_str} {ptn_str}",
                "",
                f"{qlt_str} {ptn_str}",
                _fmt_price(sl.get("avg_price")),
                _fmt_float(sl.get("liquidity")),
                _fmt_float(sl.get("sales_per_day"), 1),
                _fmt_float(sl.get("volatility")),
                _fmt_float(sl.get("trend")),
            ]
            self._items_tree.insert(
                "", parent_idx + 1 + i, iid=slice_id,
                values=values, tags=["detail"],
            )

    # -----------------------------------------------------------------------
    # Отслеживание конкретного среза (qlt × ptn) через ПКМ
    # -----------------------------------------------------------------------

    def _watch_slice(self, item_id: str, qlt: int, ptn: int) -> None:
        """Добавляет срез (item_id, qlt, ptn) в watched_items."""
        if self._db_conn is None:
            return
        added = add_watched_item(self._db_conn, item_id, qlt=qlt, ptn=ptn)
        qlt_labels = {0: "Обыч", 1: "Необыч", 2: "Особый", 3: "Редкий", 4: "Искл.", 5: "Легенд."}
        label = f"{qlt_labels.get(qlt, f'q{qlt}')} +{ptn}"
        if added:
            self._status_var.set(f"Добавлено отслеживание: {label}")
        else:
            self._status_var.set(f"Уже отслеживается: {label}")

    def _unwatch_slice(self, item_id: str, qlt: int, ptn: int) -> None:
        """Удаляет срез (item_id, qlt, ptn) из watched_items."""
        if self._db_conn is None:
            return
        from watched_items import remove_watched_item
        removed = remove_watched_item(self._db_conn, item_id, qlt=qlt, ptn=ptn)
        qlt_labels = {0: "Обыч", 1: "Необыч", 2: "Особый", 3: "Редкий", 4: "Искл.", 5: "Легенд."}
        label = f"{qlt_labels.get(qlt, f'q{qlt}')} +{ptn}"
        if removed:
            self._status_var.set(f"Снято отслеживание: {label}")

    def _toggle_item(self, row_id: str) -> None:
        """Переключить чекбокс через контекстное меню."""
        self._checked[row_id] = not self._checked.get(row_id, False)
        checked    = self._checked[row_id]
        check_mark = "☑" if checked else "☐"
        vals    = list(self._items_tree.item(row_id, "values"))
        vals[0] = check_mark
        self._items_tree.item(row_id, values=vals)
        tags = list(self._items_tree.item(row_id, "tags"))
        if checked and "checked" not in tags:
            tags.append("checked")
        elif not checked and "checked" in tags:
            tags.remove("checked")
        self._items_tree.item(row_id, tags=tags)
        self._refresh_selected_count()
        self._update_category_counts()

    def _sort_by_column(self, col_id: str) -> None:
        metric = next((m for m in SORT_METRICS if m["key"] == col_id), None)
        if metric:
            if self._sort_key == col_id:
                self._sort_reverse = not self._sort_reverse
            else:
                self._sort_key     = col_id
                self._sort_reverse = metric["reverse"]
        self._expanded.clear()
        self._refresh_items_table()

    # -----------------------------------------------------------------------
    # Массовые операции
    # -----------------------------------------------------------------------

    def _toggle_visible(self) -> None:
        visible   = self._get_visible_items()
        all_check = all(self._checked.get(i.item_id, False) for i in visible)
        new_state = not all_check
        for item in visible:
            self._checked[item.item_id] = new_state
        self._refresh_items_table()
        self._refresh_selected_count()
        self._update_category_counts()

    def _select_all(self) -> None:
        for iid in self._checked:
            self._checked[iid] = True
        self._refresh_items_table()
        self._refresh_selected_count()
        self._populate_category_tree()

    def _deselect_all(self) -> None:
        for iid in self._checked:
            self._checked[iid] = False
        self._refresh_items_table()
        self._refresh_selected_count()
        self._populate_category_tree()

    def _on_filter_change(self) -> None:
        self._filter_text = self._search_var.get().strip()
        self._expanded.clear()
        self._refresh_items_table()

    def _on_sort_change(self) -> None:
        label  = self._sort_var.get()
        metric = next((m for m in SORT_METRICS if m["label"] == label), None)
        if metric:
            self._sort_key     = metric["key"]
            self._sort_reverse = metric["reverse"]
        self._refresh_items_table()

    def _refresh_selected_count(self) -> None:
        count = sum(1 for v in self._checked.values() if v)
        self._selected_count_var.set(f"Выбрано: {count}")

    def _update_category_counts(self) -> None:
        for cat, subcats in self._grouped.items():
            cat_checked = sum(
                1 for sub_items in subcats.values()
                for item in sub_items if self._checked.get(item.item_id)
            )
            cat_total = sum(len(v) for v in subcats.values())
            self._cat_tree.item(f"cat:{cat}", text=f"{cat}  ({cat_checked}/{cat_total})")
            for sub, sub_items in subcats.items():
                sub_checked = sum(1 for i in sub_items if self._checked.get(i.item_id))
                sub_label   = f"  {sub}  ({sub_checked}/{len(sub_items)})" if sub \
                              else f"  —  ({sub_checked}/{len(sub_items)})"
                node_id = f"sub:{cat}:{sub}"
                if self._cat_tree.exists(node_id):
                    self._cat_tree.item(node_id, text=sub_label)

    def _save_selection(self) -> None:
        selected = [iid for iid, checked in self._checked.items() if checked]
        try:
            if self._db_conn:
                init_db(self._db_conn)
                saved, removed = save_watched_items(self._db_conn, selected)
            else:
                with get_connection(self.db_path) as conn:
                    init_db(conn)
                    saved, removed = save_watched_items(conn, selected)
            self._status_var.set(
                f"Сохранено: +{saved} добавлено, -{removed} удалено. Всего: {len(selected)}."
            )
            messagebox.showinfo(
                "Готово",
                f"Список обновлён.\nДобавлено: {saved}\nУдалено: {removed}\nИтого: {len(selected)}",
            )
        except sqlite3.Error as e:
            messagebox.showerror("Ошибка сохранения", str(e))


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def run(db_path: str | None = None, realm: str | None = None) -> None:
    db_path = db_path or settings.db_path
    realm   = realm   or settings.region.value
    root    = tk.Tk()
    ItemSelectorApp(root, db_path=db_path, realm=realm)
    root.mainloop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UI выбора предметов STALCRAFT")
    parser.add_argument("--db",    default=None)
    parser.add_argument("--realm", default=None)
    if hasattr(parser, "parse_args"):
        args = parser.parse_args()
    else:
        args, _ = parser.parse_known_args()
    run(db_path=args.db, realm=args.realm)
