"""
db/connection.py
=================
Единая точка создания sqlite3.Connection.

До рефакторинга sqlite3.connect() вызывался в трёх разных местах
(api/fetcher.py, api/fetcher_anal.py, ui_item_selector.py), каждый раз
с собственным набором PRAGMA и без гарантии conn.close(). Теперь везде
один путь:

    from db.connection import get_connection

    with get_connection() as conn:
        ...

Путь к БД по умолчанию берётся из settings.db_path (инстанс, прочитанный
из переменных окружения в config.py), а не из класса Settings — раньше
код по ошибке читал значения по умолчанию прямо с класса (Settings.db_path),
из-за чего DB_PATH из окружения никогда не подхватывался.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from config import settings


def _configure(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _connect(path: str) -> sqlite3.Connection:
    """
    sqlite3.connect() не создаёт отсутствующие родительские директории —
    он просто падает с OperationalError: unable to open database file.
    Создаём папку заранее (если путь не ":memory:" и не пустой), чтобы
    эта ошибка не зависела от того, успел ли кто-то вручную сделать mkdir.
    """
    if path and path != ":memory:":
        parent = Path(path).parent
        parent.mkdir(parents=True, exist_ok=True)
    try:
        return sqlite3.connect(path)
    except sqlite3.OperationalError as exc:
        raise sqlite3.OperationalError(
            f"Не удалось открыть файл БД по пути {path!r}: {exc}. "
            f"Проверь DB_PATH в .env и права доступа к директории."
        ) from exc


@contextmanager
def get_connection(path: str | None = None) -> Iterator[sqlite3.Connection]:
    """
    Открывает соединение с БД и гарантированно закрывает его при выходе.

    На успешном выходе из блока — commit, на исключении — rollback
    (стандартное поведение sqlite3.Connection как контекстного менеджера).
    conn.close() вызывается в любом случае, через finally.

    Используй для коротких операций (fetch-циклы, скрипты синхронизации).
    Для соединений с временем жизни дольше одного блока (например, GUI,
    которое держит соединение открытым между обработчиками событий) —
    open_connection().

    Args:
        path: Путь к файлу БД. По умолчанию — settings.db_path.

    Yields:
        Открытое соединение с включёнными WAL и foreign_keys.
    """
    p = path or settings.db_path
    conn = _configure(_connect(p))
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def open_connection(path: str | None = None) -> sqlite3.Connection:
    """
    Открывает соединение с теми же PRAGMA, что и get_connection(), но не
    управляет его временем жизни — вызывающий код сам отвечает за commit/
    rollback/close.

    Предназначен для случаев, когда соединение должно пережить один блок
    кода — например, GUI-приложение (ui_item_selector.py), которое держит
    одно соединение открытым на протяжении всей сессии и закрывает его
    только при закрытии окна.

    Args:
        path: Путь к файлу БД. По умолчанию — settings.db_path.

    Returns:
        Открытое соединение с включёнными WAL и foreign_keys.
    """
    p = path or settings.db_path
    return _configure(_connect(p))
