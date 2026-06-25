"""
fetcher.py
==========
Оркестрация: скачивает ZIP-архив репозитория, разбирает его
и сохраняет предметы в SQLite.

Точка входа: run() или `python fetcher.py`.
"""

import sqlite3

from config import settings
import api.utils.logger


from api.github_client import make_session, download_repo_zip, iter_item_files_from_zip
from db.connection import get_connection
from schema import init_db
from db.catalog import upsert_item, log_sync, INSERTED, UPDATED

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

# Реалмы берём из настроек, чтобы не держать константу в коде.
# Для backward-compatibility оставляем список, но формируем его по `settings.region`.
REALMS: list[str] = [settings.region.value.lower()]

# Количество предметов между промежуточными коммитами в БД.
BATCH_SIZE: int = 100

log = api.utils.logger.get_logger(__name__)


# ---------------------------------------------------------------------------
# Импорт архива в БД
# ---------------------------------------------------------------------------

def fetch_all(session, conn: sqlite3.Connection) -> tuple[int, int]:
    """
    Скачивает архив репозитория и сохраняет все предметы в БД.
    Возвращает (items_inserted, items_updated).

    Весь архив — один HTTP-запрос. Парсинг и вставка в БД — локально.
    """
    archive = download_repo_zip(session)

    try:
        inserted = updated = batch_count = 0

        for realm_raw, category, subcategory, repo_path, item_data in \
                iter_item_files_from_zip(archive, REALMS):

            status = upsert_item(
                conn, category, subcategory, repo_path, item_data,
            )

            if status == INSERTED:
                inserted += 1
            elif status == UPDATED:
                updated += 1

            batch_count += 1
            if batch_count >= BATCH_SIZE:
                conn.commit()
                log.info(
                    "Промежуточный коммит: +%d новых, ~%d обновлённых...",
                    inserted, updated,
                )
                batch_count = 0

        conn.commit()

    finally:
        archive.close()   # закрываем и удаляем временный файл

    return inserted, updated


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

def run(force: bool = False) -> None:
    """
    Запускает полный цикл синхронизации.
    Пропускает загрузку, если sync_log уже содержит запись за сегодня
    и не передан флаг force=True.
    """

    log.info("Старт. Реалмы: %s | БД: %s", REALMS, settings.db_path)

    session = make_session()

    with get_connection() as conn:
        init_db(conn)

        if not force:
            row = conn.execute(
                "SELECT 1 FROM sync_log"
                " WHERE run_at >= strftime('%s', 'now', 'start of day')"
                " LIMIT 1"
            ).fetchone()
            if row:
                log.info("Синхронизация уже выполнялась сегодня, пропускаем. (--force чтобы принудить)")
                return

        inserted, updated = fetch_all(session, conn)
        log_sync(conn, inserted, updated)

    log.info(
        "Готово. +%d новых, ~%d обновлённых. База: %s",
        inserted, updated, settings.db_path,
    )


if __name__ == "__main__":
    run()
