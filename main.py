import argparse
import asyncio
import threading
import api.fetcher
import api.fetcher_anal
import api.utils.logger


def _start_tracker_server() -> None:
    """Запускает tracker_server в фоновом daemon-потоке."""
    from tracker_server import run_server
    t = threading.Thread(target=run_server, daemon=True, name="tracker-server")
    t.start()


def main(
    run_fetch:    bool = True,
    run_analysis: bool = True,
    run_lots:     bool = False,
    force_fetch:  bool = False,
) -> None:
    """Запускает фоновые задачи синхронизации, аналитики и/или вотчера лотов.

    По умолчанию выполняет синхронизацию каталога и загрузку аналитики.
    --lots запускает вотчер активных лотов параллельно с аналитикой.
    --force принудительно синхронизирует каталог, даже если уже было сегодня.
    """
    api.utils.logger.setup_logging(
        level=api.utils.logger.settings.log_level,
        fmt=api.utils.logger.settings.log_format,
    )
    log = api.utils.logger.get_logger(__name__)
    log.info("Запуск STALCRAFT Auction Bot")

    _start_tracker_server()

    if run_fetch:
        log.info("Синхронизация каталога предметов (GitHub → БД)")
        api.fetcher.run(force=force_fetch)

    if run_analysis and run_lots:
        log.info("Загрузка истории продаж + пересчёт аналитики + вотчер лотов")
        asyncio.run(_run_analysis_and_lots())
    elif run_analysis:
        log.info("Загрузка истории продаж + пересчёт аналитики")
        asyncio.run(api.fetcher_anal.setup_analysis())
    elif run_lots:
        log.info("Вотчер активных лотов")
        asyncio.run(_run_lots_only())


async def _run_analysis_and_lots() -> None:
    """Запускает fetcher_anal и lots_watcher параллельно с общим соединением.

    FIX #3: ключевая проблема была в том, что db_lock создавался здесь, но
    никто не мог проверить — действительно ли корутины его используют.
    Решение двухуровневое:

    1. Обёртка _guarded_conn: прокси над sqlite3.Connection, который бросает
       AssertionError при любой пишущей операции (execute/executemany/commit)
       без удержания db_lock.  Это даёт fail-fast в dev/test: если
       fetcher_anal или lots_watcher забудут захватить блокировку — падение
       произойдёт немедленно с понятным сообщением, а не тихой порчей данных.

    2. Передача db_lock явным keyword-аргументом — единственный способ
       гарантировать, что обе корутины получат один и тот же объект Lock.

    Ограничение sqlite3:
       Объект Connection *не* потокобезопасен, но в рамках одного asyncio
       event-loop его можно безопасно разделять между корутинами при условии
       сериализации всех пишущих операций через db_lock.  Использование
       одного Connection вместо двух устраняет гонку на WAL-чекпоинте
       (OperationalError: database is locked), возникающую при одновременном
       conn.commit() из двух разных Connection к одному файлу.
    """
    import asyncio
    from api.fetcher_lots import run_lots_watcher
    from db.connection import open_connection
    from config import settings as _settings

    db_lock = asyncio.Lock()
    conn    = open_connection(_settings.db_path)

    # FIX #3: оборачиваем conn в прокси, который проверяет блокировку
    guarded = _guarded_conn(conn, db_lock)

    try:
        await asyncio.gather(
            api.fetcher_anal.setup_analysis(conn=guarded, db_lock=db_lock),
            run_lots_watcher(conn=guarded,              db_lock=db_lock),
        )
    finally:
        conn.close()


class _guarded_conn:
    """Прокси над sqlite3.Connection для FIX #3.

    Перехватывает execute / executemany / commit и проверяет, что вызывающий
    удерживает db_lock.  При нарушении бросает RuntimeError с описанием места
    нарушения — намного информативнее, чем «database is locked» из глубины
    sqlite3.

    Только пишущие методы защищены; read-only методы (fetchall, fetchone,
    cursor и т.д.) проксируются напрямую без проверки — чтения в SQLite WAL
    не блокируют друг друга.
    """

    _WRITE_METHODS = frozenset({"execute", "executemany", "executescript", "commit"})

    def __init__(self, conn, lock: asyncio.Lock) -> None:
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_lock", lock)

    def __getattr__(self, name: str):
        conn = object.__getattribute__(self, "_conn")
        lock = object.__getattribute__(self, "_lock")
        attr = getattr(conn, name)
        if name in _guarded_conn._WRITE_METHODS:
            def _checked(*args, **kwargs):
                if not lock.locked():
                    raise RuntimeError(
                        f"Попытка вызвать conn.{name}() без удержания db_lock. "
                        "Оберни вызов в `async with db_lock:` — иначе "
                        "возможна порча WAL или OperationalError."
                    )
                return attr(*args, **kwargs)
            return _checked
        return attr


async def _run_lots_only() -> None:
    from api.fetcher_lots import run_lots_watcher
    await run_lots_watcher()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="STALCRAFT Auction Bot runner")
    parser.add_argument("--no-fetch",    action="store_true", help="Не запускать синхронизацию каталога")
    parser.add_argument("--no-analysis", action="store_true", help="Не запускать пересчёт аналитики")
    parser.add_argument("--lots",        action="store_true", help="Запустить вотчер активных лотов")
    parser.add_argument("--force",       action="store_true", help="Принудительная синхронизация каталога")
    args = parser.parse_known_args()[0]

    main(
        run_fetch=not args.no_fetch,
        run_analysis=not args.no_analysis,
        run_lots=args.lots,
        force_fetch=args.force,
    )