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

    # #11: трекер-сервер стартует вместе с ботом
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

    Ключевое ограничение sqlite3: объект Connection *не* потокобезопасен,
    но в рамках одного asyncio event-loop его можно безопасно разделить
    между корутинами при условии, что все пишущие операции сериализованы
    через asyncio.Lock. Именно это делает общий db_lock.

    Использование одного Connection вместо двух устраняет гонку на
    WAL-чекпоинте (OperationalError: database is locked), которая
    возникает при одновременном conn.commit() из двух разных объектов
    Connection к одному файлу.
    """
    import asyncio
    from api.fetcher_lots import run_lots_watcher
    from db.connection import open_connection
    from config import settings as _settings

    db_lock = asyncio.Lock()
    conn    = open_connection(_settings.db_path)
    try:
        await asyncio.gather(
            api.fetcher_anal.setup_analysis(conn=conn, db_lock=db_lock),
            run_lots_watcher(conn=conn, db_lock=db_lock),
        )
    finally:
        conn.close()


async def _run_lots_only() -> None:
    from api.fetcher_lots import run_lots_watcher
    await run_lots_watcher()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="STALCRAFT Auction Bot runner")
    parser.add_argument("--no-fetch",    action="store_true", help="Не запускать синхронизацию каталога")
    parser.add_argument("--no-analysis", action="store_true", help="Не запускать пересчёт аналитики")
    parser.add_argument("--lots",        action="store_true", help="Запустить вотчер активных лотов")
    parser.add_argument("--force",       action="store_true", help="Принудительная синхронизация каталога, даже если уже была сегодня")
    args = parser.parse_known_args()[0]

    main(
        run_fetch=not args.no_fetch,
        run_analysis=not args.no_analysis,
        run_lots=args.lots,
        force_fetch=args.force,
    )
