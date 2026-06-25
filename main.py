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
    --lots  запускает вотчер активных лотов параллельно с аналитикой.
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

    FIX #3 (asyncio.Lock + db_lock):
        Ключевое ограничение sqlite3: объект Connection *не* потокобезопасен,
        но в рамках одного asyncio event-loop его можно безопасно разделить
        между корутинами при условии, что ВСЕ операции с БД (execute, commit)
        выполняются внутри `async with db_lock`.

        Ранее db_lock создавался здесь и передавался в корутины, но не было
        никакой гарантии, что fetcher_anal и lots_watcher действительно
        его используют — достаточно одного незащищённого commit() чтобы
        получить OperationalError: database is locked или повреждение WAL.

        Теперь db_lock обязателен для всех операций с conn.  Корутины
        fetcher_anal.setup_analysis и run_lots_watcher должны принимать
        db_lock и использовать `async with db_lock` вокруг КАЖДОГО
        conn.execute() / conn.commit().  Если ваша версия этих функций
        не поддерживает db_lock — не передавайте один Connection,
        а откройте для каждой корутины отдельное соединение (WAL позволяет
        это сделать безопасно) либо используйте aiosqlite.

        Здесь мы явно проверяем сигнатуру и при несовместимости выдаём
        понятную ошибку вместо молчаливого повреждения данных.
    """
    import inspect
    from api.fetcher_lots import run_lots_watcher
    from db.connection import open_connection
    from config import settings as _settings

    db_lock = asyncio.Lock()
    conn    = open_connection(_settings.db_path)

    # Проверяем, что корутины принимают db_lock (защита от молчаливого пропуска)
    for fn, name in [
        (api.fetcher_anal.setup_analysis, "fetcher_anal.setup_analysis"),
        (run_lots_watcher,                "fetcher_lots.run_lots_watcher"),
    ]:
        sig = inspect.signature(fn)
        if "db_lock" not in sig.parameters:
            raise RuntimeError(
                f"{name} не принимает db_lock — невозможно гарантировать "
                "сериализацию записей в БД.  Добавьте параметр db_lock "
                "и оберните все conn.execute()/conn.commit() в "
                "`async with db_lock`."
            )

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