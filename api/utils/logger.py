"""
utils/logger.py — Настройка системы логирования для STALCRAFT API клиента.

Использует стандартный модуль logging.
Вызовите setup_logging() один раз при старте приложения (main.py).
"""

import logging
import sys
from typing import Literal
from logging.handlers import RotatingFileHandler

from config import settings


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def setup_logging(
    level: LogLevel = "INFO",
    fmt: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    date_fmt: str = "%Y-%m-%d %H:%M:%S",
    log_file: str | None = None,
    rotate: bool = True,
    max_bytes: int = 10_000_000,
    backup_count: int = 5,
) -> None:
    """
    Инициализирует систему логирования приложения.

    Настраивает форматирование, уровень и (опционально) запись в файл.
    Должна вызываться один раз в начале работы приложения.

    Args:
        level: Уровень логирования (DEBUG / INFO / WARNING / ERROR / CRITICAL).
        fmt: Формат строки лога.
        date_fmt: Формат даты/времени в логе.
        log_file: Путь к файлу лога. Если None — только в stdout.

    Example:
        >>> setup_logging(level="DEBUG", log_file="stalcraft.log")
    """
    formatter = logging.Formatter(fmt=fmt, datefmt=date_fmt)

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level))

    root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_file:
        if rotate:
            file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
        else:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logging.debug("Логирование инициализировано: уровень=%s", level)

    # hooks: можно добавить интеграцию с Sentry / Telegram для критических ошибок


def get_logger(name: str) -> logging.Logger:
    """
    Возвращает именованный логгер для модуля.

    Args:
        name: Имя логгера, обычно __name__ вызывающего модуля.

    Returns:
        Настроенный экземпляр logging.Logger.

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Запрос отправлен")
    """
    return logging.getLogger(name)