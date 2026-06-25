"""
config.py — Конфигурация приложения STALCRAFT API.

Загружает параметры из переменных окружения (.env файл).
Документация API: https://eapi.stalcraft.net

Переменные окружения:
    BASE_URL            — базовый URL API (default: https://eapi.stalcraft.net)
    REGION              — регион STALCRAFT: RU | EU
    CLIENT_ID           — OAuth Client-Id (первая пара)
    CLIENT_SECRET       — OAuth Client-Secret (первая пара)
    CLIENT_ID2          — OAuth Client-Id (вторая пара, для параллельных запросов)
    CLIENT_SECRET2      — OAuth Client-Secret (вторая пара)
    PARALLEL_CLIENTS    — число параллельных API-клиентов (default: 2)
    REQUEST_DELAY       — пауза между запросами в секундах (default: 0.2)
    GITHUB_TOKEN        — токен GitHub для синхронизации каталога
    DB_PATH             — путь к SQLite базе
    ICONS_DIR           — каталог для иконок предметов
    TG_BOT_TOKEN        — токен Telegram-бота
    TG_CHAT_ID          — chat id Telegram
    DISCORD_BOT_TOKEN   — токен Discord-бота
    REQUEST_TIMEOUT     — таймаут HTTP-запроса (сек)
    MAX_RETRIES         — максимум повторных попыток
    RETRY_DELAY         — задержка между повторами (сек)
    LOG_LEVEL           — уровень логирования
    LOG_FORMAT          — формат строк лога

Правила предметной области (какой attr_type у артефактов, ядер и т.д.)
намеренно вынесены в domain_rules.py — это конфигурация домена, не
операционные настройки.
"""

import os
from dataclasses import dataclass
from enum import Enum

from dotenv import load_dotenv

load_dotenv()

API_MODE = "PRODUCTION"


class Region(str, Enum):
    RU = "RU"
    EU = "EU"


@dataclass
class Settings:
    """Операционные настройки приложения из переменных окружения."""

    base_url: str = "https://eapi.stalcraft.net"
    region: Region = Region.RU

    client_id: str = ""
    client_id2: str = ""
    client_secret: str = ""
    client_secret2: str = ""

    github_token: str = ""

    db_path: str = "data/FREAK.db"
    icons_dir: str = "data/icons"
    CATEGORIES_FILTER: set[str] | None = None

    tg_bot_token: str = ""
    tg_chat_id: str = ""

    discord_bot_token: str = ""
    discord_webhook_url: str = ""

    request_timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    parallel_clients: int = 2
    request_delay: float = 0.2
    rate_limit_rps: float | None = None

    lots_poll_interval:        int   = 120
    lots_discount_threshold:   float = 0.15
    lots_alert_cooldown_sec:   int   = 600
    lots_volatility_scale_thr: float = 0.30

    history_max_age_days: int = 30

    log_level: str = "INFO"
    log_format: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")

    def credential_pairs(self) -> list[tuple[str, str]]:
        """Возвращает пары (client_id, client_secret), пропуская неполные."""
        pairs: list[tuple[str, str]] = []
        if self.client_id and self.client_secret:
            pairs.append((self.client_id, self.client_secret))
        if self.client_id2 and self.client_secret2:
            pairs.append((self.client_id2, self.client_secret2))
        return pairs

    @classmethod
    def from_env(cls) -> "Settings":
        """Создаёт настройки из переменных окружения."""
        d = cls()
        return cls(
            base_url=os.getenv("BASE_URL", d.base_url),
            region=Region(os.getenv("REGION", d.region.value)),
            github_token=os.getenv("GITHUB_TOKEN", d.github_token),
            client_id=os.getenv("CLIENT_ID", d.client_id),
            client_id2=os.getenv("CLIENT_ID2", d.client_id2),
            client_secret=os.getenv("CLIENT_SECRET", d.client_secret),
            client_secret2=os.getenv("CLIENT_SECRET2", d.client_secret2),
            db_path=os.getenv("DB_PATH", d.db_path),
            icons_dir=os.getenv("ICONS_DIR", d.icons_dir),
            CATEGORIES_FILTER=d.CATEGORIES_FILTER,
            tg_bot_token=os.getenv("TG_BOT_TOKEN", d.tg_bot_token),
            tg_chat_id=os.getenv("TG_CHAT_ID", d.tg_chat_id),
            discord_bot_token=os.getenv("DISCORD_BOT_TOKEN", d.discord_bot_token),
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", d.discord_webhook_url),
            request_timeout=float(os.getenv("REQUEST_TIMEOUT", d.request_timeout)),
            max_retries=int(os.getenv("MAX_RETRIES", d.max_retries)),
            retry_delay=float(os.getenv("RETRY_DELAY", d.retry_delay)),
            parallel_clients=int(os.getenv("PARALLEL_CLIENTS", d.parallel_clients)),
            request_delay=float(os.getenv("REQUEST_DELAY", d.request_delay)),
            rate_limit_rps=(float(os.getenv("RATE_LIMIT_RPS")) if os.getenv("RATE_LIMIT_RPS") is not None else None),
            lots_poll_interval=int(os.getenv("LOTS_POLL_INTERVAL", d.lots_poll_interval)),
            lots_discount_threshold=float(os.getenv("LOTS_DISCOUNT_THRESHOLD", d.lots_discount_threshold)),
            lots_alert_cooldown_sec=int(os.getenv("LOTS_ALERT_COOLDOWN_SEC", d.lots_alert_cooldown_sec)),
            lots_volatility_scale_thr=float(os.getenv("LOTS_VOLATILITY_SCALE_THR", d.lots_volatility_scale_thr)),
            history_max_age_days=int(os.getenv("HISTORY_MAX_AGE_DAYS", d.history_max_age_days)),
            log_level=os.getenv("LOG_LEVEL", d.log_level),
            log_format=os.getenv("LOG_FORMAT", d.log_format),
        )


settings = Settings.from_env()
