"""
config.py — Конфигурация приложения STALCRAFT API.

Загружает параметры из переменных окружения (.env файл).
Документация API: https://eapi.stalcraft.net

Переменные окружения:
    # --- API ---
    BASE_URL                  — базовый URL API (default: https://eapi.stalcraft.net)
    REGION                    — регион STALCRAFT: RU | EU (регистронезависимо, default: RU)

    # --- OAuth ---
    CLIENT_ID                 — OAuth Client-Id (первая пара)
    CLIENT_SECRET             — OAuth Client-Secret (первая пара)
    CLIENT_ID2                — OAuth Client-Id (вторая пара, для параллельных запросов)
    CLIENT_SECRET2            — OAuth Client-Secret (вторая пара)

    # --- Внешние сервисы ---
    GITHUB_TOKEN              — токен GitHub для синхронизации каталога

    # --- Хранилище ---
    DB_PATH                   — путь к SQLite базе (default: data/FREAK.db)
    ICONS_DIR                 — каталог для иконок предметов (default: data/icons)

    # --- Фильтры предметов ---
    EXCLUDE_IDS_FILTER        — item id через запятую: исключить из обработки
    EXCLUDE_CATEGORIES_FILTER — категории через запятую: исключить из обработки
    INCLUDE_IDS_FILTER        — item id через запятую: включить принудительно
    INCLUDE_CATEGORIES_FILTER — категории через запятую: включить принудительно

    # --- Уведомления ---
    TG_BOT_TOKEN              — токен Telegram-бота
    TG_CHAT_ID                — chat id Telegram (куда слать алерты)
    DISCORD_BOT_TOKEN         — токен Discord-бота
    DISCORD_WEBHOOK_URL       — webhook URL Discord (альтернатива боту)

    # --- Бизнес-логика аукциона ---
    AUCTION_COMMISSION        — комиссия аукциона, доля (default: 0.05 = 5 %%)

    # --- Логирование ---
    LOG_LEVEL                 — уровень логирования (default: INFO)
    LOG_FORMAT                — формат строк лога
    LOG_DATE_FORMAT           — формат даты лога

Примечания:
    • Переменные надёжности HTTP (REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY,
      PARALLEL_CLIENTS, REQUEST_DELAY, RATE_LIMIT_RPS) и бизнес-параметры лотов
      (LOTS_POLL_INTERVAL, LOTS_DISCOUNT_THRESHOLD, LOTS_ALERT_COOLDOWN_SEC,
      LOTS_VOLATILITY_SCALE_THR, HISTORY_MAX_AGE_DAYS) в данный момент
      закомментированы — зарезервированы для будущих релизов.
    • Правила предметной области (attr_type артефактов, ядер и т.д.)
      намеренно вынесены в domain_rules.py — это конфигурация домена, не
      операционные настройки.
"""

import os
from dataclasses import dataclass,field
from enum import Enum

from dotenv import load_dotenv

load_dotenv()

@dataclass
class Region(str, Enum):
    RU = "RU"
    EU = "EU"

    @classmethod
    def from_str(cls, value: str) -> "Region":
        """Парсит регион регистронезависимо; при ошибке даёт понятное сообщение."""
        try:
            return cls(value.upper())
        except ValueError:
            valid = ", ".join(r.value for r in cls)
            raise ValueError(
                f"Неверное значение REGION={value!r}. Допустимые: {valid}"
            ) from None


def _parse_optional_float(env_key: str) -> float | None:
    """Возвращает float из env-переменной или None, если переменная не задана."""
    raw = os.getenv(env_key)
    return float(raw) if raw is not None else None


@dataclass
class Settings:
    """Операционные настройки приложения из переменных окружения."""

    # --- API ---
    base_url: str = "https://eapi.stalcraft.net"
    region: Region = field(default_factory=Region.RU)

    # --- OAuth ---
    client_id: str = ""
    client_secret: str = ""
    client_id2: str = ""
    client_secret2: str = ""

    # --- Внешние сервисы ---
    github_token: str = ""

    # --- Хранилище ---
    db_path: str = "data/FREAK.db"
    icons_dir: str = "data/icons"

    # --- Hardcoded фильтры ---
    # Перечисление тех, что не попадают вовсе
    exclude_ids_filter: str | None = ""
    exclude_categories_filter: str | None = ""
    # Перечисление тех, что точно попадают
    include_ids_filter: str | None = ""
    include_categories_filter: str | None = ""

    # --- Уведомления ---
    tg_bot_token: str = ""
    tg_chat_id: str = ""
    discord_bot_token: str = ""
    discord_webhook_url: str = ""

    # --- Надёжность HTTP ---
    # request_timeout: float = 30.0
    # max_retries: int = 3
    # retry_delay: float = 1.0
    # parallel_clients: int = 2
    # request_delay: float = 0.2
    # rate_limit_rps: float | None = None

    # --- Бизнес-логика лотов ---
    # lots_poll_interval: int = 120
    # lots_discount_threshold: float = 0.15
    # lots_alert_cooldown_sec: int = 600
    # lots_volatility_scale_thr: float = 0.30
    auction_commission: float = 0.05  # Комиссия аукциона 5% от цены продажи

    # --- История ---
    # history_max_age_days: int = 30

    # --- Логирование ---
    log_level: str = "INFO"
    log_format: str = "%(asctime)s|%(levelname)-8s|%(name)s|%(message)s"
    log_date_format: str ="%H:%M"

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
        d = cls.__new__(cls)
        # Инициализируем дефолтами без side-эффектов __post_init__,
        # чтобы взять дефолтные значения полей для использования как fallback.
        for f in cls.__dataclass_fields__.values():
            if f.default is not f.default_factory:  # type: ignore[misc]
                object.__setattr__(d, f.name, f.default)

        return cls(
            base_url=os.getenv("BASE_URL", "https://eapi.stalcraft.net"),
            region=Region.from_str(os.getenv("REGION", Region.RU.value)),
            github_token=os.getenv("GITHUB_TOKEN", ""),
            client_id=os.getenv("CLIENT_ID", ""),
            client_id2=os.getenv("CLIENT_ID2", ""),
            client_secret=os.getenv("CLIENT_SECRET", ""),
            client_secret2=os.getenv("CLIENT_SECRET2", ""),
            db_path=os.getenv("DB_PATH", "data/FREAK.db"),
            icons_dir=os.getenv("ICONS_DIR", "data/icons"),
            exclude_ids_filter=os.getenv("EXCLUDE_IDS_FILTER", ""),
            exclude_categories_filter=os.getenv("EXCLUDE_CATEGORIES_FILTER", ""),
            include_ids_filter=os.getenv("INCLUDE_IDS_FILTER", ""),
            include_categories_filter=os.getenv("INCLUDE_CATEGORIES_FILTER", ""),
            tg_bot_token=os.getenv("TG_BOT_TOKEN", ""),
            tg_chat_id=os.getenv("TG_CHAT_ID", ""),
            discord_bot_token=os.getenv("DISCORD_BOT_TOKEN", ""),
            discord_webhook_url=os.getenv("DISCORD_WEBHOOK_URL", ""),
            auction_commission=float(os.getenv("AUCTION_COMMISSION", "0.05")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_format=os.getenv("LOG_FORMAT", "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"),
            log_date_format=os.getenv("LOG_DATE_FORMAT", "%H:%M"),

        )

settings = Settings.from_env()