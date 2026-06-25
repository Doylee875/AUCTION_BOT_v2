"""
notifications/base.py
=====================
Абстрактный интерфейс уведомлений и датакласс LotAlert.

LotAlert содержит всё необходимое для формирования сообщения в любом канале:
  - данные о предмете и лоте
  - метрики из аналитики (для контекста «насколько выгодно»)
  - атрибуты среза (qlt, ptn, upgrade_level) для точной идентификации

Каждый канал уведомлений реализует Notifier.send(alert).
NotifierDispatcher рассылает в все каналы одновременно через asyncio.gather.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# Датакласс алерта
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LotAlert:
    """
    Данные о выгодном лоте для отправки в каналы уведомлений.

    Формируется в fetcher_lots.py при обнаружении лота, цена которого
    ниже исторической средней на discount_pct процентов.
    """
    # --- Идентификация предмета ---
    item_id:   str
    name_ru:   str
    name_en:   str = ""
    color:     str = ""          # редкость предмета (common / rare / …)

    # --- Атрибуты среза (ATTR_SENTINEL = -1 если не применимо) ---
    qlt:           int = -1
    ptn:           int = -1
    upgrade_level: int = -1

    # --- Данные лота ---
    lot_price:  float = 0.0      # цена лота (price_per_unit)
    amount:     int   = 1        # количество единиц в лоте
    seller:     str   = ""       # ник продавца
    expires_at: int | None = None  # unix timestamp истечения лота

    # --- Исторические метрики (для контекста) ---
    avg_price:     float | None = None   # эталонная цена сегмента (не всегда avg по всем)
    volatility:    float | None = None
    liquidity:     float | None = None
    sales_per_day: float | None = None

    # --- Amount-сегментация ---
    price_label:  str   = "ср."   # «розн.» / «опт.» / «ср.» — какой эталон использован
    bulk_share:   float | None = None  # доля оптового объёма (0..1)

    # --- Вычисленные поля ---
    discount_pct: float = 0.0    # (avg_price - lot_price) / avg_price * 100

    # --- Метаданные алерта ---
    detected_at: int = field(
        default_factory=lambda: int(datetime.now(timezone.utc).timestamp())
    )

    # ---------------------------------------------------------------------------
    # Вспомогательные свойства для форматирования сообщений
    # ---------------------------------------------------------------------------

    @property
    def slice_label(self) -> str:
        """Читаемое обозначение среза: «Редкий +13», «+7», «Необыч» или «»."""
        qlt_labels = {
            0: "Обыч", 1: "Необыч", 2: "Особый",
            3: "Редкий", 4: "Искл.", 5: "Легенд.",
        }
        parts = []
        if self.qlt != -1:
            parts.append(qlt_labels.get(self.qlt, f"q{self.qlt}"))
        if self.ptn != -1:
            parts.append(f"+{self.ptn}")
        if self.upgrade_level != -1:
            parts.append(f"ул.{self.upgrade_level}")
        return " ".join(parts)

    @property
    def display_name(self) -> str:
        """Имя предмета + срез, например «Тёмный страж [Редкий +13]»."""
        name = self.name_ru or self.item_id
        if self.slice_label:
            return f"{name} [{self.slice_label}]"
        return name

    @property
    def expires_dt(self) -> datetime | None:
        """Время истечения лота как datetime UTC."""
        if self.expires_at is None:
            return None
        return datetime.fromtimestamp(self.expires_at, tz=timezone.utc)

    def fmt_price(self, val: float | None) -> str:
        if val is None:
            return "—"
        if val >= 1_000_000:
            return f"{val / 1_000_000:.2f}M"
        if val >= 1_000:
            return f"{val / 1_000:.0f}K"
        return str(int(val))

    @property
    def lot_price_fmt(self) -> str:
        return self.fmt_price(self.lot_price)

    @property
    def ref_price_fmt(self) -> str:
        """Эталонная цена с меткой сегмента: «147K (розн.)»."""
        price_str = self.fmt_price(self.avg_price)
        return f"{price_str} ({self.price_label})"

    @property
    def avg_price_fmt(self) -> str:
        """Обратная совместимость — возвращает форматированную эталонную цену."""
        return self.fmt_price(self.avg_price)

    @property
    def discount_str(self) -> str:
        return f"{self.discount_pct:.1f}%"


# ---------------------------------------------------------------------------
# Абстрактный нотификатор
# ---------------------------------------------------------------------------

class Notifier(ABC):
    """
    Базовый класс канала уведомлений.

    Каждый канал (Telegram, Discord) реализует метод send().
    Ошибки в send() не должны пробрасываться наружу — только логироваться,
    чтобы сбой одного канала не блокировал другой.
    """

    @abstractmethod
    async def send(self, alert: LotAlert) -> None:
        """Отправить уведомление о выгодном лоте."""
        ...

    async def send_safe(self, alert: LotAlert) -> None:
        """Обёртка с перехватом исключений — вызывать из dispatcher."""
        try:
            await self.send(alert)
        except Exception as exc:
            # Логируем, но не пробрасываем — сбой одного канала изолирован
            import api.utils.logger
            log = api.utils.logger.get_logger(self.__class__.__name__)
            log.error("Ошибка отправки уведомления: %s", exc, exc_info=True)
