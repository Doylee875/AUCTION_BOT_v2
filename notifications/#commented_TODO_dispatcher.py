"""
notifications/dispatcher.py
============================
Диспетчер уведомлений.

Принимает LotAlert и рассылает его во все подключённые каналы одновременно
через asyncio.gather. Сбой одного канала не блокирует остальные.

Дедупликация:
  Один и тот же лот (item_id + qlt + ptn + upgrade_level + lot_price + expires_at) не отправляется
  повторно в течение ALERT_COOLDOWN_SEC секунд.
  Новый лот с другой ценой или временем истечения проходит фильтр немедленно.
  Хранится in-memory в словаре _last_sent: ключ → unix timestamp.
  При перезапуске приложения история сбрасывается — это допустимо.

Точки расширения:
  - Персистентная дедупликация: заменить _last_sent на таблицу sent_alerts в БД
  - Фильтр по пользователям: передавать список получателей в LotAlert
  - Приоритеты каналов: отправлять сначала TG, потом Discord и т.д.
"""

from __future__ import annotations

import asyncio
import time
from typing import Sequence

import api.utils.logger
from notifications.base import LotAlert, Notifier

log = api.utils.logger.get_logger(__name__)


def _alert_key(alert: LotAlert) -> tuple:
    """Ключ дедупликации: конкретный лот = вариант + цена + время истечения."""
    return (alert.item_id, alert.qlt, alert.ptn, alert.upgrade_level,
            alert.lot_price, alert.expires_at)


class NotifierDispatcher:
    """
    Рассылает LotAlert во все зарегистрированные каналы.

    Args:
        notifiers:         список каналов (TelegramNotifier, DiscordNotifier, …)
        cooldown_sec:      минимальный интервал между повторными алертами
                           для одного и того же среза предмета (в секундах)
    """

    def __init__(
        self,
        notifiers: Sequence[Notifier],
        cooldown_sec: int = 600,
    ) -> None:
        self._notifiers   = list(notifiers)
        self._cooldown    = cooldown_sec
        # ключ → unix timestamp последней отправки
        self._last_sent:  dict[tuple, float] = {}

    def is_duplicate(self, alert: LotAlert) -> bool:
        """True если алерт по этому срезу уже отправлялся в пределах cooldown."""
        key       = _alert_key(alert)
        last_time = self._last_sent.get(key, 0.0)
        return (time.time() - last_time) < self._cooldown

    async def send(self, alert: LotAlert) -> None:
        """
        Отправить алерт во все каналы одновременно.

        Пропускает дубликаты (cooldown). Ошибки каналов изолированы —
        сбой Telegram не мешает Discord и наоборот.
        """
        if not self._notifiers:
            log.debug("Нет активных каналов уведомлений, алерт пропущен: %s", alert.display_name)
            return

        if self.is_duplicate(alert):
            log.debug(
                "Алерт пропущен (cooldown %ds): %s",
                self._cooldown, alert.display_name,
            )
            return

        log.info(
            "Отправка алерта [%d каналов]: %s — цена %s (−%s от %s)",
            len(self._notifiers),
            alert.display_name,
            alert.lot_price_fmt,
            alert.discount_str,
            alert.avg_price_fmt,
        )

        # Отправляем во все каналы параллельно
        await asyncio.gather(
            *[n.send_safe(alert) for n in self._notifiers],
        )

        # Обновляем время последней отправки только после успешного gather
        self._last_sent[_alert_key(alert)] = time.time()

    def clear_cooldowns(self) -> None:
        """Сбросить все cooldown-таймеры (для тестов или ручного сброса)."""
        self._last_sent.clear()

    @property
    def channel_count(self) -> int:
        return len(self._notifiers)


def build_dispatcher(cooldown_sec: int = 600) -> NotifierDispatcher:
    """
    Фабрика: создаёт диспетчер с каналами из настроек приложения.

    Канал добавляется только если соответствующие токены заданы в .env.
    """
    from config import settings
    from notifications.telegram import TelegramNotifier
    from notifications.discord  import DiscordNotifier

    notifiers: list[Notifier] = []

    if settings.tg_bot_token and settings.tg_chat_id:
        notifiers.append(TelegramNotifier(
            bot_token=settings.tg_bot_token,
            chat_id=settings.tg_chat_id,
        ))

    if settings.discord_webhook_url:
        notifiers.append(DiscordNotifier(webhook_url=settings.discord_webhook_url))

    dispatcher = NotifierDispatcher(notifiers, cooldown_sec=cooldown_sec)
    log.info(
        "NotifierDispatcher создан: %d канала(ов), cooldown=%ds",
        dispatcher.channel_count,
        cooldown_sec,
    )
    return dispatcher
