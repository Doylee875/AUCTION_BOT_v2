"""
notifications/telegram.py
=========================
Telegram-нотификатор — отправка алертов через Bot API.

Использует urllib.request (stdlib) — без внешних зависимостей.
Для подключения заполнить TG_BOT_TOKEN и TG_CHAT_ID в .env.

Формат сообщения (Markdown v2):
  🔔 *Тёмный страж [Редкий +13]*
  💰 Цена лота: `120K` — выгода **−18.4%** от средней `147K`
  📦 Кол-во: 1 | Продавец: PlayerName
  ⏰ Истекает: 2026-06-22 18:30 UTC
  📊 Ликвидность: 0.72 | Продаж/нед.: 3.4
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from functools import partial

import api.utils.logger
from notifications.base import LotAlert, Notifier

log = api.utils.logger.get_logger(__name__)

_TG_API = "https://api.telegram.org/bot{token}/sendMessage"

# Символы, требующие экранирования в MarkdownV2
_MD2_ESCAPE = str.maketrans({c: f"\\{c}" for c in r"_*[]()~`>#+-=|{}.!"})


def _escape(text: str) -> str:
    return text.translate(_MD2_ESCAPE)


def _send_sync(token: str, chat_id: str, text: str) -> None:
    """Синхронная отправка через urllib (вызывается из executor)."""
    url = _TG_API.format(token=token)
    payload = json.dumps(
        {"chat_id": chat_id, "text": text, "parse_mode": "MarkdownV2"}
    ).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 204):
                body = resp.read().decode(errors="replace")
                raise RuntimeError(f"Telegram API {resp.status}: {body}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(f"Telegram API HTTP {exc.code}: {body}") from exc


class TelegramNotifier(Notifier):
    """
    Telegram-нотификатор.

    Args:
        bot_token: токен бота (TG_BOT_TOKEN из .env)
        chat_id:   id чата или канала для отправки
    """

    def __init__(self, bot_token: str, chat_id: str) -> None:
        self.bot_token = bot_token
        self.chat_id   = chat_id
        self._enabled  = bool(bot_token and chat_id)

        if not self._enabled:
            log.warning(
                "TelegramNotifier: TG_BOT_TOKEN или TG_CHAT_ID не заданы — "
                "уведомления в Telegram отключены."
            )

    async def send(self, alert: LotAlert) -> None:
        if not self._enabled:
            return
        message = self._format(alert)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None, partial(_send_sync, self.bot_token, self.chat_id, message)
        )
        log.info("[TG] Отправлено: %s", alert.display_name)

    def _format(self, alert: LotAlert) -> str:
        """Формирует текст сообщения для Telegram (MarkdownV2)."""
        expires_str = ""
        if alert.expires_dt:
            expires_str = f"\n⏰ Истекает: {_escape(alert.expires_dt.strftime('%Y-%m-%d %H:%M'))} UTC"

        seller_str = f" | Продавец: {_escape(alert.seller)}" if alert.seller else ""

        liquidity_str = (
            f"Ликвидность: {alert.liquidity:.2f}" if alert.liquidity is not None else ""
        )
        spd_str = (
            f"Продаж/нед\\.: {alert.sales_per_day:.1f}" if alert.sales_per_day is not None else ""
        )
        stats = " | ".join(filter(None, [liquidity_str, spd_str]))
        stats_line = f"\n📊 {stats}" if stats else ""

        bulk_str = ""
        if alert.bulk_share is not None and alert.bulk_share >= 0.5:
            bulk_str = f"\n📦 Оптовый рынок: {alert.bulk_share * 100:.0f}% объёма"

        return (
            f"🔔 *{_escape(alert.display_name)}*\n"
            f"💰 Цена лота: `{_escape(alert.lot_price_fmt)}` — выгода *−{_escape(alert.discount_str)}* от {_escape(alert.ref_price_fmt)}\n"
            f"📦 Кол\\-во: {alert.amount}{seller_str}"
            f"{expires_str}"
            f"{bulk_str}"
            f"{stats_line}"
        )
