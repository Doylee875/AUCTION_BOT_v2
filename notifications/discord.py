"""
notifications/discord.py
========================
Discord-нотификатор через Webhook.

Использует urllib.request (stdlib) — без внешних зависимостей.
Для подключения задать DISCORD_WEBHOOK_URL в .env.

  DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.request
from functools import partial

import api.utils.logger
from notifications.base import LotAlert, Notifier

log = api.utils.logger.get_logger(__name__)


def _send_sync(webhook_url: str, payload: dict) -> None:
    """Синхронная отправка через urllib (вызывается из executor)."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status not in (200, 204):
                body = resp.read().decode(errors="replace")
                raise RuntimeError(f"Discord webhook {resp.status}: {body}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(f"Discord webhook HTTP {exc.code}: {body}") from exc


class DiscordNotifier(Notifier):
    """
    Discord-нотификатор через Webhook.

    Args:
        webhook_url: URL вебхука Discord-канала (DISCORD_WEBHOOK_URL из .env)
    """

    def __init__(self, webhook_url: str) -> None:
        self.webhook_url = webhook_url
        self._enabled    = bool(webhook_url)

        if not self._enabled:
            log.warning(
                "DiscordNotifier: DISCORD_WEBHOOK_URL не задан — "
                "уведомления в Discord отключены."
            )

    async def send(self, alert: LotAlert) -> None:
        if not self._enabled:
            return
        payload = self._build_payload(alert)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, partial(_send_sync, self.webhook_url, payload)
        )
        log.info("[Discord] Отправлено: %s", alert.display_name)

    def _build_payload(self, alert: LotAlert) -> dict:
        """
        Формирует Discord Embed payload.
        https://discord.com/developers/docs/resources/message#embed-object
        """
        color_map = {
            "common":    0xF5F5F5,
            "uncommon":  0x5CB85C,
            "rare":      0x5BC0DE,
            "epic":      0x9B59B6,
            "legendary": 0xF0AD4E,
            "mythical":  0xE74C3C,
        }
        embed_color = color_map.get((alert.color or "common").lower(), 0xAAAAAA)

        fields = [
            {
                "name": "💰 Цена лота",
                "value": f"`{alert.lot_price_fmt}` (−{alert.discount_str} от {alert.ref_price_fmt})",
                "inline": False,
            },
            {
                "name": "📦 Кол-во",
                "value": str(alert.amount),
                "inline": True,
            },
        ]
        if alert.seller:
            fields.append({"name": "👤 Продавец", "value": alert.seller, "inline": True})
        if alert.expires_dt:
            fields.append({
                "name": "⏰ Истекает",
                "value": alert.expires_dt.strftime("%Y-%m-%d %H:%M UTC"),
                "inline": True,
            })
        if alert.liquidity is not None:
            fields.append({
                "name": "📊 Ликвидность",
                "value": f"{alert.liquidity:.2f}",
                "inline": True,
            })
        if alert.sales_per_day is not None:
            fields.append({
                "name": "📈 Продаж/нед.",
                "value": f"{alert.sales_per_day:.1f}",
                "inline": True,
            })
        if alert.bulk_share is not None and alert.bulk_share >= 0.5:
            fields.append({
                "name": "🏭 Опт. доля объёма",
                "value": f"{alert.bulk_share * 100:.0f}%",
                "inline": True,
            })

        return {
            "embeds": [
                {
                    "title": f"🔔 {alert.display_name}",
                    "color": embed_color,
                    "fields": fields,
                    "footer": {
                        "text": "STALCRAFT | AuctionBot"
                    },
                }
            ]
        }
