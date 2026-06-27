"""Пул параллельных StalcraftClient с разными OAuth-учётными данными."""

from dataclasses import dataclass

from api.client import StalcraftClient
from config import Settings


@dataclass
class StalcraftClientPool:
    clients: list[StalcraftClient]

    async def open(self) -> None:
        for client in self.clients:
            await client.open()

    async def close(self) -> None:
        for client in self.clients:
            await client.close()

    def client_for_worker(self, worker_id: int) -> StalcraftClient:
        return self.clients[worker_id % len(self.clients)]


def build_pool(cfg: Settings | None = None) -> StalcraftClientPool:
    pairs = cfg.credential_pairs()
    if not pairs:
        raise ValueError(
            "Не заданы OAuth-учётные данные: укажите CLIENT_ID и CLIENT_SECRET в .env"
        )
    clients = [
        StalcraftClient(
            pair,
            cfg=cfg,
            name=str(i),
        )
        for i, pair in enumerate(pairs)
    ]
    return StalcraftClientPool(clients)
