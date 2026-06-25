"""Пул параллельных StalcraftClient с разными OAuth-учётными данными."""

from dataclasses import dataclass

from api.auth import OAuthClient
from api.client import StalcraftClient
from config import Settings, settings


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
    cfg = cfg or settings
    pairs = cfg.credential_pairs()[: cfg.parallel_clients]
    if not pairs:
        raise ValueError(
            "Не заданы OAuth-учётные данные: укажите CLIENT_ID и CLIENT_SECRET в .env"
        )
    clients = [
        StalcraftClient(
            TokenGetter=OAuthClient(client_id=cid, client_secret=csec),
            cfg=cfg,
            name=str(i),
        )
        for i, (cid, csec) in enumerate(pairs)
    ]
    return StalcraftClientPool(clients)
