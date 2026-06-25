from dataclasses import dataclass, field
from config import settings
from api.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class OAuthClient:
    client_id: str = field(default_factory=lambda: settings.client_id)
    client_secret: str = field(default_factory=lambda: settings.client_secret)

    async def get_valid_token(self) -> dict[str, str]:
        """Возвращает заголовки для Secret Based Authentication."""
        return {
            "Client-Id": self.client_id,
            "Client-Secret": self.client_secret,
        }