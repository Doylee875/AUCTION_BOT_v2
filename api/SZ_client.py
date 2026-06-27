"""
api/client.py — Асинхронный HTTP-клиент для STALCRAFT API.

Обёртка над aiohttp.ClientSession с поддержкой:
- Автоматической подстановки Client-Id / Client-Secret заголовков
- additional=true во всех запросах (требуется для корректного парсинга атрибутов)
- Обработки HTTP-ошибок через кастомные исключения
- Повторных попыток при сетевых сбоях (exponential-подобный backoff)
- Опционального LRU+TTL кэша GET-ответов
- Rate-limiting через aiolimiter

Документация: https://eapi.stalzone.net
"""

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any

import aiohttp
from aiolimiter import AsyncLimiter
from cachetools import TTLCache

from api.utils.exceptions import NetworkError, RateLimitError, raise_for_status
from api.utils.logger import get_logger
from config import settings,Settings

logger = get_logger(__name__)

# Параметр, который STALCRAFT API требует для получения доп. атрибутов
# (qlt, ptn, upgrade_level и т.д.). Добавляется ко всем запросам автоматически.
_ADDITIONAL_PARAM: dict[str, str] = {"additional": "true"}
REQUEST_TIMEOUT: int = 10
REQUEST_DELAY:int = 10
MAX_RETRIES: int = 5

@dataclass(slots=True)
class StalcraftClient:
    """Асинхронный HTTP-клиент для STALCRAFT API.

    Usage:
        async with StalcraftClient() as client:
            data = await client.get("/EU/emission")

    Или ручное управление:
        client = StalcraftClient()
        await client.open()
        data = await client.get("/EU/emission")
        await client.close()
    """
    key_pair: tuple[str, str] 
    _token_get_headers : dict[str, str] = field(default=None, init = False, repr=False)
    cfg: Settings = field(default_factory=settings)
    name:str = ""

    # Опциональный in-memory GET-кэш (LRU + TTL). По умолчанию отключён.
    cache_enabled: bool = False
    cache_ttl: int = 60       # секунды
    cache_max_size: int = 1024

    _session: aiohttp.ClientSession | None = field(default=None, init=False, repr=False)
    _semaphore: asyncio.Semaphore | None = field(default=None, init=False, repr=False)
    _cache: dict = field(default_factory=dict, init=False, repr=False)
    _limiter: AsyncLimiter | None = field(default=None, init=False, repr=False)
    # Постоянный fallback-lock: разделяется между всеми вызовами одного клиента,
    # поэтому реально ограничивает параллелизм (в отличие от asyncio.Lock() на месте).
    _fallback_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        logger.info(
            "StalcraftClient инициализирован: client=%s, base_url=%s, region=%s",
            self.name or "default",
            self.cfg.base_url,
            self.cfg.region,
        )
        self._token_get_headers = self._build_token_headers()


    # ------------------------------------------------------------------
    # Управление жизненным циклом
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """Открывает HTTP-сессию. Вызывается автоматически при `async with`."""
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        self._session = aiohttp.ClientSession(
            base_url=self.cfg.base_url,
            timeout=timeout,
        )
        self._semaphore = asyncio.Semaphore(len(self.cfg.credential_pairs()))

        if self.cache_enabled:
            self._cache = TTLCache(maxsize=self.cache_max_size, ttl=self.cache_ttl)

        # rate_limit_rps: приоритет — явная настройка, иначе вычисляем из request_delay
        rps = (1.0 / max(REQUEST_DELAY, 1e-6)
        )
        if rps > 0:
            self._limiter = AsyncLimiter(int(max(1, round(rps))), time_period=1)

        logger.debug("HTTP-сессия открыта: client=%s", self.name or "default")

    async def close(self) -> None:
        """Закрывает HTTP-сессию и освобождает ресурсы."""
        if self._session is not None:
            await self._session.close()
            self._session = None
            logger.debug("HTTP-сессия закрыта: client=%s", self.name or "default")

    async def __aenter__(self) -> "StalcraftClient":
        await self.open()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Вспомогательные методы
    # ------------------------------------------------------------------
    
    
    
    def _ensure_open(self) -> aiohttp.ClientSession:
        """Проверяет что сессия открыта и возвращает её."""
        if self._session is None:
            raise RuntimeError(
                "HTTP-сессия не открыта. Используйте `async with StalcraftClient() as client:`"
            )
        return self._session

    def _build_headers(self) -> dict[str, str]:
        auth_headers = self._token_get_headers
        return {
            **auth_headers,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _build_params(self, params: dict[str, Any] | None) -> dict[str, Any]:
        """Добавляет additional=true к пользовательским параметрам.

        Создаёт новый словарь — входной аргумент не мутируется.
        additional=true обязателен: без него API не возвращает атрибуты
        предмета (qlt, ptn, upgrade_level), что ломает логику парсинга.
        """
        return {**(params or {}), **_ADDITIONAL_PARAM}


    def _build_token_headers(self) -> dict[str, str]:
        """Возвращает заголовки для Secret Based Authentication."""
        client_id, client_secret = self.key_pair
        return {
            "Client-Id": client_id,
            "Client-Secret": client_secret,
        }


    # ------------------------------------------------------------------
    # Ядро: выполнение запроса с retry-логикой
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        """Выполняет HTTP-запрос к API с повторными попытками при сбоях.

        Порядок операций:
          1. Проверка кэша (только GET).
          2. Цикл retry: rate-limit → semaphore → запрос → статус → парсинг.
          3. Запись в кэш при успехе (только GET).

        При RateLimitError использует Retry-After из заголовка ответа.
        При сетевых ошибках применяет линейный backoff (retry_delay × attempt).
        """
        session = self._ensure_open()
        headers = self._build_headers()
        merged_params = self._build_params(params)
        label = self.name or "default"
        is_get = method.upper() == "GET"

        logger.debug("→ client=%s %s %s | params=%s", label, method, path, merged_params)

        # Кэш-ключ вычисляется один раз; None если кэш отключён или метод не GET
        cache_key: str | None = None
        if is_get and self.cache_enabled:
            cache_key = f"{path}|{json.dumps(merged_params, sort_keys=True)}"
            try:
                return self._cache[cache_key]
            except KeyError:
                pass

        rate_guard = self._limiter if self._limiter is not None else self._fallback_lock
        last_exception: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with rate_guard:
                    async with self._semaphore:  # type: ignore[union-attr]  # всегда задан после open()
                        async with session.request(
                            method=method,
                            url=path,
                            headers=headers,
                            params=merged_params,
                            json=body,
                        ) as response:
                            body_text = await response.text()
                            raise_for_status(response.status, body_text, dict(response.headers))

                result = json.loads(body_text)

                if cache_key is not None:
                    try:
                        self._cache[cache_key] = result
                    except Exception:
                        pass

                return result

            except RateLimitError as exc:
                last_exception = exc
                delay = exc.retry_after if exc.retry_after is not None else self.cfg.retry_delay
                logger.warning(
                    "Rate limit: client=%s попытка %d/%d %s %s, повтор через %.1fs",
                    label, attempt, MAX_RETRIES, method, path, delay,
                )
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(delay)
                    continue
                raise

            except (
                aiohttp.ClientConnectorError,
                aiohttp.ServerTimeoutError,
                asyncio.TimeoutError,
                aiohttp.ClientOSError,
            ) as exc:
                last_exception = exc
                logger.warning(
                    "Сетевая ошибка: client=%s попытка %d/%d %s %s: %s",
                    label, attempt, MAX_RETRIES, method, path, exc,
                )
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(self.cfg.retry_delay * attempt)

        raise NetworkError(
            message=f"Не удалось выполнить {method} {path} после {MAX_RETRIES} попыток",
            response_body=str(last_exception),
        )

    # ------------------------------------------------------------------
    # Публичные методы
    # ------------------------------------------------------------------

    async def get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """GET-запрос. params мержится с additional=true автоматически."""
        return await self._request("GET", path, params=params)

    async def post(self, path: str, *, body: dict[str, Any] | None = None) -> Any:
        """POST-запрос."""
        return await self._request("POST", path, body=body)

    async def put(self, path: str, *, body: dict[str, Any] | None = None) -> Any:
        """PUT-запрос."""
        return await self._request("PUT", path, body=body)

    async def patch(self, path: str, *, body: dict[str, Any] | None = None) -> Any:
        """PATCH-запрос."""
        return await self._request("PATCH", path, body=body)

    async def delete(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        """DELETE-запрос. params мержится с additional=true автоматически."""
        return await self._request("DELETE", path, params=params)
