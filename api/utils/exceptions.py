"""
utils/exceptions.py — Кастомные исключения для STALCRAFT API клиента.

Иерархия исключений:
    StalcraftAPIError               ← базовое
    ├── AuthenticationError         ← 401
    ├── ForbiddenError              ← 403
    ├── NotFoundError               ← 404
    ├── RateLimitError              ← 429
    ├── ServerError                 ← 5xx
    └── NetworkError                ← сетевые проблемы (таймаут, DNS и т.д.)
"""


class StalcraftAPIError(Exception):
    """
    Базовое исключение для всех ошибок STALCRAFT API.

    Attributes:
        message: Человекочитаемое описание ошибки.
        status_code: HTTP-код ответа (если применимо).
        response_body: Тело ответа сервера (если применимо).
    """

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        response_body: str | None = None,
    ) -> None:
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Форматирует сообщение об ошибке с кодом и телом ответа."""
        parts = [self.message]
        if self.status_code:
            parts.append(f"HTTP {self.status_code}")
        if self.response_body:
            # Ограничиваем длину тела ответа для читаемости
            body_preview = self.response_body[:200]
            parts.append(f"Response: {body_preview}")
        return " | ".join(parts)


class AuthenticationError(StalcraftAPIError):
    """
    Исключение при ошибке аутентификации (HTTP 401).

    Возникает когда:
    - Токен отсутствует или пустой
    - Токен истёк
    - Токен имеет неверный формат
    """

    def __init__(
        self,
        message: str = "Ошибка аутентификации: проверьте APP_TOKEN или USER_TOKEN",
        **kwargs,
    ) -> None:
        super().__init__(message, status_code=401, **kwargs)


class ForbiddenError(StalcraftAPIError):
    """
    Исключение при отказе в доступе (HTTP 403).

    Возникает когда:
    - Токен действителен, но не имеет прав на ресурс
    - Используется APP_TOKEN там, где нужен USER_TOKEN
    """

    def __init__(
        self,
        message: str = "Доступ запрещён: недостаточно прав для этого ресурса",
        **kwargs,
    ) -> None:
        super().__init__(message, status_code=403, **kwargs)


class NotFoundError(StalcraftAPIError):
    """
    Исключение когда ресурс не найден (HTTP 404).

    Возникает когда:
    - Неверный ID персонажа, клана или предмета
    - Эндпоинт не существует
    """

    def __init__(
        self,
        message: str = "Ресурс не найден",
        **kwargs,
    ) -> None:
        super().__init__(message, status_code=404, **kwargs)


class RateLimitError(StalcraftAPIError):
    """
    Исключение при превышении лимита запросов (HTTP 429).

    Attributes:
        retry_after: Количество секунд до следующей попытки (из заголовка Retry-After).
    """

    def __init__(
        self,
        message: str = "Превышен лимит запросов к API",
        retry_after: int | None = None,
        **kwargs,
    ) -> None:
        self.retry_after = retry_after
        if retry_after:
            message = f"{message}. Повторите через {retry_after} сек."
        super().__init__(message, status_code=429, **kwargs)


class ServerError(StalcraftAPIError):
    """
    Исключение при серверных ошибках (HTTP 5xx).

    Возникает при внутренних ошибках сервера STALCRAFT API.
    """

    def __init__(
        self,
        message: str = "Внутренняя ошибка сервера STALCRAFT API",
        **kwargs,
    ) -> None:
        super().__init__(message, **kwargs)


class NetworkError(StalcraftAPIError):
    """
    Исключение при сетевых проблемах.

    Возникает когда:
    - Таймаут соединения
    - DNS не разрешается
    - Соединение сброшено
    """

    def __init__(
        self,
        message: str = "Сетевая ошибка при обращении к STALCRAFT API",
        **kwargs,
    ) -> None:
        super().__init__(message, **kwargs)


class ConfigurationError(StalcraftAPIError):
    """
    Исключение при ошибках конфигурации приложения.

    Возникает когда:
    - Не указан обязательный токен
    - Неверный формат конфигурации
    """

    def __init__(
        self,
        message: str = "Ошибка конфигурации приложения",
        **kwargs,
    ) -> None:
        super().__init__(message, **kwargs)


HTTP_ERROR_MAP: dict[int, type[StalcraftAPIError]] = {
    401: AuthenticationError,
    403: ForbiddenError,
    404: NotFoundError,
    429: RateLimitError,
}

# Дополнительные специфичные коды можно добавить сюда.
# Пример: 422 (unprocessable) — считаем общим StalcraftAPIError, но можно
# определить отдельный класс ValidationError при необходимости.
HTTP_ERROR_MAP[422] = StalcraftAPIError


def _parse_retry_after(headers: dict | None) -> int | None:
    if not headers:
        return None
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def raise_for_status(
    status_code: int,
    response_body: str = "",
    headers: dict | None = None,
) -> None:
    """
    Бросает соответствующее исключение по HTTP-коду ответа.

    Args:
        status_code: HTTP-код ответа.
        response_body: Тело ответа для отладки.
        headers: HTTP-заголовки ответа (для Retry-After при 429).

    Raises:
        AuthenticationError: При 401.
        ForbiddenError: При 403.
        NotFoundError: При 404.
        RateLimitError: При 429.
        ServerError: При 5xx.
        StalcraftAPIError: При других ошибочных кодах.
    """
    if status_code < 400:
        return

    if status_code in HTTP_ERROR_MAP:
        exc_class = HTTP_ERROR_MAP[status_code]
        kwargs: dict = {"response_body": response_body}
        if exc_class is RateLimitError:
            kwargs["retry_after"] = _parse_retry_after(headers)
        raise exc_class(**kwargs)

    if status_code >= 500:
        raise ServerError(
            message=f"Ошибка сервера (HTTP {status_code})",
            status_code=status_code,
            response_body=response_body,
        )

    raise StalcraftAPIError(
        message=f"Ошибка запроса (HTTP {status_code})",
        status_code=status_code,
        response_body=response_body,
    )

    # Здесь можно добавить парсинг тела ответа для специфичных ошибок
    # (например, коды ошибки в JSON). Это оставлено как точка расширения.