"""
Биннинг и гранулярность для аналитики
======================================

Модуль для работы с временными срезами (bucket_key) при разных гранулярностях.
Позволяет конвертировать unix timestamps в "bucket_key" и обратно.

Поддерживаемые гранулярности:
  - 'window_weekday': временное окно (0-5) + тип дня (weekday/weekend)
  - 'daily': день по МСК
  - 'weekly': неделя ISO по МСК
  - 'monthly': месяц по МСК
  - 'hourly': час по МСК
"""

from datetime import date, datetime as dt, timezone
from calendar import monthrange

# Смещение московского времени относительно UTC, в секундах
MSK_OFFSET_SEC: int = 3 * 3600  # UTC+3

# Окно отсчитывается от 02:00 МСК:
#   window 0 = 02:00–06:00
#   window 1 = 06:00–10:00
#   window 2 = 10:00–14:00
#   window 3 = 14:00–18:00
#   window 4 = 18:00–22:00
#   window 5 = 22:00–02:00 (следующего дня)
WINDOW_SHIFT_SEC: int = 2 * 3600   # 02:00 МСК = сдвиг на 2 часа
WINDOW_SIZE_SEC: int = 4 * 3600    # Размер окна: 4 часа


def calculate_bucket_key(timestamp_unix: int, granularity: str) -> str:
    """
    Конвертирует unix timestamp в bucket_key для заданной гранулярности.
    
    Поддерживаемые гранулярности:
      - 'window_weekday': временное окно (0-5) + тип дня (weekday/weekend)
        Формат: "0_weekday", "3_weekend" и т.д.
      - 'daily': день по МСК
        Формат: "2025-06-18"
      - 'weekly': неделя ISO по МСК
        Формат: "2025-W25"
      - 'monthly': месяц по МСК
        Формат: "2025-06"
      - 'hourly': час по МСК
        Формат: "2025-06-18T14"
    
    Args:
        timestamp_unix: Unix timestamp (UTC)
        granularity: Тип гранулярности
    
    Returns:
        Строка bucket_key
    
    Raises:
        ValueError: Если гранулярность не поддерживается
    """
    if granularity == 'window_weekday':
        # Логика из classify_timestamp
        msk_ts = timestamp_unix + MSK_OFFSET_SEC
        window_id = (msk_ts - WINDOW_SHIFT_SEC) % 86400 // WINDOW_SIZE_SEC
        
        # Определяем день недели по МСК
        msk_date = dt.fromtimestamp(msk_ts, tz=timezone.utc).date()
        day_type = "weekend" if msk_date.isoweekday() in (6, 7) else "weekday"
        
        return f"{window_id}_{day_type}"
    
    elif granularity == 'daily':
        # День по МСК
        msk_ts = timestamp_unix + MSK_OFFSET_SEC
        msk_date = dt.fromtimestamp(msk_ts, tz=timezone.utc).date()
        return msk_date.isoformat()  # "2025-06-18"
    
    elif granularity == 'weekly':
        # Неделя ISO по МСК
        msk_ts = timestamp_unix + MSK_OFFSET_SEC
        msk_date = dt.fromtimestamp(msk_ts, tz=timezone.utc).date()
        iso_year, iso_week, _ = msk_date.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"  # "2025-W25"
    
    elif granularity == 'monthly':
        # Месяц по МСК
        msk_ts = timestamp_unix + MSK_OFFSET_SEC
        msk_date = dt.fromtimestamp(msk_ts, tz=timezone.utc).date()
        return f"{msk_date.year:04d}-{msk_date.month:02d}"  # "2025-06"
    
    elif granularity == 'hourly':
        # Час по МСК
        msk_ts = timestamp_unix + MSK_OFFSET_SEC
        msk_datetime = dt.fromtimestamp(msk_ts, tz=timezone.utc)
        date_part = msk_datetime.date().isoformat()
        hour_part = f"{msk_datetime.hour:02d}"
        return f"{date_part}T{hour_part}"  # "2025-06-18T14"
    
    else:
        raise ValueError(f"Неподдерживаемая гранулярность: {granularity}")


def parse_bucket_key(bucket_key: str, granularity: str) -> tuple[int, int] | tuple[date, date]:
    """
    Обратная операция: конвертирует bucket_key обратно в временной диапазон.
    
    Возвращает (start_timestamp_msk, end_timestamp_msk) или (start_date, end_date)
    в зависимости от гранулярности.
    
    Args:
        bucket_key: Ключ среза
        granularity: Тип гранулярности
    
    Returns:
        Кортеж (start, end) в unix timestamps (UTC) или dates
    
    Raises:
        ValueError: Если format невалиден
    """
    if granularity == 'window_weekday':
        # "0_weekday" → (0, 1)
        parts = bucket_key.split('_')
        if len(parts) != 2:
            raise ValueError(f"Неверный формат bucket_key для window_weekday: {bucket_key}")
        window_id = int(parts[0])
        # Просто возвращаем window_id — фактический диапазон зависит от конкретного дня
        return (window_id, window_id)
    
    elif granularity == 'daily':
        # "2025-06-18" → дата
        return (date.fromisoformat(bucket_key), date.fromisoformat(bucket_key))
    
    elif granularity == 'weekly':
        # "2025-W25" → неделя
        iso_year, iso_week_str = bucket_key.split('-W')
        iso_week = int(iso_week_str)
        iso_year = int(iso_year)
        # Первый день недели (понедельник)
        start_date = date.fromisocalendar(iso_year, iso_week, 1)
        # Последний день недели (воскресенье)
        end_date = date.fromisocalendar(iso_year, iso_week, 7)
        return (start_date, end_date)
    
    elif granularity == 'monthly':
        # "2025-06" → месяц
        year, month = bucket_key.split('-')
        year, month = int(year), int(month)
        start_date = date(year, month, 1)
        _, last_day = monthrange(year, month)
        end_date = date(year, month, last_day)
        return (start_date, end_date)
    
    elif granularity == 'hourly':
        # "2025-06-18T14" → час
        date_part, hour_part = bucket_key.split('T')
        start_date = date.fromisoformat(date_part)
        hour = int(hour_part)
        # Просто возвращаем дату и час
        return (start_date, hour)
    
    else:
        raise ValueError(f"Неподдерживаемая гранулярность: {granularity}")


def get_supported_granularities() -> list[str]:
    """Возвращает список поддерживаемых гранулярностей."""
    return ['window_weekday', 'daily', 'weekly', 'monthly', 'hourly']
