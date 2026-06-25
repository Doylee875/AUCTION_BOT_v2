"""
github_client.py
================
Взаимодействие с GitHub API.
Скачивает весь репозиторий одним ZIP-архивом и разбирает его локально.
"""

import json
import os
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Generator

import requests

from config import settings
from api.utils import logger

# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

GITHUB_TOKEN: str = settings.github_token
MAX_RETRIES: int  = settings.max_retries

GITHUB_API_BASE = "https://api.github.com"
REPO            = "EXBO-Studio/stalcraft-database"

# Куда складывать иконки локально
ICONS_DIR = Path(settings.icons_dir)  # например Path("data/icons")

log = logger.get_logger(__name__)


# ---------------------------------------------------------------------------
# Сессия
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    if GITHUB_TOKEN:
        session.headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    else:
        log.warning("GitHub токен не задан — возможен 403 на скачивание архива.")
    return session


# ---------------------------------------------------------------------------
# Скачивание архива
# ---------------------------------------------------------------------------

def download_repo_zip(session: requests.Session) -> zipfile.ZipFile:
    url = f"{GITHUB_API_BASE}/repos/{REPO}/zipball/main"
    log.info("Скачиваю архив репозитория...")

    for attempt in range(1, MAX_RETRIES + 1):
        tmp = tempfile.TemporaryFile()
        try:
            with session.get(url, stream=True, timeout=120) as resp:
                if resp.status_code in (429, 403):
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    log.warning(
                        "Rate-limit (HTTP %s). Жду %d сек...",
                        resp.status_code, retry_after,
                    )
                    tmp.close()
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()

                downloaded = 0
                for chunk in resp.iter_content(chunk_size=65_536):
                    tmp.write(chunk)
                    downloaded += len(chunk)

            tmp.seek(0)
            log.info("Архив скачан: %.1f МБ.", downloaded / 1_000_000)
            return zipfile.ZipFile(tmp)

        except requests.RequestException as exc:
            tmp.close()
            log.error("Попытка %d/%d — ошибка: %s", attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"Не удалось скачать архив после {MAX_RETRIES} попыток.")


# ---------------------------------------------------------------------------
# Разбор архива
# ---------------------------------------------------------------------------

def _find_root_prefix(archive: zipfile.ZipFile) -> str:
    """
    GitHub zipball всегда содержит одну корневую директорию вида
    '<owner>-<repo>-<short_sha>/'. Находим и возвращаем её как префикс.
    """
    names = archive.namelist()
    if not names:
        return ""
    return names[0].split("/")[0] + "/"


def _parse_item_path(path: str) -> tuple[str, str, str] | None:
    """
    Разбирает путь относительно корня репозитория.
    Поддерживаемые паттерны:
        ru/items/<category>/<item_id>.json
        ru/items/<category>/<subcategory>/<item_id>.json
    """
    if not path.endswith(".json"):
        return None

    parts = path.split("/")

    if len(parts) == 4 and parts[1] == "items":
        realm, _, category, _ = parts
        return realm, category, ""

    if len(parts) == 5 and parts[1] == "items":
        realm, _, category, subcategory, _ = parts
        return realm, category, subcategory

    return None


def extract_icons(
    archive: zipfile.ZipFile,
    prefix: str,
) -> dict[str, Path]:
    # # Добавим диагностику — выведем первые 30 путей из архива
    # all_names = archive.namelist()
    # log.info("Первые 30 путей в архиве:")
    # for name in all_names[:30]:
    #     log.info("  %s", name)

    # # И отдельно — всё, что содержит "icon"
    # log.info("Пути содержащие 'icon':")
    # for name in all_names:
    #     if "icon" in name.lower():
    #         log.info("  %s", name)
    #         break  # достаточно одного примера для понимания структуры
    
    """
    Извлекает все иконки из папки icons/ в ICONS_DIR на диске.

    Структура в архиве:
        <prefix>icons/<category>/<subcategory>/<item_id>.jpg
        <prefix>icons/<category>/<item_id>.jpg          (без подкатегории)

    Возвращает словарь:  item_id (без расширения) → абсолютный Path до файла.

    Если у двух предметов одинаковый item_id, но разные категории —
    в словаре останется последний (на практике item_id уникален в репо).
    """
    ICONS_DIR.mkdir(parents=True, exist_ok=True)
    icon_map: dict[str, Path] = {}
    # Внутри extract_icons, если путь ru/icons/<category>/...:
    icons_prefix = f"{prefix}ru/icons/"
    extracted = 0

    for zip_path in archive.namelist():
        if not zip_path.startswith(icons_prefix):
            continue

        # Путь внутри папки icons/ (без ведущего префикса)
        relative = zip_path[len(icons_prefix):]  # например: weapon/pistol/12ab.jpg

        # Пропускаем директории и файлы не-картинки
        if not relative or relative.endswith("/"):
            continue
        if not relative.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            continue

        # item_id — имя файла без расширения
        filename = os.path.basename(relative)
        item_id  = os.path.splitext(filename)[0]

        # Сохраняем, повторяя структуру подпапок: ICONS_DIR/weapon/pistol/12ab.jpg
        dest = ICONS_DIR / relative
        dest.parent.mkdir(parents=True, exist_ok=True)

        with archive.open(zip_path) as src, open(dest, "wb") as dst:
            dst.write(src.read())

        icon_map[item_id] = dest.resolve()
        extracted += 1

    log.info("Извлечено иконок: %d → %s", extracted, ICONS_DIR)
    return icon_map


def iter_item_files_from_zip(
    archive: zipfile.ZipFile,
    realms: list[str],
) -> Generator[tuple[str, str, str, str, dict, Path | None], None, None]:
    """
    Обходит ZIP-архив и возвращает кортежи:
        (realm, category, subcategory, repo_path, item_data, icon_path)

    icon_path — абсолютный Path до файла иконки или None, если не найдена.

    Сначала один раз извлекает все иконки, потом обходит JSON-файлы —
    два прохода по namelist(), но это дёшево (только имена, не содержимое).
    """
    prefix     = _find_root_prefix(archive)
    log.info(prefix)
    prefix_len = len(prefix)
    realms_set = set(realms)

    # --- Шаг 1: извлечь иконки и построить карту item_id → path ---
    icon_map = extract_icons(archive, prefix)
    # --- Шаг 2: обойти JSON-предметы ---
    for zip_path in archive.namelist():
        if not zip_path.endswith(".json"):
            continue

        relative_path = zip_path[prefix_len:]

        parsed = _parse_item_path(relative_path)
        if parsed is None:
            continue

        realm, category, subcategory = parsed

        if realm not in realms_set:
            continue

        if settings.CATEGORIES_FILTER and category not in settings.CATEGORIES_FILTER:
            continue

        try:
            with archive.open(zip_path) as f:
                item_data = json.load(f)
        except (json.JSONDecodeError, KeyError) as exc:
            log.error("Ошибка парсинга %s: %s", zip_path, exc)
            continue

        # item_id — имя файла без .json
        item_id   = os.path.splitext(os.path.basename(zip_path))[0]
        icon_path = icon_map.get(item_id)

        if icon_path is None:
            log.debug("Иконка не найдена для item_id=%s", item_id)

        item_data["icon_path"] = str(icon_path) if icon_path else None

        yield realm, category, subcategory, relative_path, item_data