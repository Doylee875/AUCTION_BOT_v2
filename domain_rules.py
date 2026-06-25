"""
domain_rules.py
===============
Правила предметной области: какой attr_type назначить предмету.

Выделены из config.py, т.к. это конфигурация домена («артефакты имеют
qlt+ptn»), а не операционные настройки («таймаут запроса», «путь к БД»).
Операционные параметры меняются под конкретное окружение деплоя; доменные
правила — часть бизнес-логики и должны жить рядом с кодом, который их
применяет.

Структуры:

  ATTR_TYPE_RULES — приоритетный список правил (category, subcategory, attr_type).
      subcategory="" означает «любая подкатегория данной категории».
      Более специфичные правила (с subcategory) должны стоять раньше общих.

  ATTR_TYPE_ITEM_LISTS — явные переопределения по item_id.
      Переопределяет ATTR_TYPE_RULES: если item_id присутствует в каком-либо
      списке, он получает соответствующий attr_type независимо от category.
      Ключ: attr_type ('none' | 'artifact' | 'upgrade' | 'qlt_only').
      Значение: множество item_id.

Чтобы добавить новое правило — отредактируй этот файл. Никаких переменных
окружения, никаких Settings-полей.
"""

# Порядок важен: первое совпавшее правило выигрывает.
# Tuple: (category, subcategory, attr_type)
# subcategory="" — совпадение только по категории (любая подкатегория).
ATTR_TYPE_RULES: list[tuple[str, str, str]] = [
    ("weapon_modules", "weapon_module_core", "qlt_only"),
    ("artefact",       "",                   "artifact"),
]

# Явные переопределения по item_id — имеют приоритет над ATTR_TYPE_RULES.
# Пример: {"upgrade": {"weapon_42", "armor_17"}, "artifact": {"special_item_99"}}
ATTR_TYPE_ITEM_LISTS: dict[str, set[str]] = {}
