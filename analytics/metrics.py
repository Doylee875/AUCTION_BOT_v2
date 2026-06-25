"""
analytics/metrics.py
====================
Фасад обратной совместимости.

Весь код разнесён по специализированным модулям:
    analytics/compute.py      — compute_metrics()
    analytics/storage.py      — save_metrics(), fetch_all_sales_for_item(), group_by_bucket()
    analytics/recalculate.py  — recalculate_for_*()/recalculate_for_any()
    analytics/baselines.py    — recalculate_baselines(), apply_relative_volume(), get_best_slice()

Импортируйте напрямую из этих модулей. Этот файл будет удалён после
обновления всех импортов в codebase.
"""

from analytics.compute import compute_metrics
from analytics.storage import save_metrics, fetch_all_sales_for_item as _fetch_all_sales_for_item, group_by_bucket as _group_by_bucket
from analytics.recalculate import (
    recalculate_for_item,
    recalculate_for_artifact,
    recalculate_for_qlt_only,
    recalculate_for_upgradeable,
    recalculate_for_any,
)
from analytics.baselines import (
    recalculate_baselines,
    apply_relative_volume,
    get_best_slice,
)

# Приватные функции, использовавшиеся внутри старого монолита
_fetch_all_sales_for_item = _fetch_all_sales_for_item
_group_by_bucket = _group_by_bucket

__all__ = [
    "compute_metrics",
    "save_metrics",
    "recalculate_for_item",
    "recalculate_for_artifact",
    "recalculate_for_qlt_only",
    "recalculate_for_upgradeable",
    "recalculate_for_any",
    "recalculate_baselines",
    "apply_relative_volume",
    "get_best_slice",
]
