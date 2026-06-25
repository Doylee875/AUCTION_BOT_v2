"""
analytics — аналитика аукционных данных.

Модули:
    compute.py      — compute_metrics(): вычисление метрик из строк продаж
    storage.py      — save_metrics(), fetch_all_sales_for_item(), group_by_bucket()
    recalculate.py  — recalculate_for_*(), recalculate_for_any()
    baselines.py    — recalculate_baselines(), apply_relative_volume(), get_best_slice()
    bucket.py       — работа с временными срезами
    auto_select.py  — автоматический выбор лучшего среза
"""
