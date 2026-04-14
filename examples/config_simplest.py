CONFIG_SIMPLEST = {
    # Кастомное имя для папки с результатом. Если пусто - берётся из колонки control_jbi_name
    "custom_name": "Simplest_Examle",

    # Путь к папке для результатов
    "output_path": "results",
    
    # Записывать только изменившиеся строки
    "optimize": True,
    # Убирать префиксы группы из названий колонок (например control_jbi_name -> jbi_name)
    "strip_prefixes": True,
    # Спрашивать подтверждение перед перезаписью существующей сессии
    "confirm_overwrite": True,
    
    # Список групп колонок. Каждая группа сохраняется в отдельный CSV файл
    # (Распределение данных по группам работает на основе префиксов у названий колонок данных)
    "groups": ['coordinate', 'io', 'control', 'weld', 'scanner', 'termo', 'set', 'positioner'],
}