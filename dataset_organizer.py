import shutil
import pandas as pd
from pathlib import Path

SYS_TIME = 'sys_time'
GROUPS = ['coordinate', 'io', 'control', 'weld', 'scanner', 'termo', 'set', 'positioner']


def _strip_prefixes(df, group):
    return df.rename(columns=lambda col: col.removeprefix(group + '_') if col != SYS_TIME else col)


def _get_jbi_name(df, custom_name=""):
    if custom_name.strip():
        return custom_name.strip()
    if 'control_jbi_name' in df.columns:
        name = df['control_jbi_name'].iloc[0]
        if not pd.isna(name) and str(name).strip() != '':
            return str(name).strip()
    return 'UNNAMED'


def process_csv(
    file,
    output_path="processed_data",
    custom_name="",
    strip_prefixes=True,
    additional_files=None,
    confirm_overwrite=True
):
    """
    Парсит CSV датасет и раскладывает по файловой структуре.

    Параметры:
        file              - путь к входному CSV файлу
        output_path       - папка для результата
        custom_name       - кастомное имя сессии (по умолчанию берётся из control_jbi_name)
        strip_prefixes    - убирать ли префиксы из названий колонок
        additional_files  - список путей к доп. файлам, которые будут скопированы в папку сессии
        confirm_overwrite - спрашивать ли подтверждение при перезаписи (False для автоматизации)

    Возвращает:
        Path сессии если успешно, None если отменено или ошибка

    Пример использования:
        from dataset_parser import process_csv

        process_csv(
            file="data/log_20260108.csv",
            output_path="results",
            strip_prefixes=True,
            additional_files=["data/program.json"]
        )
    """
    df = pd.read_csv(file)

    if SYS_TIME not in df.columns:
        print(f"Ошибка: колонка '{SYS_TIME}' не найдена в датасете.")
        return None

    jbi_name = _get_jbi_name(df, custom_name)
    start_time = pd.to_datetime(df[SYS_TIME].iloc[0]).strftime('%Y-%m-%d_%H-%M-%S')
    session_path = Path(output_path) / jbi_name / start_time

    if session_path.exists():
        if confirm_overwrite:
            answer = input(f"Папка {session_path} уже существует. Перезаписать? (y/n): ")
            if answer.lower() != 'y':
                print("Парсинг отменён.")
                return None
        else:
            print(f"Предупреждение: папка {session_path} уже существует, файлы будут перезаписаны.")

    session_path.mkdir(parents=True, exist_ok=True)

    # Стандартные группы
    known_cols = set()
    for group in GROUPS:
        group_cols = [col for col in df.columns if col.startswith(group + '_')]
        if group_cols:
            known_cols.update(group_cols)
            result = df[[SYS_TIME] + group_cols]
            if strip_prefixes:
                result = _strip_prefixes(result, group)
            result.to_csv(session_path / f'{group}.csv', index=False)

    # Неизвестные колонки -> other_data.csv
    other_cols = [col for col in df.columns if col != SYS_TIME and col not in known_cols]
    if other_cols:
        df[[SYS_TIME] + other_cols].to_csv(session_path / 'other_data.csv', index=False)
        print(f"Найдено {len(other_cols)} неизвестных колонок. Сохранены в other_data.csv")

    # Доп. файлы
    if additional_files:
        for path in additional_files:
            src = Path(path)
            if src.exists():
                shutil.copy(src, session_path / src.name)
            else:
                print(f"Доп. файл не найден: {path}")

    return session_path
