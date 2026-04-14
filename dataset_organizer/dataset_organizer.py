import shutil
import pandas as pd
from pathlib import Path
from fnmatch import fnmatch
from .default_config import DEFAULT_CONFIG

SYS_TIME = 'sys_time'

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


def _get_column_mode(col, group, cfg):
    if col == SYS_TIME:
        return "passive"
    change_modes = cfg.get("change_modes", {})
    group_cfg = change_modes.get(group, {})
    common_cfg = change_modes.get("_common", {})

    for mode in ("any", "self", "passive"):
        if any(fnmatch(col, p) for p in group_cfg.get(mode, [])):
            return mode

    for mode in ("any", "self", "passive"):
        if any(fnmatch(col, p) for p in common_cfg.get(mode, [])):
            return mode

    if "_default" in group_cfg:
        return group_cfg["_default"]

    return cfg.get("default_change_mode", "any")

#старый вариант
def _apply_change_filter(current_row, prev_row, group, cfg):
    if prev_row is None:
        return current_row
    
    passive_cols = cfg.get("passive_columns", [SYS_TIME])
    cols = [col for col in current_row.index if col not in passive_cols]
    
    triggered = [col for col in cols if current_row[col] != prev_row[col]
                and _get_column_mode(col, group, cfg) != "passive"]
    
    if not triggered:
        return None 
    
    result = {}
    for col in current_row.index:
        mode = _get_column_mode(col, group, cfg)
        if mode == "passive":
            result[col] = current_row[col] 
        elif mode == "any":
            result[col] = current_row[col]
        elif mode == "self":
            result[col] = current_row[col] if col in triggered else NotImplemented
    
    return pd.Series(result)

def _cols_changed(df, col):
    try:
        return ~df[col].eq(df[col].shift(1)) & ~(df[col].isna() & df[col].shift(1).isna())
    except TypeError:
        # колонка содержит массивы
        return df[col].astype(str) != df[col].shift(1).astype(str)

def _apply_change_filter_vectorized(df, group, cfg, prev_row=None):
    if prev_row is not None:
        df = pd.concat([prev_row.to_frame().T, df]).reset_index(drop=True)

    passive_cols = set(cfg.get("passive_columns", [SYS_TIME]))

    any_cols, self_cols = [], []
    for col in df.columns:
        if col in passive_cols:
            continue
        mode = _get_column_mode(col, group, cfg)
        if mode == "passive":
            continue
        elif mode == "self":
            self_cols.append(col)
        else:
            any_cols.append(col)

    changed = pd.DataFrame({col: _cols_changed(df, col) for col in df.columns})

    triggered = changed[any_cols + self_cols].any(axis=1)

    result = df[triggered].copy()

    for col in self_cols:
        result[col] = result[col].astype(object)
        result.loc[~changed.loc[result.index, col], col] = None

    if prev_row is not None:
        result = result[result.index > 0]

    new_prev_row = df.iloc[-1]
    return result.reset_index(drop=True), new_prev_row

def _process_single_df(df, session_path, cfg, prev_rows=None):
    if prev_rows is None:
        prev_rows = {}  # {group: last_row}
    
    known_cols = set()
    for group in cfg["groups"]:
        group_cols = [col for col in df.columns if col.startswith(group + '_')]
        if group_cols:
            known_cols.update(group_cols)
            
            result = df[[SYS_TIME] + group_cols]

            out_path = session_path / f'{group}.csv'
            write_header = not out_path.exists()

            if cfg["optimize"]:
                filtered_result, prev_rows[group] = _apply_change_filter_vectorized(
                    result, group, cfg, prev_rows.get(group)
                )
                if cfg["strip_prefixes"]:
                    filtered_result = _strip_prefixes(filtered_result, group)
                if not filtered_result.empty:
                    filtered_result.to_csv(out_path, mode='a', header=write_header, index=False)
            else:
                if cfg["strip_prefixes"]:
                    result = _strip_prefixes(result, group)
                result.to_csv(out_path, mode='a', header=write_header, index=False)
    
    other_cols = [col for col in df.columns if col != SYS_TIME and col not in known_cols]
    if other_cols:
        other_path = session_path / 'other_data.csv'
        write_header = not other_path.exists()
        df[[SYS_TIME] + other_cols].to_csv(other_path, mode='a', header=write_header, index=False)
        print(f"Найдено {len(other_cols)} неизвестных колонок. Сохранены в other_data.csv")

    return prev_rows

def process_log_csv(config = None, files = [], additional_files = []):
    """
    Парсит CSV лог-файл(ы) и раскладывает данные по файловой структуре.

    Параметры:
        config           - словарь с настройками (переопределяет DEFAULT_CONFIG)
        files            - путь или список путей к входным CSV файлам
        additional_files - путь или список путей к доп. файлам,
                           которые будут скопированы в папку сессии

    Возвращает:
        Path папки сессии если успешно, None если отменено или ошибка

    Пример использования:
        from dataset_organizer import process_log_csv

        process_log_csv(
            files="data/log_20260108.csv"
        )

        process_log_csv(
            config={
                "custom_name": "my_session",
                "default_change_mode": "self",
            },
            files=["data/log_part1.csv", "data/log_part2.csv"],
            additional_files="data/program.json"
        )

        CONFIG = {
            "custom_name": "my_session",
            "default_change_mode": "self",
        }

        process_log_csv(
            CONFIG,
            files=["data/log_part1.csv", "data/log_part2.csv"],
            additional_files="data/program.json"
        )
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    #print(config)
    #print(cfg)
    if not cfg['files'] and not files:
        print("Не указан путь к файлу(ам)")
        return None
    if cfg['files'] and files:
        print("Предупреждение: Путь к файлу('files') указан как при вызове функции так и в конфиге.", 
              "Предполагается исползование только 1 из этих способов указания пути."
              "Путь из аргумента функции был взят для работы как более приоретеный.")
    if cfg['additional_files'] and additional_files:
        print("Предупреждение: Путь к дополнительным файлам('additional_files') указан как при вызове функции так и в конфиге.", 
              "Предполагается исползование только 1 из этих способов указания пути."
              "Путь из аргумента функции был взят для работы как более приоретеный.")
    if files: cfg['files'] = files
    if additional_files: cfg['additional_files'] = additional_files
    if not isinstance(cfg['files'], list): cfg['files'] = [cfg['files']]
    if not isinstance(cfg['additional_files'], list): cfg['additional_files'] = [cfg['additional_files']]

    dataframes = []
    for file in cfg['files']:
        df = pd.read_csv(file)
        if SYS_TIME not in df.columns:
            print(f"Ошибка: '{SYS_TIME}' не найдена в {file}, файл пропущен.")
            continue
        dataframes.append(df)

    if not dataframes:
        return None

    dataframes.sort(key=lambda df: pd.to_datetime(df[SYS_TIME].iloc[0]))

    earliest_df = dataframes[0]
    jbi_name = _get_jbi_name(earliest_df, cfg["custom_name"])
    start_time = pd.to_datetime(earliest_df[SYS_TIME].iloc[0]).strftime('%Y-%m-%d_%H-%M-%S')
    session_path = Path(cfg["output_path"]) / jbi_name / start_time

    # проверка overwrite
    if session_path.exists():
        if cfg["confirm_overwrite"]:
            answer = input(f"Папка {session_path} уже существует. Перезаписать? (y/n): ")
            if answer.lower() != 'y':
                print("Парсинг отменён.")
                return None
        shutil.rmtree(session_path)  # удаляем старую папку
        print(f"Папка {session_path} перезаписана.")

    session_path.mkdir(parents=True, exist_ok=True)

    prev_rows = {}
    for df in dataframes:
        prev_rows = _process_single_df(df, session_path, cfg, prev_rows)

    for path in cfg['additional_files']:
        src = Path(path)
        if src.exists():
            shutil.copy(src, session_path / src.name)
        else:
            print(f"Доп. файл не найден: {path}")

    return session_path
