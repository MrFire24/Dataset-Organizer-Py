from dataset_organizer import process_log_csv

# Самый простой пример использования 
# при None вместо пути к конфигу программа будет использовать стандартный
result = process_log_csv(None, ["data/log_file.csv"])

# Стандартный конфиг также при желании можно достать из пакета
# from dataset_organizer import DEFAULT_CONFIG
# result = process_log_csv(DEFAULT_CONFIG, ["data/log_file.csv"])

print(result) # Выведет путь к результату или None в случае неудачи
# файл будет сохранён в папку под назапниме "WALL_SIDE_TO_SIDE_3" из-за отсутсвия конфига и следователько кастомного имени