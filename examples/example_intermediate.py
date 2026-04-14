from dataset_organizer import process_log_csv
from config_intermediate import CONFIG_INTERMEDIATE

result = process_log_csv(CONFIG_INTERMEDIATE) # Пути можно указать и в конфиге
print(result)