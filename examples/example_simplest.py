from dataset_organizer import process_log_csv
from config_simplest import CONFIG_SIMPLEST

result = process_log_csv(CONFIG_SIMPLEST, ["data/log_file.csv"])
print(result)