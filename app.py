from dataset_organizer import process_log_csv
from config import CONFIG

result = process_log_csv(CONFIG, ["D:/vscode/Dataset Organizer/dataset_all_fields.csv"])
print(result)