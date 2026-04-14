from dataset_organizer import process_log_csv
from config_advanced import CONFIG_ADVANSED

result = process_log_csv(
    CONFIG_ADVANSED, 
    ["data/log_file.csv"], # можно загружать несколько файлов
    ["data/SUPERDUPLEX_HALFBODY_11_02.JBI"] # и также дополнительные файлы, которые будут скопированы к остольным результатам 
    )
print(result)