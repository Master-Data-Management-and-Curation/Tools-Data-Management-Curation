# load_nffa_instruments.py
import os
import csv
import pandas as pd


path="/Users/federica/Documents/Esercizi_MDMC25/project"

#path = settings.BASE_DIR + '/'  # o qualunque tu stia usando
        
file_name = 'nffa_instruments.csv'

df_dump = pd.read_csv(path+'/'+file_name, delimiter=',', quoting=csv.QUOTE_NONNUMERIC, encoding='utf-8', index_col=False)

print(df_dump.columns.tolist())

#python manage.py load_nffa_instruments --file=my_dump.csv (senza file cerca qu
