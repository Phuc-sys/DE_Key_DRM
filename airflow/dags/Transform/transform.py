import pandas as pd
import os
from rename_col_df import column_dict

# Parent Class
class Transform_DF: 
    def __init__(self, name):
        self.read_dir = "/opt/airflow/dataset"
        self.write_dir = "/opt/airflow/transformed_data"
        self.name = name

        filePath = f"{self.read_dir}/{self.name}.csv"

        try:
            self.df = pd.read_csv(filePath)
        except:
            self.df = pd.DataFrame()

        self.clean()
        self.remove_special_characters()
        self.rename_col_df()

    
    def clean(self):
        self.df.drop_duplicates(keep='first', inplace=True)
    
    def remove_special_characters(self):
        self.df.columns = self.df.columns.str.replace(' ', '')

    def rename_col_df(self):
        self.df.rename(columns=column_dict[self.name], inplace=True)

    def write_csv(self):
        wirtePath = f"{self.write_dir}/{self.name}.csv"
        self.df.to_csv(wirtePath, encoding='utf-8-sig', index=False)