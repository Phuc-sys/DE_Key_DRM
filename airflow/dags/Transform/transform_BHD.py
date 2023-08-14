from transform import Transform_DF
import pandas as pd

# Child Class
class Transform_BHD_Class(Transform_DF):

    def transform(self):
        self.df.drop(columns=["Ftype", "Folder", "Utype"], axis=1, inplace=True)

        dtype = {
            'CustomerID': 'string', 
            'MovieID': 'string',
            'RealTimePlaying': 'int64',
            'Date': 'string'
        }
        self.df = self.df.astype(dtype)
        # Reorder columns
        self.df = self.df[['CustomerID', 'MovieID', 'RealTimePlaying', 'Date']]

def transform_BHD(name):
    BHD = Transform_BHD_Class(name)
    BHD.transform()
    BHD.write_csv()