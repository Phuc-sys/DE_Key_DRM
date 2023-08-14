from transform import Transform_DF
import pandas as pd

# Child Class
class Transform_DRM_Class(Transform_DF):
    def transform(self):
        dtype = {
            'CustomerID': 'string', 
            'Date': 'string',
            'Mac': 'string'
        }
        self.df = self.df.astype(dtype)
        # Reorder columns
        self.df = self.df[['CustomerID', 'Date', 'Mac']]

def transform_DRM(name):
    DRM = Transform_DRM_Class(name)
    DRM.transform()
    DRM.write_csv()