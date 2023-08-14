from transform import Transform_DF

# Child Class
class Transform_Fimplus_Class(Transform_DF):
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

def transform_Fimplus(name):
    Fimplus = Transform_Fimplus_Class(name)
    Fimplus.transform()
    Fimplus.write_csv()