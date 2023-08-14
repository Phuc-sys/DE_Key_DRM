from transform import Transform_DF

# Child Class
class Transform_MV_Class(Transform_DF):
    def transform(self):
        dtype = {
            'ID': 'string', 
            'TopTitle': 'string',
            'TitleEN': 'string',
            'Release': 'string',
            'Actors': 'string', 
            'Directors': 'string',
            'Producers': 'string',
            'PublishCountry': 'string',
            'Duration': 'int64', 
            'isDRM': 'bool'
        }
        self.df = self.df.astype(dtype)
        # Reorder columns
        self.df = self.df[['ID', 'TopTitle', 'TitleEN', 'Release', 'Actors', 'Directors', 'Producers', 'PublishCountry', 'Duration', 'isDRM']]

def transform_MV(name):
    movie = Transform_MV_Class(name)
    movie.transform()
    movie.write_csv()