from transform import Transform_DF

# Child Class
class Transform_Customer_Class(Transform_DF):
    def transform(self):
        dtype = {
            'CustomerID': 'string', 
            'Mac': 'string',
            'Created_date': 'string'
        }
        self.df = self.df.astype(dtype)
        # Reorder columns
        self.df = self.df[['CustomerID', 'Mac', 'Created_date']]

def transform_Customer(name):
    customer = Transform_Customer_Class(name)
    customer.transform()
    customer.write_csv()