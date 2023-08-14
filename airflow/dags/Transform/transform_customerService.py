from transform import Transform_DF

# Child Class
class Transform_CustomerService_Class(Transform_DF):
    def transform(self):
        dtype = {
            'CustomerID': 'string', 
            'ServiceID': 'string',
            'Amount': 'int64',
            'Date': 'string'
        }
        self.df = self.df.astype(dtype)
        # Reorder columns
        self.df = self.df[['CustomerID', 'ServiceID', 'Amount', 'Date']]

def transform_CustomerService(name):
    customerService = Transform_CustomerService_Class(name)
    customerService.transform()
    customerService.write_csv()