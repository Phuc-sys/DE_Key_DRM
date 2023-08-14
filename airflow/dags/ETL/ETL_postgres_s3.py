import pandas as pd
import pandasql as ps
from datetime import datetime, timedelta
import boto3
from io import BytesIO

from DB_Connection.DB_Connection import connect_postgres


def Extract_from_Posgres(df_dict):
    conn = connect_postgres()
    cursor = conn.cursor()

    table_list = ["Customers", "CustomerService", "Log_BHD_MovieID", "Log_Fimplus_MovieID", "Log_Get_DRM_List", "MV_PropertiesShowVN"]

    for table in table_list:
        # Retrieve Table Columns
        sql_column = f"""
                SELECT *
                FROM information_schema.columns
                WHERE table_schema = 'Key_DRM'
                AND table_name = {table}
                ;
        """

        cursor.execute(sql_column)
        table_columns = list(cursor.fetchall())
        column_list = [column[0] for column in table_columns]

        # Retrieve Data
        sql_data = f"""
                SELECT * FROM Key_DRM.{table};
        """

        cursor.execute(sql_data)
        data = cursor.fetchall()

        df_dict[table] = pd.DataFrame(columns=column_list, data=data)
    
    cursor.close()
    conn.close()
    
    return df_dict

def get_min_max_date(df1, df2):
    min_a = df1.min()
    min_b = df2.min()
    max_a = df1.max()
    max_b = df2.max()
    min_date = min_a
    max_date = max_a
        
    if min_b < min_date:
        min_date = min_b
    if max_b > max_date:
        max_date = max_b
    return min_date, max_date

def Transform(df_dict):
    '''Transform into Datawarehouse Schema'''
    
    df_Customers = df_dict['Customers']
    df_BHD = df_dict['Log_BHD_MovieID']
    df_Fimplus = df_dict['Log_Fimplus_MovieID']
    df_Movie = df_dict['MV_PropertiesShowVN']
    df_CustomerService = df_dict['CustomerService']
    df_DRM = df_dict['Log_Get_DRM_List']

    # Convert to datetime and remove timezone
    df_BHD['Date'] = pd.to_datetime(df_BHD['Date'], utc=False)
    df_Fimplus['Date'] = pd.to_datetime(df_Fimplus['Date'], utc=False)
    df_DRM['Date'] = pd.to_datetime(df_DRM['Date'], utc=False)

    # Get Total Key from BHD
    query_totalKey_BHD = """ select lbm.Date, count(distinct lbm.CustomerID) as BHD_Total_Key 
            from df_BHD lbm left join df_Movie mp on lbm.MovieID = mp.ID
            where mp.isDRM = 1
            group by lbm.Date
        """
    df_BHD_Key = ps.sqldf(query_totalKey_BHD)

    # Get Total Key from Fimplus
    query_totalKey_Fimplus = """ select lbm.Date, count(distinct lbm.CustomerID) as Fimplus_Total_Key 
            from df_Fimplus lbm left join df_Movie mp on lbm.MovieID = mp.ID 
            where mp.isDRM = 1
            group by lbm.Date
        """
    df_Fimplus_Key = ps.sqldf(query_totalKey_Fimplus)

    # Create TVOD table with incremental ID
    df_TVOD = pd.merge(df_BHD_Key, df_Fimplus_Key, on='Date', how='outer')
    df_TVOD['BHD_Total_Key'] = df_TVOD['BHD_Total_Key'].fillna(0)
    df_TVOD['Fimplus_Total_Key'] = df_TVOD['Fimplus_Total_Key'].fillna(0)
    df_TVOD['TVOD_Total_Key'] = df_TVOD['BHD_Total_Key'] + df_TVOD['Fimplus_Total_Key']
    df_TVOD['ID'] = range(1, 1 + len(df_TVOD))

    # Create SVOD table with incremental ID
    query_totalKey_SVOD = """ select lgdl.Date, count(distinct lgdl.CustomerID) as SVOD_Total_Key 
            from df_DRM lgdl inner join df_CustomerService c on lgdl.CustomerID = c.CustomerID 
            group by lgdl.Date
        """
    df_SVOD = ps.sqldf(query_totalKey_SVOD)

    df_SVOD['ID'] = range(1, 1 + len(df_SVOD))

    # Get min, max date
    min_date, max_date = get_min_max_date(df_TVOD['Date'], df_SVOD['Date'])

    # Create DATE table
    time_arr = [min_date + timedelta(days = i) for i in range((max_date - min_date).days + 1)]
    time_dict = {}
    time_dict["Date"] = []
    time_dict["Day"] = []
    time_dict["Month"] = []
    time_dict["Year"] = []
        
    for date in time_arr :
        time_dict['Date'].append(date.date())
        time_dict['Day'].append(date.day)
        time_dict['Month'].append(date.month)
        time_dict['Year'].append(date.year)

    df_time = pd.DataFrame(time_dict)

    # Convert data type
    dtype_TVOD = {
        'ID': 'string', 
        'BHD_Total_Key': 'int64',
        'Fimplus_Total_Key': 'int64',
        'TVOD_Total_Key': 'int64',
        'Date': 'string'
    }
    df_TVOD = df_TVOD.astype(dtype_TVOD)
    # -------- #
    dtype_SVOD = {
        'ID': 'string', 
        'SVOD_Total_Key': 'int64',
        'Date': 'string'
    }
    df_SVOD = df_SVOD.astype(dtype_SVOD)
    # -------- #
    dtype_TIME = {
        'Date': 'string', 
        'Day': 'string',
        'Month': 'string',
        'Year': 'string'
    }
    df_time = df_time.astype(dtype_TIME)

    # Reorder columns
    df_TVOD = df_TVOD[['ID', 'BHD_Total_Key', 'Fimplus_Total_Key', 'TVOD_Total_Key', 'Date']]
    df_SVOD = df_SVOD[['ID', 'SVOD_Total_Key', 'Date']]
    df_Time = df_time[['Date', 'Day', 'Month', 'Year']]

    new_table = {
        'TVOD': df_TVOD,
        'SVOD': df_SVOD, 
        'Time': df_Time
    }

    for table, df in new_table.items():
        df_dict[table] = df

    return df_dict

def load_S3(df_dict):
    # Create session to connect to s3 bucket
    session = boto3.Session( 
        aws_access_key_id = "AKIAVBF5FG5KUMIMKE4Y",
        aws_secret_access_key = "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"
    )

    s3 = session.client("s3")
    bucket_name = "nthph-redshift"

    #Loading all df to s3 bucket
    for table, df in df_dict.items(): 
        print(f"Loading {table} to s3")
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index = False)
        csv_buffer.seek(0)
        s3.upload_fileobj(csv_buffer, bucket_name, table + ".csv")
        print("Load successfully \n")

def etl_postgres_s3():
    df_dict = {}
    Extract_from_Posgres(df_dict)
    Transform(df_dict)
    load_S3(df_dict)



        




    


