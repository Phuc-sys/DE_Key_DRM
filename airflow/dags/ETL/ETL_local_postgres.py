import pandas as pd

from DB_Connection.DB_Connection import connect_postgres
from Transform.transform_customer import transform_Customer
from Transform.transform_customerService import transform_CustomerService
from Transform.transform_BHD import transform_BHD
from Transform.transform_Fimplus import transform_Fimplus
from Transform.transform_DRM import transform_DRM
from Transform.transform_MV import transform_MV


def transform():
    transform_Customer("Customers")
    transform_CustomerService("CustomerService")
    transform_BHD("Log_BHD_MovieID")
    transform_Fimplus("Log_Fimplus_MovieID")
    transform_DRM("Log_Get_DRM_List")
    transform_MV("MV_PropertiesShowVN")

# def connect_postgres():
#     # host = postgres, when get into postgres container, it require password, not for localhost instead
#     conn = psycopg2.connect(database="airflow", user="airflow", password="airflow", host="postgres", port="5432")
#     # All changes will take place
#     conn.autocommit = True
#     return conn

def load_table(path, cursor, table):
    df = pd.read_csv(path)

    records = df.to_records(index = False)
    # string of column names of df
    column_names = ', '.join(df.columns)
    # The number of values to be inserted
    s_list = ', '.join(['%s'] * len(df.columns))

    sql = f"""
        INSERT INTO {table} ({column_names}) VALUES ({s_list});
    """
    # Execute multiple rows in 1 query
    cursor.executemany(sql, records)

def load_postgres():
    conn = connect_postgres()
    cursor = conn.cursor()

    table_list = ["Customers", "CustomerService", "Log_BHD_MovieID", "Log_Fimplus_MovieID", "Log_Get_DRM_List", "MV_PropertiesShowVN"]
    root_dir = "/opt/airflow/transformed_data"

    for table in table_list:
        path = f"{root_dir}/{table}.csv"
        load_table(path, cursor, table)
    
    cursor.close()
    conn.close()

def etl_local_postgres():
    transform()
    load_postgres()