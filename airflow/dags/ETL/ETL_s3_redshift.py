from DB_Connection.DB_Connection import connect_redshift 

def Create_Redshift_Schema(root_dir):
    conn = connect_redshift()
    cursor = conn.cursor()

    path = f"{root_dir}/redshift_schema.sql"
    with open(path, 'r') as file :
        redshift_sql = file.read()

    cursor.execute(redshift_sql)
    print("Create redshift schema successfully")

    cursor.close()
    conn.close()

def Load_s3_Redshift():
    conn = connect_redshift()
    cursor = conn.cursor()

    table_list = ["Customers", "CustomerService", "Log_BHD_MovieID", "Log_Fimplus_MovieID", "Log_Get_DRM_List", "MV_PropertiesShowVN", "TVOD", "SVOD", "Time"]
    bucket_name = "nthph-redshift"
    schema = "Warehouse_Key_DRM"
    iam_role = "arn:aws:iam::346141570901:role/redshift-role"

    for table in table_list:
        query = f"""
                COPY {schema}.{table}
                FROM 's3://{bucket_name}/{table}.csv'
                credentials 'aws_iam_role={iam_role}'
                FORMAT AS CSV
                IGNOREHEADER 1
                FILLRECORD;
            """
        cursor.execute(query)
        print(f"Load table {table} successfully")
    
    cursor.close()
    conn.close()

