import psycopg2
#import redshift_connector

def connect_postgres():
    # Parameters for establishing connection to postgreSQL
    connect_params = {
        "host": "localhost",
        "port": "5432",
        "database": "airflow",
        "user": "airflow",
        "password": "airflow"
    }

    # Connect
    conn = psycopg2.connect(**connect_params)
    conn.autocommit = True
    return conn


def connect_redshift():
    # conn = redshift_connector.connect( 
    #     iam = True,
    #     database = 'main_db',
    #     db_user = 'admin',
    #     password = 'nthphAdmin1',
    #     cluster_identifier = 'ntph-redshift-cluster',
    #     access_key_id = 'AKIAVBF5FG5KUMIMKE4Y',  
    #     secret_access_key = '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', region = 'us-east-1'
    # )
    conn = psycopg2.connect(
        host='ntph-redshift-cluster.cfokpgt4ulnn.us-east-1.redshift.amazonaws.com',
        user='admin',
        port=5439,
        password='nthphAdmin1',
        dbname='main_db'
    )
    conn.autocommit = True
    return conn