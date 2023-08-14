from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from ETL.ETL_local_postgres import etl_local_postgres
from ETL.ETL_postgres_s3 import etl_postgres_s3
from ETL.ETL_s3_redshift import Create_Redshift_Schema, Load_s3_Redshift

default_args = {
    "owner": "nthph",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}
# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="etl_postgres_redshift",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:
    
    Create_Posgres_Schema = PostgresOperator(
        task_id = "create-postgres-schema", 
        postgres_conn_id="postgres_schema",
        sql = "postgres_setup/postgres_schema.sql"
    )

    ETL_Local_Postgres = PythonOperator(
        task_id = "etl-local-postgres",
        python_callable=etl_local_postgres
    )

    ETL_Postgres_S3 = PythonOperator(
        task_id = "etl-postgres-s3",
        python_callable=etl_postgres_s3
    )

    Create_Redshift_Schema = PythonOperator(
        task_id = "create-redshift-schema",
        python_callable=Create_Redshift_Schema,
        op_kwargs= {"root_dir" : "/opt/airflow/redshift_setup"}
    )

    Load_S3_Redshift = PythonOperator(
        task_id = "etl-s3-redshift",
        python_callable=Load_s3_Redshift
    )

    Create_Posgres_Schema >> ETL_Local_Postgres >> ETL_Postgres_S3 >> Create_Redshift_Schema >> Load_S3_Redshift