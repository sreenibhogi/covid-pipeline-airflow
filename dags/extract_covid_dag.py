from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from include.extract_covid_data_to_blob import  get_covid_data_and_upload_to_blob  # adjust import
from include.load_to_snowflake import load_data_to_snowflake

with DAG(
    dag_id='covid_data_to_blob_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None, #Run Manually or once
    catchup=False,
    tags=['covid', 'azure', 'blob']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_and_upload_covid_data',
        python_callable=get_covid_data_and_upload_to_blob
    )

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_data_to_snowflake
    )

    extract_task >> load_task
