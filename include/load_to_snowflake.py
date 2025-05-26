from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable

def load_data_to_snowflake(ti):
    # Extract the blob path from XCom
    blob_path = ti.xcom_pull(task_ids='extract_and_upload_covid_data', key='blob_path')
    if not blob_path:
        raise ValueError("No blob path found in XCom.")
    #file_name = blob_path.split('/')[-1]
    COVID_BLOB_URL = Variable.get("AZURE_BLOB_COVID_DATA")  # Use Airflow Variable for security
    AZURE_SAS_TOKEN = Variable.get("AZURE_SAS_TOKEN")  # Use Airflow Variable for security

    sqls = [
        # 1. Create stage with SAS token
        f"""
        CREATE STAGE IF NOT EXISTS HR.azure_blob_stage_covid
        --URL='azure://sasnwflk.blob.core.windows.net/coviddata'
        URL ='{COVID_BLOB_URL}'  
        CREDENTIALS=(AZURE_SAS_TOKEN='{AZURE_SAS_TOKEN}');
        """,

        # 2. Create file format
        f"""
        CREATE OR REPLACE FILE FORMAT HR.COVID_CSV_FORMAT
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        NULL_IF = ('', 'NULL')
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        DATE_FORMAT = 'MM/DD/YYYY'
        EMPTY_FIELD_AS_NULL = TRUE
        TRIM_SPACE = TRUE;
        """,

        # 3. Copy data from blob into table
        f"""
        COPY INTO HR.COVID_STATS
        FROM @HR.azure_blob_stage_covid
        FILE_FORMAT = HR.COVID_CSV_FORMAT
        ON_ERROR = CONTINUE;
        """
    ]

    # Execute in Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    for sql in sqls:
        snowflake_hook.run(sql)
