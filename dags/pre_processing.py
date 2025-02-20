import logging
import os
from datetime import datetime
from airflow.decorators import dag, task
from google.cloud import bigquery
from google.oauth2 import service_account

# Set up logging
logger = logging.getLogger(__name__)

# Path to the base directory containing SQL folders
SQL_BASE_DIR = '/opt/airflow/dags/sql/'

@dag(schedule_interval="0 10 * * *", start_date=datetime(2022, 2, 15), catchup=False, tags=['pre_processing'])
def pre_process():
    @task()
    def sql_extract(folder_name: str):
        try:
            # Construct the full path to the specified folder
            sql_folder_path = os.path.join(SQL_BASE_DIR, folder_name)
            # List all SQL files in the specified folder
            sql_files = [f for f in os.listdir(sql_folder_path) if f.endswith('.sql')]
            # Sort the SQL files by name
            sql_files.sort()
            logger.info("Found SQL files in folder '%s': %s", folder_name, sql_files)
            return sql_files
        except Exception as e:
            logger.error("Error reading SQL files: %s", str(e))
            return []

    @task()
    def execute_sql_files(sql_files: list,folder_name: str):
        try:
            credentials = service_account.Credentials.from_service_account_file('/opt/airflow/creds/boreal-graph-444300-j7-4f7223790ea0.json')
            client = bigquery.Client(credentials=credentials, project=credentials.project_id)

            for sql_file in sql_files:
                sql_file_path = os.path.join(SQL_BASE_DIR, folder_name, sql_file)
                with open(sql_file_path, 'r') as file:
                    sql = file.read()
                    logger.info("Executing SQL from file: %s", sql_file)
                    # Execute the SQL query
                    query_job = client.query(sql)
                    query_job.result()  # Wait for the job to complete
                    logger.info("Successfully executed SQL from file: %s", sql_file)
        except Exception as e:
            logger.error("Error executing SQL files: %s", str(e))

    # Call task functions
    folder_name='pre_processing'
    sql_files = sql_extract(folder_name)
    execute_sql_files(sql_files,folder_name)

# Example of how to call the DAG with a specific folder name
pre_process()