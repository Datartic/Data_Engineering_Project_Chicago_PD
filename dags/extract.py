import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from google.oauth2 import service_account

# Set up logging
logger = logging.getLogger(__name__)

@dag(schedule_interval="0 10 * * *", start_date=datetime(2022, 2, 15), catchup=False, tags=['load_gcp'])
def extract_and_load():
    @task()
    def sql_extract():
        try:
            hook = MsSqlHook(mssql_conn_id="sqlserver")
            sql = """ SELECT 
                TABLE_SCHEMA AS SchemaName,
                TABLE_NAME AS TableName
            FROM 
                INFORMATION_SCHEMA.TABLES
            WHERE 
                TABLE_TYPE = 'BASE TABLE' """
            df = hook.get_pandas_df(sql)
            tbl_dict = df.to_dict('dict')  # Keep the original structure
            logger.info("Extracted Table Dictionary: %s", tbl_dict)  # Log the extracted dictionary
            
            # Transform tbl_dict into a list of dictionaries
            transformed_tbl_dict = [
                {'SchemaName': tbl_dict['SchemaName'][i], 'TableName': tbl_dict['TableName'][i]}
                for i in range(len(tbl_dict['SchemaName']))
            ]
            logger.info("Transformed Table Dictionary: %s", transformed_tbl_dict)  # Log the transformed dictionary
            
            return transformed_tbl_dict
        except Exception as e:
            logger.error("Data extract error: %s", str(e))

    @task()
    def gcp_load(tbl_dict: list):
        try:
            logger.info("Starting data load for tables: %s", tbl_dict)  # Log the tables to be loaded
            credentials = service_account.Credentials.from_service_account_file('/opt/airflow/creds/boreal-graph-444300-j7-4f7223790ea0.json')
            project_id = "boreal-graph-444300-j7"
            dataset_ref = "Chicago_PD_Data"
            
            for table_info in tbl_dict:
                logger.info("Processing table_info: %s", table_info)  # Log the current table info
                if isinstance(table_info, dict):
                    schema_name = table_info['SchemaName']
                    table_name = table_info['TableName']
                    logger.info("Schema: %s, Table: %s", schema_name, table_name)  # Log schema and table names
                    
                    # Construct the SQL query using schema and table name
                    sql = f'SELECT * FROM {schema_name}.{table_name}'
                    hook = MsSqlHook(mssql_conn_id="sqlserver")
                    df = hook.get_pandas_df(sql)
                    
                    # Load the DataFrame to BigQuery
                    destination_table = f'{dataset_ref}.src_{schema_name}_{table_name}'
                    logger.info("Importing rows to %s...", destination_table)  # Log the destination table
                    df.to_gbq(destination_table=destination_table, project_id=project_id, credentials=credentials, if_exists="replace")
                    
                    logger.info("Completed loading table %s from schema %s to BigQuery.", table_name, schema_name)
                else:
                    logger.warning("Unexpected format for table_info: %s", table_info)  # Log unexpected format
        except Exception as e:
            logger.error("Data load error: %s", str(e))

    # Call task functions
    tbl_dict = sql_extract()
    gcp_load(tbl_dict)

gcp_extract_and_load = extract_and_load()