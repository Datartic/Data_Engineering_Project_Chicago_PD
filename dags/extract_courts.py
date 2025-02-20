import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from google.oauth2 import service_account

# Set up logging
logger = logging.getLogger(__name__)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['datartictutorials@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

@dag(default_args=default_args,schedule_interval="0 10 * * *", start_date=datetime(2022, 2, 15), catchup=False, tags=['load_gcp'])
def extract_and_load_courts():
    """
    DAG to extract table data from SQL Server and load it into Google BigQuery.
    """

    @task()
    def sql_extract():
        """
        Extract table metadata from SQL Server.
        Returns a transformed list of dictionaries with schema and table names.
        """
        try:
            # Initialize SQL Server hook
            hook = MsSqlHook(mssql_conn_id="sql_server_courts")
            
            # Define the SQL query to extract metadata
            sql_query = """ 
                SELECT 
                    TABLE_SCHEMA AS SchemaName,
                    TABLE_NAME AS TableName
                FROM 
                    INFORMATION_SCHEMA.TABLESS
                WHERE 
                    TABLE_TYPE = 'BASE TABLE'
            """
            
            # Execute query and convert results to DataFrame
            df = hook.get_pandas_df(sql_query)
            logger.info("Successfully extracted table metadata: %s", df.head())
            
            # Transform DataFrame to a list of dictionaries
            table_metadata = [
                {'SchemaName': row['SchemaName'], 'TableName': row['TableName']}
                for _, row in df.iterrows()
            ]
            
            logger.info("Transformed table metadata: %s", table_metadata)
            return table_metadata
        except Exception as e:
            logger.error("Error during SQL extraction: %s", str(e))
            raise

    @task()
    def gcp_load(table_metadata: list):
        """
        Load data from SQL Server tables into Google BigQuery.
        """
        try:
            # Initialize GCP credentials and project details
            credentials_path = '/opt/airflow/creds/boreal-graph-444300-j7-4f7223790ea0.json'
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            project_id = "boreal-graph-444300-j7"
            dataset_ref = "Courts"
            
            # Iterate through table metadata to load each table
            for table_info in table_metadata:
                if isinstance(table_info, dict):
                    schema_name = table_info['SchemaName']
                    table_name = table_info['TableName']
                    
                    logger.info("Processing Schema: %s, Table: %s", schema_name, table_name)
                    
                    # Construct SQL query for table data
                    sql_query = f"SELECT * FROM {schema_name}.{table_name}"
                    
                    # Extract data from SQL Server
                    hook = MsSqlHook(mssql_conn_id="sql_server_courts")
                    df = hook.get_pandas_df(sql_query)
                    logger.info("Extracted %d rows from %s.%s", len(df), schema_name, table_name)
                    
                    # Define BigQuery destination table
                    destination_table = f"{dataset_ref}.src_{schema_name}_{table_name}"
                    
                    # Load data into BigQuery
                    logger.info("Loading data into BigQuery table: %s", destination_table)
                    df.to_gbq(
                        destination_table=destination_table,
                        project_id=project_id,
                        credentials=credentials,
                        if_exists="replace"
                    )
                    
                    logger.info("Successfully loaded table %s from schema %s into BigQuery.", table_name, schema_name)
                else:
                    logger.warning("Invalid table metadata format: %s", table_info)
        except Exception as e:
            logger.error("Error during GCP load: %s", str(e))
            raise

    # Define task dependencies
    table_metadata = sql_extract()
    gcp_load(table_metadata)

# Instantiate the DAG
gcp_extract_and_load = extract_and_load_courts()
