from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

#from common.week_3.config import DATA_TYPES, normalized_columns


# PROJECT_ID = # Modify HERE
# DESTINATION_BUCKET = # Modify HERE
# BQ_DATASET_NAME = # Modify HERE


@dag(
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    ) 
def data_warehouse_transform_dag():
    """
    ### Data Warehouse Transform DAG
    This DAG performs four operations:
        1. Extracts zip file into two dataframes
        2. Loads these dataframes into parquet files on GCS, with valid column names
        3. Builds external tables on top of these parquet files
        4. Builds normalized views on top of the external tables
        5. Builds a joined view on top of the normalized views, joined on time
    """


    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned
        """
        from google.cloud import storage
        from zipfile import ZipFile
        import io
        # Initialize the storage client
        storage_client = storage.Client()
        #Define the name of the bucket and the folder
        bucket_name = "documents-used-airflow"
        folder_name = "energy-consumption-generation-prices-and-weather.zip"
        # Get the bucket object
        bucket = storage_client.get_bucket(bucket_name)
        # Get the blob (file) object
        blob = bucket.blob(folder_name)
        content = blob.download_as_string()
        # Create a file-like buffer to receive the content of the file
        filename = io.BytesIO(content)

        dfs = [pd.read_csv(ZipFile(filename).open(i)) for i in ZipFile(filename).namelist()]
        
        return dfs


    @task
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "extract" task, formats
        columns to be BigQuery-compliant, and writes data to GCS.
        """

#        from airflow.providers.google.cloud.hooks.gcs import GCSHook
#        client = GCSHook().get_conn()       
#        bucket = client.get_bucket(DESTINATION_BUCKET)

        for index, df in enumerate(unzip_result):
            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.replace("/", "_")
            df.columns = df.columns.str.replace("-", "_")
#            bucket.blob(f"week-3/{DATA_TYPES[index]}.parquet").upload_from_string(df.to_parquet(), "text/parquet")

            from google.cloud import bigquery
            client = bigquery.Client()

            dataset = bigquery.Dataset('theoreticalmonkey.airflow_week3')

            # TODO(developer): Specify the geographic location where the dataset should reside.
            dataset.location = "US"

            # Send the dataset to the API for creation, with an explicit timeout.
            # Raises google.api_core.exceptions.Conflict if the Dataset already
            # exists within the project.
            dataset = client.create_dataset(dataset, timeout=30,exists_ok=True) 
            print(df.dtypes)

    @task
    def create_bigquery_dataset():
#        from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
#        EmptyOperator(task_id='placeholder')
        # TODO Modify here to create a BigQueryDataset if one does not already exist
        # This is where your tables and views will be created
        
        from google.cloud import bigquery
        client = bigquery.Client()

        dataset = bigquery.Dataset('theoreticalmonkey.airflow_week3')

        # TODO(developer): Specify the geographic location where the dataset should reside.
        dataset.location = "US"

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = client.create_dataset(dataset, timeout=30, 
                               exists_ok=True)  

        
    @task
    def create_external_tables():
 #       from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
#        EmptyOperator(task_id='placeholder')

        # TODO Modify here to produce two external tables, one for each data type, referencing the data stored in GCS

        # When using the BigQueryCreateExternalTableOperator, it's suggested you use the table_resource
        # field to specify DDL configuration parameters. If you don't, then you will see an error
        # related to the built table_resource specifying csvOptions even though the desired format is 
        # PARQUET.
        from google.cloud import bigquery
        client = bigquery.Client()

        sql = """
        CREATE OR REPLACE EXTERNAL TABLE theoreticalmonkey.airflow_week3.energy_dataset
        OPTIONS(
        format = 'CSV',
        uris = ['gs://documents-used-airflow/energy_dataset.csv']
        );  
        CREATE OR REPLACE EXTERNAL TABLE theoreticalmonkey.airflow_week3.weather_features
        OPTIONS(
        format = 'CSV',
        uris = ['gs://documents-used-airflow/weather_features.csv']
        );  
        """
        # Start the query, passing in the extra configuration.
        query_job = client.query(sql)  # Make an API request.
        query_job.result()     
        
        

    def produce_select_statement(timestamp_column: str, columns: List[str]) -> str:
        # TODO Modify here to produce a select statement by casting 'timestamp_column' to 
        # TIMESTAMP type, and selecting all of the columns in 'columns'
        print(1)
        from google.cloud import bigquery
        client = bigquery.Client()

        sql = """
        SELECT
          CAST(time AS TIMESTAMP) TimeStamp,
          total_load_actual,
          price_day_ahead,
          price_actual,
          generation_fossil_hard_coal,
          generation_fossil_gas,
          generation_fossil_brown_coal_lignite,
          generation_fossil_oil,
          generation_other_renewable,
          generation_waste,
          generation_biomass,
          generation_other,
          generation_solar,
          generation_hydro_water_reservoir,
          generation_nuclear,
          generation_hydro_run_of_river_and_poundage,
        generation_wind_onshore,
          generation_hydro_pumped_storage_consumption
                FROM
          `theoreticalmonkey.airflow_week3.energy_dataset`   
"""
# Start the query, passing in the extra configuration.
        query_job = client.query(sql)  # Make an API request.
        query_job.result()  
        
        
        

    @task
    def produce_normalized_views():
        #from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce views for each of the datasources, capturing only the essential
        # columns specified in normalized_columns. A key step at this stage is to convert the relevant 
        # columns in each datasource from string to time. The utility function 'produce_select_statement'
        # accepts the timestamp column, and essential columns for each of the datatypes and build a 
        # select statement ptogrammatically, which can then be passed to the Airflow Operators.
        #EmptyOperator(task_id='placeholder')
        from google.cloud import bigquery
        client = bigquery.Client()

        sql = """
CREATE OR REPLACE VIEW 
  `theoreticalmonkey.airflow_week3.normalized_energy_dataset`
  AS
  SELECT
  CAST(time AS TIMESTAMP) TimeStamp,
  total_load_actual,
  price_day_ahead,
  price_actual,
  generation_fossil_hard_coal,
  generation_fossil_gas,
  generation_fossil_brown_coal_lignite,
  generation_fossil_oil,
  generation_other_renewable,
  generation_waste,
  generation_biomass,
  generation_other,
  generation_solar,
  generation_hydro_water_reservoir,
  generation_nuclear,
  generation_hydro_run_of_river_and_poundage,
  generation_wind_onshore,
  generation_hydro_pumped_storage_consumption
FROM
  `theoreticalmonkey.airflow_week3.energy_dataset`
  
"""
        # Start the query, passing in the extra configuration.
        query_job = client.query(sql)  # Make an API request.
        query_job.result()
        

    @task
    def produce_joined_view():
        #from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
        # TODO Modify here to produce a view that joins the two normalized views on time
        #EmptyOperator(task_id='placeholder')
        pass


    unzip_task = extract()
    load_task = load(unzip_task)
    create_bigquery_dataset_task = create_bigquery_dataset()
    load_task >> create_bigquery_dataset_task
    external_table_task = create_external_tables()
    create_bigquery_dataset_task >> external_table_task
    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task
    joined_view_task = produce_joined_view()
    normal_view_task >> joined_view_task


data_warehouse_transform_dag = data_warehouse_transform_dag()
