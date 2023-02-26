from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator

#from common.week_3.config import DATA_TYPES, normalized_columns

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryCreateExternalTableOperator

PROJECT_ID = "theoreticalmonkey"
DESTINATION_BUCKET = "documents-used-airflow"
BQ_DATASET_NAME = "airflow_week3"



@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "owner" : 'abel camacho'
    },
    tags =['bigquery','example']
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


    @task(task_id = "extract_data_from_zip_file")
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


    @task(task_id = "move_data_to_gcs")
    def load(unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "extract" task, formats
        columns to be BigQuery-compliant, and writes data to GCS.
        """

        files_names = ['energy','weather']
        for index, df in enumerate(unzip_result):
            df.columns = df.columns.str.replace(" ", "_")
            df.columns = df.columns.str.replace("/", "_")
            df.columns = df.columns.str.replace("-", "_")
            df.to_parquet("gs://documents-used-airflow/"+files_names[index]+"_dataset.parquet")

 

        
    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator( task_id = "create_dataset", project_id = PROJECT_ID,
                                           dataset_id = BQ_DATASET_NAME,
                                           exists_ok= True)


#    @task_group()
#    def create_external_tables():
        
#        create_external_table_weather = BigQueryCreateExternalTableOperator(
#    task_id = "create_external_table_weather",
#    destination_project_dataset_table = f"{BQ_DATASET_NAME}.weather_features",
#    bucket = "documents-used-airflow",
#    source_objects = ["weather_dataset.parquet"]#,
#            source_format = "PARQUET"
#            )
        
#        create_external_table_energy = BigQueryCreateExternalTableOperator(
#    task_id="create_external_table_energy",
#    destination_project_dataset_table=f"{BQ_DATASET_NAME}.energy_features",
#    bucket = "documents-used-airflow",
#    source_ob

    @task
    def create_external_tables():
        from google.cloud import bigquery
        client = bigquery.Client()

        sql = """
        CREATE OR REPLACE EXTERNAL TABLE theoreticalmonkey.airflow_week3.energy_features
        OPTIONS(
        format = 'PARQUET',
        uris = ['gs://documents-used-airflow/energy_dataset.parquet']
        );  
        CREATE OR REPLACE EXTERNAL TABLE theoreticalmonkey.airflow_week3.weather_features
        OPTIONS(
        format = 'PARQUET',
        uris = ['gs://documents-used-airflow/weather_dataset.parquet']
        );  
        """
        # Start the query, passing in the extra configuration.
        query_job = client.query(sql)  # Make an API request.
        query_job.result()     
 

       
            
 #       create_external_table_weather = create_external_table_weather
        
 #       create_external_table_energy = create_external_table_energy

        

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
          `theoreticalmonkey.airflow_week3.energy_features`   
"""
# Start the query, passing in the extra configuration.
        query_job = client.query(sql)  # Make an API request.
        query_job.result()  
        
        
    @task_group
    def produce_normalized_views():
        
        create_normalized_energy_view = BigQueryCreateEmptyTableOperator(
        task_id = "create_normalized_energy_view",
        dataset_id = BQ_DATASET_NAME,
        table_id="normalized_energy_view",
        view={
            "query" : f"""SELECT 
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
          generation_hydro_pumped_storage_consumption FROM `{PROJECT_ID}.{BQ_DATASET_NAME}.energy_features`""",
            "useLegacySql": False,
        }
    )


        create_normalized_weather_view = BigQueryCreateEmptyTableOperator(
             task_id = "create_normalized_weather_view",
             dataset_id = BQ_DATASET_NAME,
             table_id="normalized_weather_view",
             view={
            "query" : f"""SELECT
            CAST(dt_iso AS TIMESTAMP) AS TimeStamp,
            city_name,
            TEMP,
            pressure,
            humidity,
            wind_speed,
            wind_deg,
            rain_1h,
            rain_3h,
            snow_3h,
            clouds_all,
          FROM `{PROJECT_ID}.{BQ_DATASET_NAME}.weather_features`""",
            "useLegacySql": False,
        }
    )

        

        create_normalized_energy_view = create_normalized_energy_view
        
        create_normalized_weather_view = create_normalized_weather_view
        
        
        
    produce_joined_view = BigQueryCreateEmptyTableOperator(
             task_id = "produce_joined_view",
             dataset_id = BQ_DATASET_NAME,
             table_id="enery_weather_view",
             view={
            "query" : f"""SELECT
w.*
,e.total_load_actual
,e.price_day_ahead
,e.price_actual
,e.generation_fossil_hard_coal
,e.generation_fossil_gas
,e.generation_fossil_brown_coal_lignite
,e.generation_fossil_oil
,e.generation_other_renewable
,e.generation_waste
,e.generation_biomass
,e.generation_other
,e.generation_solar
,e.generation_hydro_water_reservoir
,e.generation_nuclear
,e.generation_hydro_run_of_river_and_poundage
,e.generation_wind_onshore
,e.generation_hydro_pumped_storage_consumption
FROM
 `{PROJECT_ID}.{BQ_DATASET_NAME}.normalized_weather_view` AS w
 INNER JOIN 
  `{PROJECT_ID}.{BQ_DATASET_NAME}.normalized_energy_view` AS e
  ON
w.TimeStamp = e.TimeStamp""",
            "useLegacySql": False,
        }
    )

    unzip_task = extract()
    load_task = load(unzip_task)
    create_bigquery_dataset_task = create_bigquery_dataset#()
    load_task >> create_bigquery_dataset_task
    external_table_task = create_external_tables()
    create_bigquery_dataset_task >> external_table_task
    normal_view_task = produce_normalized_views()
    external_table_task >> normal_view_task
    joined_view_task = produce_joined_view
    normal_view_task >> joined_view_task


data_warehouse_transform_dag = data_warehouse_transform_dag()
