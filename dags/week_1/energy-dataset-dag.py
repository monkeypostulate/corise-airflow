from datetime import datetime
from typing import List

import pandas as pd
from airflow.decorators import dag, task # DAG and task decorators for interfacing with the TaskFlow API

@dag(
    # This defines how often your DAG will run, or the schedule by which your DAG runs. In this case, this DAG
    # will run daily
    schedule_interval="@daily",
    # This DAG is set to run for the first time on January 1, 2021. Best practice is to use a static
    # start_date. Subsequent DAG runs are instantiated based on scheduler_interval
    start_date=datetime(2023, 1, 1),
    # When catchup=False, your DAG will only run for the latest schedule_interval. In this case, this means
    # that tasks will not be run between January 1, 2021 and 30 mins ago. When turned on, this DAG's first
    # run will be for the next 30 mins, per the schedule_interval
    catchup=False,
    default_args={
        "retries": 2, # If a task fails, it will retry 2 times.
    },
    tags=['example']) # If set, this tag is shown in the DAG view of the Airflow UI
def energy_dataset_dag():
    """
    ### Basic ETL Dag
    This is a simple ETL data pipeline example that demonstrates the use of
    the TaskFlow API using two simple tasks to extract data from a zipped folder
    and load it to GCS.
    """

    @task
    def extract() -> List[pd.DataFrame]:
        """
        #### Extract task
        A simple task that loads each file in the zipped file into a dataframe,
        building a list of dataframes that is returned.
        """
        from google.cloud import storage
        from zipfile import ZipFile
        import io

        # Initialize the storage client
        storage_client = storage.Client()
        
        # Define the name of the bucket and the folder
        bucket_name = "documents-used-airflow"
        folder_name = "energy-consumption-generation-prices-and-weather.zip"

        # Get the bucket object
        bucket = storage_client.get_bucket(bucket_name)
        # Get the blob (file) object
        blob = bucket.blob(folder_name)
        content = blob.download_as_string()
        # Create a file-like buffer to receive the content of the file
        file = io.BytesIO(content)

        with ZipFile(file, 'r') as zip_ref:
            zip_ref.extractall("unziped_folder")
    
        energy_dataset = pd.read_csv("unziped_folder/energy_dataset.csv")
        weather_features = pd.read_csv("unziped_folder/weather_features.csv")
        
        return [energy_dataset, weather_features]
        

    @task
    def transform(data: List[pd.DataFrame]):#unzip_result: List[pd.DataFrame]):
        """
        #### Load task
        A simple "load" task that takes in the result of the "transform" task, prints out the 
        schema, and then writes the data into GCS as parquet files.
        """
        from datetime import datetime

        data_combined = data[0].merge(data[1],
                    left_on = 'time',
                    right_on = 'dt_iso')

        day = list(map(lambda i: datetime.strptime(i, '%Y-%m-%d %H:%M:%S%z').day, data_combined.time))
        month = list(map(lambda i: datetime.strptime(i, '%Y-%m-%d %H:%M:%S%z').month, data_combined.time))
        year = list(map(lambda i: datetime.strptime(i, '%Y-%m-%d %H:%M:%S%z').year, data_combined.time))

        data_combined['day'] = day
        data_combined['month'] = month
        data_combined['year'] = year
        data_combined['one'] = 1
        
        return [data_combined]
        
        
    @task
    def load(data: List[pd.DataFrame]):#unzip_result: List[pd.DataFrame]):        
#        data[0].to_csv("gs://documents-used-airflow/energy_weather_data.csv")
        data[0].to_parquet('gs://documents-used-airflow/energy_weather_data.gzip',
              compression='gzip')  
        
    # TODO Add task linking logic here
    datasets_extract = extract()
    datasets_transformed = transform(datasets_extract)
    load(datasets_transformed)
    

energy_dataset_dag = energy_dataset_dag()
