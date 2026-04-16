import requests 
from airflow.sdk import task, dag 
from datetime import datetime , timedelta
import os
from dotenv import load_dotenv
import json

@dag(
    dag_id='batch_processing',
    schedule=timedelta(minutes=30),
    start_date=datetime(2026,4,15),
    catchup=False
)

def batch_processing():

    #A task to get the data from alpha vantage api
    @task.python(task_id='get_data_from_alpha_vantage')
    def get_data_from_alpha_vantage():

        load_dotenv()

        url=  f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey={os.getenv('api_key')}"
        request= requests.get(url)
        data = json.loads(request.text)
        data = data['Time Series (Daily)']
        data = json.dumps(data,indent=4)

        return data 
    
    #A task to load the data Into MinIO as a csv data 
    @task.python(task_id='load_to_minio')
    def load_to_minio(data:dict):

        #Minio configaration 
        minio_endpoint_url = os.getenv('MINIO_ENDPOINT')
        minio_access_key = os.getenv('MINIO_ACCESS_KEY')
        minio_secret_key = os.getenv('MINIO_SECRET_KEY')
        minio_bucket = os.getenv('MINIO_BUCKET_ALPHA')

    #set dependencies 
    getting_data = get_data_from_alpha_vantage()

    #set the task flow 
    getting_data

#instantiate the dag 
batch_processing()