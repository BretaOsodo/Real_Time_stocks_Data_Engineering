import requests 
from airflow.sdk import task, dag 
from datetime import datetime , timedelta
import os
from dotenv import load_dotenv
import json
import logging 
from pydantic import BaseModel,Field,PositiveInt,field_validator
from typing import Annotated
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

class Ibm_data(BaseModel):
    Date: datetime
    Open: float
    Close: float
    High: float
    Low: float
    Volume: float

    @field_validator('Date')
    @classmethod
    def validate_date(cls,value):
        if value > datetime.now():
            raise ValueError("Date cannot be in the future")
        return value 
    
    @field_validator('Open','Close','High','Low','Volume')
    @classmethod
    def validate_if_positive(cls,value):
        if value <= 0:
            raise ValueError('stock proces cannot be negative and also stock price cnnot be zero')
        return value 

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
    
    # a task to transform the alpha vantage data 
    @task.python(task_id='transform_data')
    def transform_data(data:dict):
        if isinstance(data,str):
            import json 
            data = json.loads(data)

        transformed=[]

        for date, values in data.items():
            transformed.append({
                "Date": date,
                "Open": float(values["1. open"]),
                "High": float(values["2. high"]),
                "Low": float(values["3. low"]),
                "Close": float(values["4. close"]),
                "Volume": float(values["5. volume"])
            })

        return transformed
    #validate the data we are getting from minio
    @task.python(task_id='validate_data')
    def validate_data(data):
       
        validated=[]
        for records in data:
            try:
                object=Ibm_data(**records)
                validated.append(object.model_dump())
                logger.info('Successfully validated the data we are getting from the Alpha vantage Api')
            except Exception as e:
                logger.error(f"Validation failed for the record:{records};error:{e}")
        return validated
                
            

    
    #A task to load the data Into MinIO as a csv data 
    @task.python(task_id='load_to_minio')
    def load_to_minio(data:dict):
        import io
        import pandas as pd

        load_dotenv()

        #converting the parsed data to dictionary 
        parsed_data = data

        #convert to dataframe 
        df=pd.DataFrame(parsed_data)

        #Minio configaration 
        minio_endpoint_url = os.getenv('MINIO_ENDPOINT')
        minio_access_key = os.getenv('MINIO_ACCESS_KEY')
        minio_secret_key = os.getenv('MINIO_SECRET_KEY')
        minio_bucket = os.getenv('MINIO_BUCKET_ALPHA')

        #validate the configarations 
        logger.info('Validating the minio configarations')
        if not all([minio_access_key,minio_bucket,minio_endpoint_url,minio_secret_key]):
            missing = [val for val, var in [
                ('MINIO_ACCESS_KEY',minio_access_key),
                ('MINIO_SECRET_KEY',minio_secret_key),
                ('MINIO_BUCKET_ALPHA',minio_bucket),
                ('MINIO_ENDPOINT',minio_endpoint_url)
            ]if not var]

            raise ValueError (f'Missing requirement{missing}')
        #connect to minio
        logger.info('Connecting to Minio')
         
        try:
            ibm_client = boto3.client(
            's3',
            aws_secret_access_key=minio_secret_key,
            aws_access_key_id=minio_access_key,
            endpoint_url=minio_endpoint_url,
            verify= False)

        except Exception as e:
            logger.error(f"failed to connect to Minio:{e}")
        #list all the available buckets
        logger.info('Listing all the available buckets in Minio')
        try:
            buckets= ibm_client.list_buckets() 
            bucket_name = [b['Name'] for b in buckets['Buckets']]
            logger.info(f'Founnd {len(bucket_name)}. And the buckets are {list(bucket_name)}')
        except Exception as e:
            logger.error('Failed to list the buckets in Minio')
        
        #check if the ibm_bucket exists and if it doesn't create a new one 
        logger.info('Checking if the Buckets exists ')
        try:
            ibm_client.head_bucket(Bucket=minio_bucket)
            logger.info("Bucket already exists")
        except:
            ibm_client.create_bucket(Bucket=minio_bucket)
            logger.info("Bucket created successfully")

        #convert data into a csv form 
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer,index=True)
        csv_content=csv_buffer.getvalue().encode('utf-8')

        #create an object key
        object_key ='stock/data/ibm_stock_prices.csv'

        #upload to minio
        try:
            ibm_client.put_object(
                Bucket= minio_bucket,
                Key=object_key,
                Body= csv_content,
                ContentType='text/csv'
            )

            logger.info(f'Successfully loaded the data to {minio_bucket}')
        except Exception as e:
            logger.error(f'Failed to load data to {minio_bucket} bucket')

            raise



        


    #set dependencies 
    data = get_data_from_alpha_vantage()
    cleaned=transform_data(data)
    validated= validate_data(cleaned)
    loading= load_to_minio(validated)

    #set the task flow 
    data>>cleaned>>validated >> loading

#instantiate the dag 
batch_processing()