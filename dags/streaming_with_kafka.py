from airflow.sdk import dag,task
from datetime import datetime,timedelta

import logging
import pandas as pd 
import json
import os 
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import io
import pandas as pd 
import subprocess

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)



@dag(
    dag_id='Streaming_with_kafka',
    schedule=timedelta(seconds=30),
    start_date = datetime(2026,4,11),
    end_date= None,
    catchup=False 
)
def Streaming_with_kafka():
    
    @task.python(task_id='get_stock_data')
    def get_stock_data():
        #import yfinance package 
        import yfinance as yf
        data = yf.download(
            tickers='AAPL',
            start="2024-01-01",
            end= datetime.now().strftime("%Y-%m-%d"),
            interval="1d")
        
        
        #check if the columns are multiindex 
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
            logger.info('Flattened Multiindex Columns ')
        #make data a column 
        data_reset= data.reset_index()

        #convert to a serializable format 
        records =[]
        for _, row in data_reset.iterrows():
            record={}
            for col,value in row.items():
                if isinstance(value,pd.Timestamp):
                    record[col]=value.isoformat() #convert to ISO format string 
                elif isinstance(value,(pd.Series,pd.DataFrame)):
                    #Handles nested data structure like multi-index columns 
                    record[col]=str(value)

                else:
                    record[col]= value
            records.append(record)
        logger.info(f"Converted {len(records)} records to serializable format")
        return records 
    #create a task to validate the data before loading to MinIo 
    @task.python(task_id='validate_json_data')
    def validate_json_data(data):
        logger.info('validating the Data we get from the api')
        #import the pydantic and typing packages 
        from pydantic import BaseModel, PositiveInt, Field, field_validator
        from decimal import Decimal
        from typing import Annotated
        from datetime import datetime 

        class Stock(BaseModel):
            Open: Annotated [
                Decimal,
                Field (
                    title='Opening Price',
                    description='The Opening price os apple stocks',
                    gt=0,
                    max_digits=30,
                    decimal_places=20
                )
            ]
            Close: Annotated [
                Decimal,
                Field(
                    title='Closing Proce',
                    description='Closing price of the apple stock',
                    gt=0,
                    max_digits=30,
                    decimal_places=20
                )
            ]

            High: Annotated [
                Decimal,
                Field (
                    title='High Price',
                    description='This is the highest price of the day of the apple stock',
                    gt=0,
                    max_digits=30,
                    decimal_places=20
                )
            ]

            Low : Annotated [
                Decimal,
                Field (
                    title='Lower price',
                    description='The lowest Price of the apple stock of that day',
                    gt=0,
                    max_digits=30,
                    decimal_places=20
                )
            ]
            Volume : Annotated[
                PositiveInt,
                Field(
                    title='Volume of Stock',
                    description='The volume of the apple stocks that day',
                    gt=0
                )
            ]

            Date: Annotated[
                datetime,
                Field(
                    title='Date',
                    description='The date the stocks were traded',

                )
            ]
            #======================================================
            """
            Returned Data has Decimal places more than 4.
            Return data with 4 decimal places 
            """
            #======================================================

            @field_validator('Date')
            @classmethod
            def validate_date(cls,value):
                if value > datetime.now():
                    raise ValueError ('Date cannot be in the Future')
                return value
            
            

        validated=[]
        for record in data:
            try:
                object= Stock(**record)
                validated.append(object.model_dump())

            except Exception as e:
                logger.error(f"validation failed for the record: {record} error:{e}")
            
        return validated



    @task.branch(task_id='check_data-availability')
    def check_data_availability(data):
        if json_data is None or len(data)==0:
            logger.info("No data available, routing to no_data task")
            return 'no_data'
        else:
            logger.info(f"Data available with {len(data)} records,routing to check_and_convert_data_to_jason task")
            return 'check_and_convert_data_to_json'
    
    @task.python(task_id='check_and_convert_data_to_json')
    def check_and_convert_data_to_json(data):
       """
       Ensure data is properly formatted as JSON string 
       Data is already a dict?list from get_stock_price
       """
       
       #convert to JSON string 
       try:
           logger.info("Converting to JSON string")
           json_string=json.dumps(data,default=str)
           logger.info(f"Successfully converted data to JSon . Size:{len(json_string)}")
           return json_string
       except Exception as e:
           logger.error(f"Failed to convert data to JSON:{e}")
           return None 
        
    # A task to return empty data is there is no data 
    @task.python(task_id='no_data')
    def no_data():
        """
        Handles case when no data is available
        Returns No data 
        """
        logger.warning('No data from the yfinance API')     
    

    # A task to load the Raw data to Minio 
    @task.python(task_id='Load_raw_to_minio')  
    def Load_raw_to_minio(data):

        load_dotenv()

        #converting the parsed data to dictionary 
        parsed_data = json.loads(data)

        #convert to dataframe 
        df=pd.DataFrame(parsed_data if isinstance(parsed_data,list) else [parsed_data])


        #minio configarations 
        minio_endpoint= os.getenv('MINIO_ENDPOINT')
        minio_access_key = os.getenv('MINIO_ACCESS_KEY')
        minio_secret_key = os.getenv('MINIO_SECRET_KEY')
        minio_bucket = os.getenv('MINIO_BUCKET')

        #validate the configurations 
        if not all([minio_access_key,minio_bucket,minio_endpoint,minio_secret_key]):
            missing = [val for val, var in [
                ('MINIO_ENDPOINT',minio_endpoint),
                ('MINIO_ACCESS_KEY',minio_access_key),
                ('MINIO_SECRET_KEY',minio_secret_key),
                ('MINIO_BUCKET',minio_bucket)
            ] if not val]
            raise ValueError(f"Missing requirements:{missing}")
        
        #connect to minio 
        logger.info('Connecting to MinIO')
        apple_client= boto3.client(
            's3',
            aws_access_key_id = minio_access_key,
            aws_secret_access_key= minio_secret_key,
            endpoint_url = minio_endpoint,
            verify = False 
        )

        #List all the existing buckets 
        logger.info('Listing the Existing buckets in MinIO')
        try:
            buckets=apple_client.list_buckets()
            buckets_name = [b['Name'] for b in buckets['Buckets']]
            logger.info(f'Found {len(buckets_name)} and the are: {list(buckets_name)}')
        except Exception as e:
            logger.error(f'No bucket found {e}')

        #check if the bucket exists and it doesn't create one 
        try:
            apple_client.create_bucket(Bucket=minio_bucket)
            logger.info(f'Successfully created bucket:{minio_bucket}')
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['BucketAlreadyOwnedByYou', 'BucketAlreadyExists']:
                logger.error(f'Bucket already exists:{minio_bucket}')
            else:
                logger.error(f'Failed to create the bucket:{e}')
                raise 
        #convert data to csv in memory 
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer,index= False)
        csv_content=csv_buffer.getvalue().encode('utf-8')

        #create the file path 
        object_key=f"stock/data/apple_stock_prices.csv"

        #upload to minio bucket 
        apple_client.put_object(
            Bucket=minio_bucket,
            Key= object_key,
            Body= csv_content,
            ContentType='text/csv'

        )

        logger.info(f'Successfully uploaded{object_key} to bucket {minio_bucket}')
        
        #returning the path of the bucket
        logger.info('Returning the path of the file ')
        return f's3://{minio_bucket}/{object_key}'




    @task.python(task_id='kafa_producer')
    def kafka_producer(data):
        from confluent_kafka import Producer 

        #producer configaration 
        producer_config={
            "bootstrap.servers":"kafka:9092"
        }

        producer= Producer(producer_config)

        #delivery function 
        def delivery_report(err,msg):
            if err:
                print(f'Message delivery failed: {err}')

            else:
                print(f'Message delivered to {msg.topic()}')

        #send the message 
        value= json.dumps(data).encode('utf-8')
        producer.produce(
            topic='apple_stocks_data',
            value=value,
            callback=delivery_report
        )
        producer.poll(0)
        #wait for all messages 
        remaining=producer.flush(10)

        if remaining> 0:
            print(f"{remaining} message not delivered!")
        else:
            print("All messages delivered successfully!")
    


    #set dependencies 
    getting_data = get_stock_data()
    validating_data=validate_json_data(getting_data)
    branch = check_data_availability(validating_data)

    json_data = check_and_convert_data_to_json(validating_data)
    no_data_results= no_data()

    producing_data = kafka_producer(json_data)
    loading_minio= Load_raw_to_minio(json_data)
    


    # Flow
    getting_data >> validating_data>>branch
    branch >> json_data

    #parallel execution 
    json_data >> producing_data
    json_data >> loading_minio

    

    #no data path 
    branch >> no_data_results

#instanciate the dag 
Streaming_with_kafka()
        
