from airflow.sdk import dag,task
from datetime import datetime,timedelta
import logging
import pandas as pd 
import json


logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)



@dag(
    dag_id='Streaming_with_kafka',
    schedule=timedelta(minutes=30),
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
                    record[col]=str(value)
            records.append(record)
        logger.info(f"Converted {len(records)} records to serializable format")
        return records 


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
    branch = check_data_availability(getting_data)

    json_data = check_and_convert_data_to_json(getting_data)
    no_data_results= no_data()

    producing_data=kafka_producer(json_data)


    #set the task flow 
    getting_data >> branch
    branch >>json_data>> producing_data
    branch>>no_data_results

#instanciate the dag 
Streaming_with_kafka()
        
