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
        
        return data 
    
    @task.python(task_id='check_and_convert_data_to_json')
    def check_and_convert_data_to_json(data):
        """
        Check data type and convert to json 
        Handles : DataFrame, CSV string, dict, list
        """

        #check the type of incoming data 
        data_type = type(data).__name__
        logger.info("Checking the type of incoming data")
        print(f"Received Data type: {data_type}")

        #initialize variable for json output 
        json_output= None 

        #Case1: Pandas Dataframe
        if isinstance(data, pd.DataFrame):
            print('Detected: pandas Dataframa')
            print(f"shape: {data.shape}")
            print(f"columns:{list(data.columns)}")

            json_output=data.to_json(orient='records',date_format='iso')

        #case2 : CSV string
        elif isinstance(data,str) and (data.endswith('.csv') or',' in data[:100]):
            print('Detected a CSV file ')

            #read the csv into dataframe first 
            from io import StringIO
            df = pd.read_csv(StringIO(data))
            json_output=df.to_json(orient='records',data_format='iso')

        #case 3: Dictionary 
        elif isinstance(data,dict):
            print('Detected a Dictionary ')
            json_output=json.dumps(data, default=str)

        #case 4 : list 
        elif isinstance(data, list):
            print("detected a list")
            json_output=json.dumps(data, default=str)

        #case 5 : Already json string 
        elif isinstance(data,str):
            print('Detected string(Checking if its JSON)')
            try:
                #validate if it's already json
                json_output=json.loads(data)
                print('Already Valid Json')
            except json.JSONDecodeError:
                print('INVALID JSON')
                json_output= None 
        else:
            print(f"Unknown datatype:{data_type}")
            json_output=json.dumps({'value':str(data)}, default= str)

        return json_output
    
    #a task to return no data if there is no data from the api 
    @task.branch(task_id='check_data-availability')
    def check_data_availability(json_data):
        if json_data is None:
            return 'no_data'
        else:
            return 'check_and_convert_data_to_json'
        
    # A task to return empty data is there is no data 
    @task.python(task_id='no_data')
    def no_data():
        logger.warning('No data from the yfinance API')       
    
    #set dependencies 
    getting_data = get_stock_data()
    branch = check_data_availability(getting_data)

    json_data = check_and_convert_data_to_json(getting_data)
    no_data_results= no_data()


    #set the task flow 
    getting_data >> branch
    branch >> [json_data,no_data_results]

#instanciate the dag 
Streaming_with_kafka()
        
