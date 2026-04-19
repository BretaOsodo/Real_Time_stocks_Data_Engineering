from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import logging 
import os 
from dotenv import load_dotenv



load_dotenv()

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

raw_minio_path = 's3a://ibm-stock-data/stock/data/ibm_stock_prices.csv'
processed_ibm_data = 's3a://processed-ibm-data/streaming-data'

#starting a spark session 
spark = SparkSession.builder.appName('ibm_stocks')\
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key",'minio_admin')\
    .config("spark.hadoop.fs.s3a.secret.key",'123456789')\
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3") \
    .config("spark.hadoop.fs.s3a.endpoint",'http://minio:9000')\
    .config("spark.hadoop.fs.s3a.path.style.access",'true')\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.sql.adaptive.enabled','true')\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled",'true')\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .getOrCreate()

ibm_schema= StructType([
    StructField('Date',DateType(),False),
    StructField("Open",FloatType(),False),
    StructField("High",FloatType(),False),
    StructField("Low",FloatType(),False),
    StructField("Close",FloatType(),False),
    StructField("Volume",FloatType(),False)
])


df = spark.read\
    .schema(ibm_schema)\
    .option('header','true')\
    .option('inferSchema','true')\
    .csv(raw_minio_path)
print("COUNT:", df.count())
df.show(5)

#=========Transformations==========
#1. Convert data and time components 
df = df.withColumn('Date', to_date(col("Date"), "yyyy-MM-dd"))\
    .withColumn('year',year('Date'))\
    .withColumn('month',month('Date'))\
    .withColumn('day',day("Date"))

#2. Calculating the basic metrics 
df = df.withColumn('daily_returns',(col("Close")-col("Open"))/col("Open"))\
    .withColumn('price_range',col("High")-col("Low"))\
    .withColumn("typical_price",(col("High")+col("Low")+col("Close"))/3)

# 3. Selct the final columns 
final_df=df.select(
    "Date",
    "year",
    "month",
    "day",
    "Open",
    "High",
    "Low",
    "Close",
    "Volume",
    "daily_returns",
    "price_range",
    "typical_price"
)


#save as parquet 

final_df.write\
    .mode('overwrite')\
    .partitionBy('year','month')\
    .format("parquet") \
    .save(processed_ibm_data)
