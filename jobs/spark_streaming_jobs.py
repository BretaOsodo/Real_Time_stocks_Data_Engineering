from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os 
import sys
from dotenv import load_dotenv
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

#configarations from environment 
minio_endpoint= os.getenv('MINIO_ENDPOINT')
minio_access_key= os.getenv('MINIO_ACCESS_KEY')
minio_secret_key=os.getenv('MINIO_SECRET_KEY')
raw_data_path = 's3://apple-stocks-data/stock/data/apple_stock_prices/'
kafka_topic = "apple_stocks_data"
processed_data_path ='s3a://processed-data/streaming-data/'

schema = StructType([
    StructField('Date',StringType(),False),
    StructField('Open',FloatType(),False),
    StructField("Close",FloatType(),False),
    StructField('High',FloatType(),False),
    StructField('Low',FloatType(),False),
    StructField('Volume',IntegerType(),False)
])
spark = SparkSession.builder.appName('apple_stocks_streaming')\
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

def main():

        logger.info(
            'Starting Spark structured Streaming from kafka topic :%s',kafka_topic
        )

        df_raw=(
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers",'kafka:9092')
            .option("subscribe",kafka_topic)
            .option("startingOffsets",'earliest')
            .option("failOnDataLoss",'false')
            .load()
        )

        df_parsed = df_raw.select(from_json(col('value').cast('string'),schema).alias('data')).select("data.*")

        df_clean= df_parsed.filter(
            col('Date').isNotNull()
            & col('Open').isNotNull()
            & col('Close').isNotNull()
            & col('High').isNotNull()
            & col('Low').isNotNull()
            & col('Volume').isNotNull()
        ).withColumn('processed_at',current_timestamp())


        #write clean data to MinIO as Parquet 
        (
            df_clean.writeStream.format('parquet')
            .option('checkpointLocation',"/tmp/spark_checkpoints/raw")
            .option('path',processed_data_path)
            .outputMode("append")
            .queryName('processed_data_to_minio')
            .start()
        )

        logger.info("Streaming job started. Processing events in real-time")
        spark.streams.awaitAnyTermination()
if __name__=="__main__":
        main()
      





