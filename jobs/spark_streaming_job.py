from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os 
from dotenv import load_dotenv
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

spark = SparkSession.builder.appName('apple_stocks_streaming')\
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,"
            "org.apache.hadoop:hadoop-aws:3.5.0,"
            "software.amazon.awssdk:s3:2.42.28")\
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key",os.getenv('MINIO_ACCESS_KEY'))\
    .config("spark.hadoop.fs.s3a.secret.key",os.getenv('MINIO_SECRET_KEY'))\
    .config("spark.hadoop.fs.s3a.endpoint",os.getenv('MINIO_ENDPOINT'))\
    .config("spark.hadoop.fs.s3a.path.style.access",'true')\
    .config('spark.sql.adaptive.enabled','true')\
    .config("spark.sql.adaptive.coalescePartitions.enabled",'true')\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider')\
    .getOrCreate()