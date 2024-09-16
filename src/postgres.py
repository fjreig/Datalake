from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import concat,concat_ws,to_timestamp
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import os

## Create the SparkSession builder
spark = SparkSession.builder \
    .appName("Postgres") \
    .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.3') \
    .getOrCreate()

## Postgres Config
properties = {
    "user": os.environ['POSTGRES_PASSWORD'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "driver": "org.postgresql.Driver"
}

url_read = "jdbc:postgresql://postgres:5432/" + os.environ['POSTGRES_DB']
table_name_read = "public.pabat_aarr"

df = spark.read.jdbc(url_read, table_name_read, properties=properties)
df.printSchema()

df.show()