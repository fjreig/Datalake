import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import os

## DEFINE SENSITIVE VARIABLES
mongo_url = "mongodb://mongo:27017"

# Configure Spark with necessary packages and Mongo settings
conf = (
    pyspark.SparkConf()
        .setAppName('sales_data_app')
        # Include necessary packages
        .set('spark.jars.packages', 'org.postgresql:postgresql:42.7.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0')
        # Enable Mongo config
        .set("spark.mongodb.read.connection.uri", mongo_url) \
        .set("spark.mongodb.write.connection.uri", mongo_url) \
)

# Start Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Session Started")

## Postgres Config
properties = {
    "user": os.environ['POSTGRES_PASSWORD'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "driver": "org.postgresql.Driver"
}

url_read = "jdbc:postgresql://postgres:5432/" + os.environ['POSTGRES_DB']
table_name_read = "public.pabat_aarr"

df = spark.read.jdbc(url_read, table_name_read, properties=properties)
df.show()

df.write.format("mongodb")\
        .mode("append")\
        .option("database", "Monitorizacion")\
        .option("collection", "pabat_aarr")\
        .save()

# Stop the Spark session
spark.stop()