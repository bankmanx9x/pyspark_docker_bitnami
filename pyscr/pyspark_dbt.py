from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

spark = SparkSession\
        .builder\
        .appName("Read dbt table")\
        .getOrCreate()

load_dotenv()
PGSQL_host = os.environ['PGSQL_host']
PGSQL_port = os.environ['PGSQL_port']
PGSQL_db = os.environ['PGSQL_db']
PGSQL_user = os.environ['PGSQL_user']
PGSQL_password = os.environ['PGSQL_password']

df = spark\
        .read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .parquet("sparkwarehouse/dim_date")

df.show()

df\
    .write\
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://172.29.0.3:{PGSQL_port}/{PGSQL_db}") \
    .option("driver","org.postgresql.Driver") \
    .option("dbtable", "public.dim_date") \
    .option("user", PGSQL_user) \
    .option("password", PGSQL_password) \
    .mode("overwrite") \
    .save()

print("Write Spark WH to PostgreSQL Table Successfully")

spark.stop()