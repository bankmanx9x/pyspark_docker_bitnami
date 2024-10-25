from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

spark = SparkSession\
        .builder\
        .appName("chapter1_Read PostgreSQL with env")\
        .getOrCreate()

"""
sqlContext = SparkSession(spark)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")
.config('spark.jars.packages', 'org.postgresql:postgresql:42.5.4')\
"""
# read value from .env file
load_dotenv()
PGSQL_host = os.environ['PGSQL_host']
PGSQL_port = os.environ['PGSQL_port']
PGSQL_db = os.environ['PGSQL_db']
PGSQL_user = os.environ['PGSQL_user']
PGSQL_password = os.environ['PGSQL_password']

postgredf = spark\
            .read\
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{PGSQL_host}:{PGSQL_port}/{PGSQL_db}") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "public.empl") \
            .option("user", PGSQL_user) \
            .option("password", PGSQL_password)\
            .load()

print(postgredf.show())
print(postgredf.printSchema())

spark.stop()