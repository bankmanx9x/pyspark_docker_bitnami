import pyspark
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("chapter1_Read PostgreSQL")\
        .getOrCreate()

"""
sqlContext = SparkSession(spark)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")
.config('spark.jars.packages', 'org.postgresql:postgresql:42.5.4')\
"""

postgredf = spark\
            .read\
            .format("jdbc") \
            .option("url", "jdbc:postgresql://172.25.0.4:5432/postgres") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "public.empl") \
            .option("user", "postgres") \
            .option("password", "postgres")\
            .load()

print(postgredf.show())
print(postgredf.printSchema())

spark.stop()