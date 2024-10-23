from pyspark.sql import SparkSession
from pyspark import SparkFiles
from pyspark.sql.functions import *

spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName("pyspark_datetime")\
        .getOrCreate()

data = [("2022-03-15", "2022-03-16 12:34:56.789"), 
        ("2022-03-01", "2022-03-16 01:23:45.678")]

df = spark.createDataFrame(data, ["date_col", "timestamp_col"])

df.show()

print(df.printSchema())

df.select("date_col").show()
print("print datecol columns only")

df.select("date_col", \
          date_format("date_col", "yyyy-MMMM-dd")
          .alias("new_cols"))\
        .show()

spark.stop()