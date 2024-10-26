from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Read dbt table")\
        .getOrCreate()

df = spark\
        .read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .parquet("sparkwarehouse/dim_date")

df.show()

spark.stop()