import pyspark
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("chapter1_Read PostgreSQL")\
        .config("spark.sql.warehouse.dir", 'file:///opt/bitnami/spark/sparkwarehouse') \
        .config("spark.sql.catalogImplementation", "hive")\
        .enableHiveSupport() \
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
            .option("url", "jdbc:postgresql://172.21.0.4:5432/postgres") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "public.empl") \
            .option("user", "postgres") \
            .option("password", "postgres")\
            .load()

print(postgredf.show())
print(postgredf.printSchema())

postgredf\
.write\
.mode('overwrite')\
.option("path", "file:///opt/bitnami/spark/sparkwarehouse")\
.saveAsTable("emp")

spark.stop()