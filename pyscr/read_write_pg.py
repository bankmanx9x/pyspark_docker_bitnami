import pyspark
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("chapter1PostgreSQL")\
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.5.4')\
        .getOrCreate()
sqlContext = SparkSession(spark)
#Dont Show warning only error
spark.sparkContext.setLogLevel("ERROR")

postgredf = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://rds-db-test-yml-4.chh1ytozzee1.ap-southeast-1.rds.amazonaws.com:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "public.mpg") \
    .option("user", "postgres") \
    .option("password", "Ub44332148$")\
    .load()

#postgredf.printSchema()

postgredf.show()