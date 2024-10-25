from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, sum

spark = SparkSession\
            .builder\
            .master("local[*]")\
            .appName("pyspark test windows function")\
            .getOrCreate()

data=[(1,'manish','india',10000),
      (2,'rani','india',50000),
      (3,'sunny','UK',5000),
      (4,'sohan','UK',25000),
      (5,'mona','india',2000)]

columns=['id','name','country','salary']

df =spark.createDataFrame(data,columns)
df.show()

window = Window.partitionBy("country").orderBy("salary")

df.withColumn("row_number", row_number().over(window)).show()
df.withColumn("rank", rank().over(window)).show()
df.withColumn("dense_rank", dense_rank().over(window)).show()

spark.stop()