from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
import pandas as pd
import requests
import json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, DoubleType

# Create SparkSession
spark = SparkSession\
			.builder\
			.master("local[*]")\
			.appName("conversion_rate_api")\
    		.getOrCreate()

# Read api with requests lib to Pandas DataFrame
url = ""
response = requests.get(url)

# Convert dict from api to Pandas DataFrame
df = pd.read_json(response.text)\
    	.reset_index()\
        .rename(columns={'index':'date'})\
        .sort_values('date')

# Convert date columns from string to date columns
df['date'] = pd.to_datetime(df['date'])
df = df.reset_index()
df = df[['date', 'conversion_rate']]

# Create PySpark DataFrame
listdf = spark.createDataFrame(df)

# Print PySpark DataFrame
# print(listdf.show())
# print(listdf.printSchema())
listdf = listdf.withColumn("date" , listdf.date.cast(DateType()))

# Print PySpark DataFrame after convert datetime to date
print('-'*100)
print('Conversion Rate')
print('-'*100)

print(listdf.show(5))
print(listdf.printSchema())

spark.stop()
