from pyspark.sql import SparkSession
from transformations import filter_high_value

spark = SparkSession.builder.getOrCreate()

data = [(1, 50), (2, 200)]
df = spark.createDataFrame(data, ["id", "amount"])

result = filter_high_value(df)
result.show()
