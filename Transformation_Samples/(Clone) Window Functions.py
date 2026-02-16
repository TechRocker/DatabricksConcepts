# Databricks notebook source
# DBTITLE 1,Spark DataFrame Initialization and Schema Overview
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([
	Row(a=1, b=4., c='GFG1', d=date(2000, 8, 1),
		e=datetime(2000, 8, 1, 12, 0)),

	Row(a=2, b=8., c='GFG2', d=date(2000, 6, 2),
		e=datetime(2000, 6, 2, 12, 0)),

	Row(a=4, b=5., c='GFG3', d=date(2000, 5, 3),
		e=datetime(2000, 5, 3, 12, 0))
])

df.show()

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Status DataFrame Creation and Schema Display
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
data = [
    ('2024-01-01', 'success'),
    ('2024-01-02', 'success'),
    ('2024-01-03', 'success'),
    ('2024-01-04', 'fail'),
    ('2024-01-05', 'fail'),
    ('2024-01-06', 'success'),
    ('2024-01-07', 'fail')    
]
df_status = spark.createDataFrame(data, schema=['date', 'status'])
df_status.show()
df_status.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTIONS USAGE & SYNTAX	PYSPARK WINDOW FUNCTIONS DESCRIPTION
# MAGIC - row_number()	Returns a sequential number starting from 1 within a window partition
# MAGIC - rank()	Returns the rank of rows within a window partition, with gaps.
# MAGIC - percent_rank()	Returns the percentile rank of rows within a window partition.
# MAGIC - dense_rank()	Returns the rank of rows within a window partition without any gaps. Where as Rank() returns rank with gaps.
# MAGIC - ntile(n)	Returns the ntile id in a window partition
# MAGIC - cume_dist()	Returns the cumulative distribution of values within a window partition
# MAGIC - lag(e, offset)
# MAGIC - lag(columnname, offset)
# MAGIC - lag(columnname, offset, defaultvalue)	In PySpark, the lag() function retrieves the value of a column from a preceding row within the same window. It enables users to compare values across adjacent rows and perform calculations based on the difference or relationship between consecutive values in a DataFrame.
# MAGIC - lead(columnname, offset)
# MAGIC - lead(columnname, offset)
# MAGIC - lead(columnname, offset, defaultvalue)	The lead() function in PySpark retrieves the value of a column from a succeeding row within the same window. It enables users to access values ahead of the current row and perform comparisons or calculations based on future values in a DataFrame.

# COMMAND ----------

# DBTITLE 1,Employee DataFrame Creation and Display
# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Status Partitioned Date Window and Lead Calculation
s_win = Window.partitionBy("status").orderBy("date")
df_status.withColumn("rn", row_number().over(s_win)).show()

(
    df_status.withColumn("date", to_date("date"))
    .withColumn("lead", lead("date", 1).over(s_win))
    .withColumn("datediff", date_diff("lead", "date"))
    # .select(
    #     col("date").alias("start_date"),
    #     when(col("datediff") > 1, col("date")).otherwise(col("lead")).alias("end_date"),
    #     "status",
    # )
    # .orderBy("start_date")
).show()

# COMMAND ----------

# DBTITLE 1,row_number
# row_number() example
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import rank
df.withColumn("rank",rank().over(windowSpec)) \
    .show()

# The rank() function assigns ranks to rows within a window partition based on the specified ordering. When there are duplicate values for the ordering column(s), rank() assigns the same rank to all tied rows. However, it then skips the next rank numbers equal to the number of tied rows. For example, if three rows have the same value and are ranked as 1, the next rank will be 4, skipping 2 and 3.

# COMMAND ----------

from pyspark.sql.functions import dense_rank
df.withColumn("dense_rank",dense_rank().over(windowSpec)) \
    .show()

# COMMAND ----------

from pyspark.sql.functions import percent_rank
df.withColumn("percent_rank",percent_rank().over(windowSpec)) \
    .show()

# COMMAND ----------

from pyspark.sql.functions import ntile
df.withColumn("ntile",ntile(2).over(windowSpec)) \
    .show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("GroupByStatus").getOrCreate()

# Sample data
data = [
    ("2024-01-01", "success"),
    ("2024-01-02", "success"),
    ("2024-01-03", "success"),
    ("2024-01-04", "fail"),
    ("2024-01-05", "fail"),
    ("2024-01-06", "success"),
    ("2024-01-07", "fail")
]

# Create DataFrame
df = spark.createDataFrame(data, ["date", "status"])

# Convert date column to proper format
df = df.withColumn("date", col("date").cast("date"))

# Define window for lag function
window_spec = Window.orderBy("date")

# Create a column to detect group changes
df = df.withColumn("prev_status", lag("status").over(window_spec))

df = df.withColumn("group",
                   when(col("prev_status") != col("status"), col("date"))
                   .otherwise(None))

# Fill the group column forward
df = df.withColumn("group", df.group.fillna(method="ffill"))

# Aggregate to get start_date and end_date for each group
result_df = df.groupBy("group", "status").agg(
    col("group").alias("start_date"),
    col("date").alias("end_date")
).select("start_date", "end_date", "status")

# Show final DataFrame
result_df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("GroupByStatus").getOrCreate()

# Sample data
data = [
    ("2024-01-01", "success"),
    ("2024-01-02", "success"),
    ("2024-01-03", "success"),
    ("2024-01-04", "fail"),
    ("2024-01-05", "fail"),
    ("2024-01-06", "success"),
    ("2024-01-07", "fail")
]

# Create DataFrame
df = spark.createDataFrame(data, ["date", "status"])

# Convert date column to proper format
df = df.withColumn("date", col("date").cast("date"))

# Define window for lag function
window_spec = Window.orderBy("date")

# Create a column to detect group changes
df = df.withColumn("prev_status", lag("status").over(window_spec))

df = df.withColumn("group",
                   when(col("prev_status") != col("status"), col("date"))
                   .otherwise(None))

# # Fill the group column forward
# df = df.withColumn("group", df.group.fillna(method="ffill"))

# # Aggregate to get start_date and end_date for each group
# result_df = df.groupBy("group", "status").agg(
#     col("group").alias("start_date"),
#     col("date").alias("end_date")
# ).select("start_date", "end_date", "status")

# Show final DataFrame
df.show()

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# Sample data
data = [
    ("2024-01-01", "success"),
    ("2024-01-02", "success"),
    ("2024-01-03", "success"),
    ("2024-01-04", "fail"),
    ("2024-01-05", "fail"),
    ("2024-01-06", "success"),
    ("2024-01-07", "fail")
]

# Create DataFrame
df = spark.createDataFrame(data, ["date", "status"])

df.show()
# Assuming the input DataFrame is named 'df'
window = Window.orderBy("date")

# Identify status changes by comparing with the previous row
df = df.withColumn("prev_status", F.lag("status").over(window))

# Flag rows where status changes (or is the first row)
df = df.withColumn("change_flag", 
                   F.when((F.col("status") != F.col("prev_status")) | F.col("prev_status").isNull(), 1).otherwise(0))

# Create a group ID using cumulative sum over the change flags
df = df.withColumn("group_id", F.sum("change_flag").over(window))

df.show()
# Aggregate to get start_date, end_date, and status per group
result = df.groupBy("group_id", "status") \
    .agg(F.min("date").alias("start_date"), 
         F.max("date").alias("end_date")) \
    .drop("group_id") \
    .orderBy("start_date")

result.show()

# COMMAND ----------


