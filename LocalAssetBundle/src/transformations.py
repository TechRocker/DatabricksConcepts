from pyspark.sql.functions import col

def filter_high_value(df):
    return df.filter(col("amount") > 100)