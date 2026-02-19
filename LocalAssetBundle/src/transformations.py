from pyspark.sql.functions import col, sum as sum_

def bronze_ingest(df):
    # Basic pass-through (no logic)
    return df

def clean_sales(df):
    # Filter out invalid and low values
    return df.filter(col("amount").isNotNull()).filter(col("amount") > 100)

def aggregate_sales(df):
    # Compute total sales
    return df.groupBy().agg(sum_("amount").alias("total_sales"))
