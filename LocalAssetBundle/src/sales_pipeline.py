import argparse
from pyspark.sql import SparkSession
from transformations import clean_sales, aggregate_sales

def run_sales_pipeline(input_path, bronze_table, silver_table, gold_table):

    spark = SparkSession.builder.getOrCreate()

    # ----------------
    # Bronze: Auto Loader
    # ----------------
    df_raw = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .load(input_path)
    )

    (
        df_raw.writeStream
              .format("delta")
              .outputMode("append")
              .option("checkpointLocation", f"/tmp/{bronze_table}_checkpoint")
              .toTable(bronze_table)
    )

    # ----------------
    # Silver: Transformation
    # ----------------
    df_bronze = spark.read.table(bronze_table)
    df_silver = clean_sales(df_bronze)

    df_silver.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    # ----------------
    # Gold: Aggregation
    # ----------------
    df_gold = aggregate_sales(df_silver)

    df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-table", required=True)
    args = parser.parse_args()

    run_sales_pipeline(
        args.input_path,
        args.bronze_table,
        args.silver_table,
        args.gold_table,
    )
