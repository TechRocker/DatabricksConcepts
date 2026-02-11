from pyspark import pipelines as dp
from pyspark.sql.functions import col


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


# @dp.table
# def sample_trips_lake_flow_asset_bundle():
#     return spark.read.table("samples.nyctaxi.trips")


@dp.table(
    name="bronze_addresses",
    table_properties={"quality": "bronze"},
    comment="Raw address data from source cloud file",
)
def create_bronze_addresses():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")       
        .load("/Volumes/circuitbox/landing/operational_data/addresses")
    ).select(
        "*",
        F.col("_metadata.file_path").alias("input_file_path"),
        F.current_timestamp().alias("Ingest_timestamp"),
    )