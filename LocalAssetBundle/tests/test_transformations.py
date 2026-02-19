import pytest
from pyspark.sql import SparkSession
# from src.transformations import filter_high_value
# from local_asset_bundle.transformations import filter_high_value

from transformations import filter_high_value
from chispa import assert_df_equality

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_filter_high_value(spark):
    df = spark.createDataFrame([(1,50),(2,200)],["id","amount"])
    result = filter_high_value(df)

    expected = spark.createDataFrame([(2,200)],["id","amount"])

    assert_df_equality(result, expected)
