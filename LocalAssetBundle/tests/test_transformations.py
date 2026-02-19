import pytest
from pyspark.sql import SparkSession
from chispa import assert_df_equality
from transformations import clean_sales, aggregate_sales

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()

def test_clean_sales(spark):
    data = [(1,50),(2,200),(3,None)]
    df = spark.createDataFrame(data,["id","amount"])
    result = clean_sales(df)

    expected = spark.createDataFrame([(2,200)],["id","amount"])
    assert_df_equality(result, expected)

def test_aggregate_sales(spark):
    data = [(1,200),(2,300)]
    df = spark.createDataFrame(data,["id","amount"])
    result = aggregate_sales(df).collect()

    assert result[0]["total_sales"] == 500
