from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from DLTBundle import taxis


def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5
