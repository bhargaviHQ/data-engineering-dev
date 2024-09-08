import pytest
from pyspark.sql import SparkSession
from src.aggregations import customerWithMostPurchase


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Spark Test") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_get_customer_with_most_purchases(spark):
    # Create a test DataFrame
    data = [
        ("1", "A", "12/1/2010  8:26:00 AM", 10.0),
        ("2", "A", "12/1/2010  8:27:00 AM", 20.0),
        ("3", "B", "12/1/2010  8:28:00 AM", 15.0),
        ("4", "C", "12/1/2010  8:29:00 AM", 30.0),
        ("5", "A", "12/1/2010  8:30:00 AM", 25.0),
    ]
    
    columns = ["InvoiceNo", "CustomerID", "InvoiceDate", "UnitPrice"]
    
    df = spark.createDataFrame(data, schema=columns)
    
    # Call the function under test
    result = customerWithMostPurchase(df)
    
    # Expected result
    expected_customer_id = 'A'
    expected_purchase_count = 3
    print(result)
    assert result["CustomerID"] == expected_customer_id
    assert result["PurchaseCount"] == expected_purchase_count