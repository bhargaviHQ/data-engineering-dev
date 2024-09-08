import unittest
from pyspark.sql import SparkSession
from src.aggregations import calculate_revenue_per_customer

class TestAggregations(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test").getOrCreate()
        data = [("A", 10, 1.5, 123), ("B", 5, 2.0, 456)]
        cls.df = cls.spark.createDataFrame(data, ["InvoiceNo", "Quantity", "UnitPrice", "CustomerID"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_calculate_revenue_per_customer(self):
        df_revenue = calculate_revenue_per_customer(self.df)
        expected_revenue = df_revenue.filter(df_revenue['CustomerID'] == 123).collect()[0]['TotalRevenue']
        self.assertEqual(expected_revenue, 10 * 1.5)

if __name__ == '__main__':
    unittest.main()
