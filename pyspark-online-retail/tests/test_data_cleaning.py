import unittest
from pyspark.sql import SparkSession
from src.data_cleaning import clean_data

class TestDataCleaning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test").getOrCreate()
        data = [("A", -1, None), ("B", 5, 123), ("C", 10, None)]
        cls.df = cls.spark.createDataFrame(data, ["InvoiceNo", "Quantity", "CustomerID"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_data(self):
        df_cleaned = clean_data(self.df)
        self.assertEqual(df_cleaned.filter(df_cleaned['Quantity'] <= 0).count(), 0)
        self.assertEqual(df_cleaned.filter(df_cleaned['CustomerID'].isNull()).count(), 0)

if __name__ == '__main__':
    unittest.main()
