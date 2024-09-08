from pyspark.sql.functions import col, to_timestamp,when

# The data cleaning file contains the steps to clean the 
# dataset by removing the null values, invalid transactions,
# converting the InvoiceDate datatype to timestamp
def clean_data(df):
    df_cleaned = df.dropna(subset=["InvoiceNo", "StockCode", "CustomerID"])

    df_cleaned = df_cleaned.withColumn("Description", when(col("Description").isNull(), "Unknown")
                                       .otherwise(col("Description")))
    
    df_cleaned = df_cleaned.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))

    df_cleaned = df_cleaned.withColumn("TotalPrice", col("Quantity") * col("UnitPrice"))

    return df_cleaned
