from pyspark.sql import SparkSession
from data_cleaning import clean_data
from aggregations import calculate_revenue_per_customer, top_countries_by_revenue

# Initialize Spark session
spark = SparkSession.builder \
    .appName("UCI Online Retail Analysis") \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .getOrCreate()

# Load dataset
data_path = "data/OnlineRetail.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Data cleaning
df_cleaned = clean_data(df)

# # Save cleaned data rdd to CSV files
# # Each partition is written to seperate files
# df_cleaned.write.mode("overwrite").csv("output/cleaned_data_files", header=True)
# # Convert to a single parittion file before saving output 
# df_cleaned.coalesce(1).write.mode("overwrite").csv("output/cleaned_data.csv", header=True)

# Aggregations
revenue_per_customer = calculate_revenue_per_customer(df_cleaned)
top_10_countries = top_countries_by_revenue(df_cleaned)

# Show results
revenue_per_customer.show(5)
top_10_countries.show(5)

df_cleaned.printSchema()
# Stop Spark session
spark.stop()
