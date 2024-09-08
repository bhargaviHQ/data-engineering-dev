from pyspark.sql.functions import sum, expr, col,count

# To calculate the total revenue per customer
def calculate_revenue_per_customer(df):
    df = df.withColumn('Revenue', expr('Quantity * UnitPrice'))
    revenue_per_customer = df.groupBy('CustomerID').agg(sum('Revenue').alias('TotalRevenue'))
    return revenue_per_customer

# Returns the top (n) countries by total revenue
def top_countries_by_revenue(df, top_n=10):
    df = df.withColumn('Revenue', expr('Quantity * UnitPrice'))
    revenue_per_country = df.groupBy('Country').agg(sum('Revenue').alias('TotalRevenue'))
    top_countries = revenue_per_country.orderBy(col('TotalRevenue').desc()).limit(top_n)
    return top_countries

def customerWithMostPurchase(df):
    customer_purchase_counts = df.groupBy('CustomerID').agg(count('InvoiceNo').alias("PurchaseCount"))
    customer_with_most_purchases = customer_purchase_counts.orderBy(col('PurchaseCount').desc()).first()
    return customer_with_most_purchases
