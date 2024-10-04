from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, max, min

# Initialize Spark session
spark = SparkSession.builder.appName("PySparkExample1").getOrCreate()

# Load data
df = spark.read.csv("data/sales_data.csv", header=True, inferSchema=True)

# Display schema
df.printSchema()

# Show first 10 rows
df.show(10)

# Select specific columns
df.select("Product", "Sales", "Region").show(5)

# Filter data: Sales > 5000
filtered_df = df.filter(col("Sales") > 5000)
filtered_df.show(10)

# Group by product and calculate total sales per product
grouped_df = df.groupBy("Product").agg(sum("Sales").alias("Total_Sales"))
grouped_df.show(5)

# Average sales by region
avg_sales_region = df.groupBy("Region").agg(avg("Sales").alias("Avg_Sales"))
avg_sales_region.show(5)

# Get the maximum sales for each region
max_sales_region = df.groupBy("Region").agg(max("Sales").alias("Max_Sales"))
max_sales_region.show(5)

# Get the minimum sales for each product
min_sales_product = df.groupBy("Product").agg(min("Sales").alias("Min_Sales"))
min_sales_product.show(5)

# Sorting based on total sales
sorted_sales = grouped_df.orderBy(col("Total_Sales").desc())
sorted_sales.show(5)

# Perform a cache to speed up repeated access
sorted_sales.cache()

# Save the results
sorted_sales.write.csv("output/sorted_sales", header=True)

# Stop the Spark session
spark.stop()
