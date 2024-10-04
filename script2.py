from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, sum

# Initialize Spark session
spark = SparkSession.builder.appName("PySparkExample2").getOrCreate()

# Load two datasets
df_orders = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
df_customers = spark.read.csv("data/customers.csv", header=True, inferSchema=True)

# Show first few rows of each DataFrame
df_orders.show(5)
df_customers.show(5)

# Join two DataFrames on customer_id
joined_df = df_orders.join(df_customers, df_orders["customer_id"] == df_customers["id"], "inner")
joined_df.show(10)

# Calculate total orders per customer
total_orders_per_customer = joined_df.groupBy("customer_id").agg(sum("order_amount").alias("Total_Amount"))
total_orders_per_customer.show(5)

# Using window functions to rank orders by amount
window_spec = Window.partitionBy("customer_id").orderBy(col("order_amount").desc())

# Assign row numbers to each order
ranked_orders = joined_df.withColumn("rank", row_number().over(window_spec))
ranked_orders.show(10)

# Filter top 3 orders per customer
top_orders = ranked_orders.filter(col("rank") <= 3)
top_orders.show()

# Save the top orders DataFrame
top_orders.write.csv("output/top_orders", header=True)

# Stop the Spark session
spark.stop()
