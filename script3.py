from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("PySparkExample3").getOrCreate()

# Create RDD from a text file
rdd = spark.sparkContext.textFile("data/transactions.txt")

# Map each line into key-value pairs (product, sales amount)
rdd_kv = rdd.map(lambda line: (line.split(",")[0], float(line.split(",")[2])))

# Reduce by key to get total sales for each product
rdd_total_sales = rdd_kv.reduceByKey(lambda a, b: a + b)

# Collect results
collected_data = rdd_total_sales.collect()
for product, total_sales in collected_data:
    print(f"Product: {product}, Total Sales: {total_sales}")

# Perform a filter transformation (sales > 10000)
high_sales_rdd = rdd_total_sales.filter(lambda x: x[1] > 10000)
high_sales_data = high_sales_rdd.collect()
for product, total_sales in high_sales_data:
    print(f"Product: {product}, Total Sales: {total_sales}")

# Perform count action
product_count = rdd_total_sales.count()
print(f"Total number of products: {product_count}")

# Save results to a file
high_sales_rdd.saveAsTextFile("output/high_sales_rdd")

# Stop the Spark session
spark.stop()
