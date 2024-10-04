from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("PySparkExample4").getOrCreate()

# Load CSV as DataFrame
df = spark.read.csv("data/employee_data.csv", header=True, inferSchema=True)

# Select columns and show
df.select("Name", "Department", "Salary").show(5)

# Convert DataFrame to RDD
rdd = df.rdd

# Map and filter on RDD
rdd_filtered = rdd.filter(lambda row: row["Salary"] > 50000)
rdd_mapped = rdd_filtered.map(lambda row: (row["Name"], row["Salary"]))

# Convert RDD back to DataFrame
df_filtered = rdd_mapped.toDF(["Name", "Salary"])

# Show the filtered DataFrame
df_filtered.show()

# Save the filtered data
df_filtered.write.csv("output/high_salary_employees", header=True)

# Stop the Spark session
spark.stop()
