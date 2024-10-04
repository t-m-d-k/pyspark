from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, max

# Initialize Spark session
spark = SparkSession.builder.appName("PySparkExample5").getOrCreate()

# Load data into a DataFrame
df = spark.read.csv("data/student_scores.csv", header=True, inferSchema=True)

# Create a temporary SQL view
df.createOrReplaceTempView("students")

# Run a basic SQL query
result_df = spark.sql("SELECT * FROM students WHERE score > 80")
result_df.show()

# Perform aggregation using SQL
aggregated_scores = spark.sql("""
    SELECT student_id, SUM(score) AS total_score
    FROM students
    GROUP BY student_id
""")
aggregated_scores.show()

# Calculate average score per subject
avg_score_per_subject = spark.sql("""
    SELECT subject, AVG(score) AS avg_score
    FROM students
    GROUP BY subject
""")
avg_score_per_subject.show()

# Find maximum score in each subject
max_score_subject = spark.sql("""
    SELECT subject, MAX(score) AS max_score
    FROM students
    GROUP BY subject
""")
max_score_subject.show()

# Save SQL results to a file
max_score_subject.write.csv("output/max_score_per_subject", header=True)

# Stop the Spark session
spark.stop()
