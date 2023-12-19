# Import PySpark SQL functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize a SparkSession in Databricks
spark = SparkSession.builder.appName("DatabricksDataFrameExample").getOrCreate()

# Sample data
data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600),
        ("Robert", "IT", 4100),
        ("Maria", "Finance", 3000),
        ("James", "Sales", 3000),
        ("Scott", "Finance", 3300),
        ("Jen", "Finance", 3900),
        ("Jeff", "Marketing", 3000),
        ("Kumar", "Marketing", 2000),
        ("Saif", "Sales", 4100)]

# Column names
columns = ["EmployeeName", "Department", "Salary"]

# Creating DataFrame
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame
df.show()

# Basic DataFrame operations

# Group by "Department" and calculate average salary
avg_salary = df.groupBy("Department").agg(avg(col("Salary")).alias("AverageSalary"))
avg_salary.show()

# Filter employees with salary greater than 3000
high_earners = df.filter(col("Salary") > 3000)
high_earners.show()

# Stop the SparkSession
spark.stop()
