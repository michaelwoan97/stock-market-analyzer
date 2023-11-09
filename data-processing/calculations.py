import sys
sys.path.append("../")
import os

from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("stock-analyzer") \
        .config("spark.jars", "drivers/postgresql-42.6.0.jar"). \
            getOrCreate()

# Read data from PostgreSQL
jdbc_url = f"jdbc:postgresql://{os.environ.get('DB_HOST')}:{os.environ.get('DB_PORT')}/{os.environ.get('DB_NAME')}"
properties = {"user": os.environ.get('DB_USER'), "password": os.environ.get('DB_PASSWORD'), "driver": "org.postgresql.Driver"}
df = spark.read.jdbc(jdbc_url, '"Users"', properties=properties)

df.printSchema()
# Perform data processing operations
# result_df = df.filter(df["column_name"] > 10).groupBy("another_column").count()

# # Show the results
# result_df.show()

# Stop the Spark session
spark.stop()
