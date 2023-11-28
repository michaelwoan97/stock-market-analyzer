from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
import os
from dotenv import load_dotenv
from pyspark.sql.window import Window
from database import create_connection, execute_sql

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

# Specify the path to the PostgreSQL JDBC driver JAR file
postgres_jar_path = "./drivers/postgresql-42.6.0.jar"

def create_spark_session():
    return SparkSession.builder \
        .appName("StockDataCleaning") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars", postgres_jar_path) \
        .getOrCreate()

def read_database_properties():
    return {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER"),
        "url": os.getenv("DB_URL"),
    }

def read_data_from_postgresql(spark, db_properties, schema_name, table_name):
    try:
        return spark.read.jdbc(url=db_properties["url"], table=table_name, properties=db_properties)
    except Exception as e:
        print("Error reading data from PostgreSQL:")
        print(e)
        return None

def define_custom_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("stock_id", StringType(), True),
        StructField("ticker_symbol", StringType(), True),
        StructField("date", DateType(), True),
        StructField("low", FloatType(), True),
        StructField("open", FloatType(), True),
        StructField("high", FloatType(), True),
        StructField("volume", IntegerType(), True),
        StructField("close", FloatType(), True)
    ])

def apply_custom_schema(data, custom_schema):
    for field in custom_schema.fields:
        data = data.withColumn(field.name, data[field.name].cast(field.dataType))
    return data

def preprocess_stock_data(stock_data):
    # Your data preprocessing steps here
    return processed_data

def calculate_moving_averages(stock_data):
    # Your moving averages calculation steps here
    return moving_averages_data

def calculate_bollinger_bands(stock_data):
    # Your Bollinger Bands calculation steps here
    return bollinger_bands_data

def calculate_relative_indexes(stock_data):
    # Your relative indexes calculation steps here
    return relative_indexes_data

def update_tables_to_original_structures(connection):
    # Your code to update tables to original structures
    pass

def stock_technical_data_processing(stock_data, operation_mode):
    spark = create_spark_session()
    db_properties = read_database_properties()
    schema_name = "public"  # Replace with your actual schema name
    table_name = f'{schema_name}."Stocks"'

    stock_data = read_data_from_postgresql(spark, db_properties, schema_name, table_name)

    if stock_data is not None:
        custom_schema = define_custom_schema()
        stock_data = apply_custom_schema(stock_data, custom_schema)

        processed_data = preprocess_stock_data(stock_data, )
        moving_averages_data = calculate_moving_averages(processed_data)
        bollinger_bands_data = calculate_bollinger_bands(processed_data)
        relative_indexes_data = calculate_relative_indexes(processed_data)

        # Update tables to original structures
        connection = create_connection()
        update_tables_to_original_structures(connection)

        # Stop the Spark session when you're done
        spark.stop()

# if __name__ == "__main__":
#     main()
