from pyspark.sql.functions import col, lit
import uuid
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from pyspark.sql import functions as F

def clean_stock_data(spark, stock_data):
    try:
        # Convert PriceMovement objects to dictionaries
        price_movement_dicts = [price_movement.to_dict() for price_movement in stock_data.data]

        # Define the schema
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("date", StringType(), True),  
            StructField("low", FloatType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("volume", IntegerType(), True),
            StructField("close", FloatType(), True),
        ])

        # Create a DataFrame from the StockData's data attribute
        stock_data_df = spark.createDataFrame(price_movement_dicts, schema=schema)

        # Convert the 'date' column to DateType
        stock_data_df = stock_data_df.withColumn("date", F.to_date("date"))
       
        # # Fill missing values in the 'volume' column with 0
        stock_data_df = stock_data_df.na.fill(0, subset=['volume'])

        # # Round numeric columns to 2 decimal places
        # # numeric_columns = [col for col, data_type in stock_data_df.dtypes if data_type.startswith('float')]
        # # for col_name in numeric_columns:
        # #     stock_data_df = stock_data_df.withColumn(col_name, F.round(F.col(col_name), 2))

        # # Drop duplicate rows based on 'date' and 'close' columns
        cleaned_stock_data = stock_data_df.dropDuplicates(['date', 'close'])
        

        # Check for duplicate values in the 'date' column again
        duplicate_rows = cleaned_stock_data.groupBy('date', 'close').count().filter('count > 1')

        # Show the duplicate dates and close prices, if any
        if duplicate_rows.count() > 0:
            print("Duplicate dates and close prices found after deduplication:")
            duplicate_rows.show()
        else:
            print("No duplicate dates and close prices found.")

        # Add 'stock_id' and 'ticker_symbol' columns with constant values to all rows
        cleaned_stock_data = cleaned_stock_data.withColumn("stock_id", lit(str(stock_data.stock_id)))
        cleaned_stock_data = cleaned_stock_data.withColumn("ticker_symbol", lit(stock_data.ticker_symbol))

        # Reorder the columns
        cleaned_stock_data = cleaned_stock_data.select("stock_id", "ticker_symbol", "transaction_id", "date", "low", "open", "high", "volume", "close")

        cleaned_stock_data.orderBy(F.desc("date")).show()

        return cleaned_stock_data

    except Exception as e:
        # Handle any exception that might occur during the process
        print(f"Error in clean_stock_data: {e}")
        return None
    
def display_stock_data(spark, stock_data):
    try:
        # Convert PriceMovement objects to dictionaries
        price_movement_dicts = [price_movement.to_dict() for price_movement in stock_data.data]

        # Define the schema
        schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("low", StringType(), True),
            StructField("open", StringType(), True),
            StructField("high", StringType(), True),
            StructField("volume", StringType(), True),
            StructField("close", StringType(), True),
        ])

        # Create a DataFrame from the list of dictionaries with the specified schema
        spark_df = spark.createDataFrame(price_movement_dicts, schema=schema)

        # Add 'stock_id' and 'ticker_symbol' columns with constant values to all rows
        spark_df = spark_df.withColumn("stock_id", lit(str(stock_data.stock_id)))
        spark_df = spark_df.withColumn("ticker_symbol", lit(stock_data.ticker_symbol))

        # Reorder the columns
        spark_df = spark_df.select("stock_id", "ticker_symbol", "transaction_id", "date", "low", "open", "high", "volume", "close")

        # Show the DataFrame
        spark_df.show()

    except Exception as e:
        # Handle any exception that might occur during DataFrame creation or processing
        print(f"Error: {e}")


def process_stock_data_with_spark(spark, stock_data):
    clean_stock_data(spark,stock_data)