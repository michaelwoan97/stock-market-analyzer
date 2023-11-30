from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit
import uuid

def process_stock_data_with_spark(spark, stock_data):

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

    finally:
        # Stop the Spark session
        spark.stop()