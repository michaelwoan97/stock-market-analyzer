from pyspark.sql.functions import col, lit
import uuid
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Array numbers for moving averages, Bollinger Bands, and RSI
ma_periods = [5, 20, 50, 200]
bollinger_periods = [5, 20, 50, 200]
rsi_periods = [14, 20, 50, 200]

# Define a UDF to generate UUIDs
@F.udf(StringType())
def generate_uuid():
    return str(uuid.uuid4())

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
        cleaned_stock_data = cleaned_stock_data.select("stock_id", "ticker_symbol", "transaction_id", "date", "low", "open", "high", "volume", "close").orderBy(F.desc("date"))

        # cleaned_stock_data.orderBy(F.desc("date")).show()

        return cleaned_stock_data

    except Exception as e:
        # Handle any exception that might occur during the process
        print(f"Error in clean_stock_data: {e}")
        return None

def calculate_moving_averages(cleaned_stock_data, periods):
    try:
        round_to_decimal = 2
        # Generate a UUID for each row
        cleaned_stock_data = cleaned_stock_data.withColumn("cal_id", generate_uuid())

        def calculate_ema(data, alpha):
            ema = data[0]
            for i in range(1, len(data)):
                ema = alpha * data[i] + (1 - alpha) * ema
            return ema

        calculate_ema_udf = F.udf(lambda data, alpha: round(float(calculate_ema(data, alpha)), round_to_decimal), FloatType())

        alpha_values = [2 / (p + 1) for p in periods]

        partition_cols = ["stock_id", "ticker_symbol"]

        windows = [Window().partitionBy(partition_cols).orderBy(F.desc("date")).rowsBetween(0, p - 1) for p in periods]

        # Calculate simple moving averages
        for p in periods:
            cleaned_stock_data = cleaned_stock_data.withColumn(f"ma_{p}_days_sma", F.round(F.avg("close").over(windows[periods.index(p)]), 2))

        # Calculate exponential moving averages using UDF
        for p, alpha in zip(periods, alpha_values):
            cleaned_stock_data = cleaned_stock_data.withColumn(f"ma_{p}_days_ema", F.round(calculate_ema_udf(F.collect_list("close").over(windows[periods.index(p)]), F.lit(alpha)), round_to_decimal))

        # Show the result
        moving_averages_data = cleaned_stock_data.select(['cal_id', 'transaction_id', "stock_id", "ticker_symbol", 'date', 'close'] + [f"ma_{p}_days_sma" for p in periods] + [f"ma_{p}_days_ema" for p in periods]).orderBy(F.desc("date"))
        # moving_averages_data.show()

        return moving_averages_data

    except Exception as e:
        # Handle any exception that might occur during the process
        print(f"Error in calculate_moving_averages: {e}")
        return None

def calculate_bollinger_bands(cleaned_stock_data, bollinger_periods):
    try:
        round_to_decimal = 2
        partition_cols = ["stock_id", "ticker_symbol"]
        windows = [Window().partitionBy(partition_cols).orderBy(F.desc("date")).rowsBetween(0, p - 1) for p in bollinger_periods]

        for p in bollinger_periods:
            upper_band_col = col(f"ma_{p}_days_ema") + (2 * F.stddev("close").over(windows[bollinger_periods.index(p)]))
            lower_band_col = col(f"ma_{p}_days_ema") - (2 * F.stddev("close").over(windows[bollinger_periods.index(p)]))

            cleaned_stock_data = cleaned_stock_data.withColumn(f"bb_{p}_upper_band", F.round(upper_band_col, round_to_decimal))
            cleaned_stock_data = cleaned_stock_data.withColumn(f"bb_{p}_lower_band", F.round(lower_band_col, round_to_decimal))

        # cleaned_stock_data.show()
        return cleaned_stock_data

    except Exception as e:
        print(f"Error in calculate_bollinger_bands: {e}")
        return None

def calculate_rsi(data, n, round_to_decimal=2):
    # Calculate price changes
    price_diff = F.col("close") - F.lag("close", 1).over(Window().partitionBy("stock_id", "ticker_symbol").orderBy("date"))
    
    # Separate gains and losses
    gains = F.when(price_diff > 0, price_diff).otherwise(0)
    losses = F.when(price_diff < 0, -price_diff).otherwise(0)
    
    # Calculate average gains and losses over n periods from the latest day backward
    avg_gains = F.avg(gains).over(Window().partitionBy("stock_id", "ticker_symbol").orderBy(F.desc("date")).rowsBetween(0, n-1))
    avg_losses = F.avg(losses).over(Window().partitionBy("stock_id", "ticker_symbol").orderBy(F.desc("date")).rowsBetween(0, n-1))
    
    # Handle NULL values for average gains
    avg_gains = F.coalesce(avg_gains, F.lit(0))

    # Handle 0 values for average losses
    avg_losses = F.when(avg_losses.isNull() | (avg_losses == 0), 0).otherwise(avg_losses)

    # Calculate RSI
    rs = F.when((avg_losses == 0) & (avg_gains != 0), F.lit(avg_gains)) \
      .when((avg_losses != 0) & (avg_gains == 0), F.lit(0)) \
      .otherwise(F.when(avg_losses == 0, F.lit(float('inf'))) \
                  .otherwise(avg_gains / avg_losses))
    
    # Calculate RSI and round to specified decimal places
    rsi = 100 - (100 / (1 + rs))
    
    return F.round(rsi, round_to_decimal)

def calculate_relative_strength_index(cleaned_stock_data, rsi_periods):
    try:
        for n in rsi_periods:
            column_name = f"{n}_days_rsi"
            cleaned_stock_data = cleaned_stock_data.withColumn(column_name, calculate_rsi(cleaned_stock_data, n))

        # cleaned_stock_data.show()
        return cleaned_stock_data

    except Exception as e:
        print(f"Error in calculate_relative_strength_index: {e}")
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


def process_stock_data_with_spark(spark, stock_data, start_date=None, end_date=None):
    try:
        cleaned_data = clean_stock_data(spark, stock_data)
        moving_averages_data = calculate_moving_averages(cleaned_data, ma_periods)
        bollinger_bands_data = calculate_bollinger_bands(moving_averages_data, bollinger_periods)
        rsi_data = calculate_relative_strength_index(bollinger_bands_data, rsi_periods)

        if rsi_data:
            # Check if start_date and end_date are provided
            if start_date is not None and end_date is not None:
                # Filter the data based on the specified date range
                filtered_rsi_data = rsi_data.filter((F.col("date") >= start_date) & (F.col("date") <= end_date))
                return rsi_data, filtered_rsi_data
            else:
                # If start_date or end_date is None, return the original data without filtering
                return rsi_data, rsi_data
        else:
            print("Error: Unable to process data.")
            return None, None

    except Exception as e:
        print(f"Error in process_stock_data_with_spark: {e}")
        return None, None
    