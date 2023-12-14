import sys
sys.path.append('..')  

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, DoubleType, ArrayType, LongType
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dotenv import load_dotenv
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from database import create_connection, execute_sql
from stock_market_operator import StockMarketOperator

import uuid

# Load environment variables
load_dotenv()

default_db_properties = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": os.getenv("DB_DRIVER"),
    "url": os.getenv("DB_URL"),
}

public_schema_name = "public"
app_name = "StockDataProcessing"
# Specify the path to the PostgreSQL JDBC driver JAR file
default_postgres_jar_path = "data_processing/drivers/postgresql-42.6.0.jar"

default_spark = SparkSession.builder \
                        .appName(app_name) \
                        .config("spark.executor.memory", "4g") \
                        .config("spark.jars", default_postgres_jar_path ) \
                        .getOrCreate()

# Notes: StockAnalyzerSQL is a class that is used to analyze stock market data, it operate with StockMarketOperator class, and
#        it is used to analyze stock market data and store the result to database. Only one instance of spark session is allowed
class StockAnalyzerSQL:
    def __init__(self, stock_market_operator, spark=None, db_properties=None, postgres_jar_path=None, schema_name=None):
        self.db_properties = db_properties if db_properties else default_db_properties
        self.schema_name = schema_name if schema_name else public_schema_name
        self.postgres_jar_path = postgres_jar_path if postgres_jar_path else default_postgres_jar_path
        self.spark = spark if spark else default_spark
        self.stock_market_operator = stock_market_operator
        self.round_to_decimal = 2
    
    def get_spark(self):
        return self.spark
    
    def get_schema(self):
        # Define your custom schema
        custom_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("stock_id", StringType(), True),
            StructField("ticker_symbol", StringType(), True),
            StructField("date", DateType(), True),
            StructField("low", FloatType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("volume", LongType(), True),
            StructField("close", FloatType(), True)
        ])
        return custom_schema
    
    def get_stock_data(self, table_name):
        full_table_name = f'{self.schema_name}."{table_name}"'
        # Attempt to read data from PostgreSQL
        try:
            schema = self.get_schema()
            stockData = self.spark.read.jdbc(url=self.db_properties["url"],
                                        table=full_table_name,
                                        properties=self.db_properties)

            # Create a new DataFrame with the transformed data and custom schema
            transformedStockData = self.spark.createDataFrame(stockData.rdd, schema=schema)
            
            return transformedStockData

        except Exception as e:
            print("Error reading data from PostgreSQL:")
            print(e)
    
    def stop_spark(self):
        try:
            # Stop the Spark session when you're done
            self.spark.stop()
        except Exception as e:
            print("Error stopping spark:")
            print(e)

    def get_stocks_exist(self, stock_data_df):
        try:
            # Convert the 'date' column to a date type if it's not already
            stockData = stock_data_df.withColumn("date", F.to_date(stock_data_df["date"]))

            # Create a window specification for each group, ordered by the 'date' column in descending order
            windowSpec = Window().partitionBy("ticker_symbol").orderBy(F.desc("date"))

            # Add a row number to the DataFrame based on the window specification
            rankedData = stockData.withColumn("row_number", F.row_number().over(windowSpec))

            # Filter the rows with row number equal to 1 (latest date) for each group
            latestData = rankedData.filter("row_number = 1").drop("row_number")

            # Select only the necessary columns
            result = latestData.select("ticker_symbol", "date", "low", "open", "high", "volume", "close")

            # Show the result
            return True, result
        except Exception as e:
            # Handle the exception and return False
            print(f"Error Getting Stocks Exist: {e}")
            return False, None

    def clean_stock_data(self, stock_data_df):
        try: 
            # Fill missing values in the 'volume' column with 0
            stockData = stock_data_df.na.fill(0, subset=['volume'])

            # Drop duplicate rows based on 'date' and 'close' columns
            cleanedStockData = stockData.dropDuplicates(['date', 'close'])

            # Check for duplicate values in the 'date' column again
            duplicate_rows = cleanedStockData.groupBy('date', 'close').count().filter('count > 1')

            # Show the duplicate dates and close prices, if any
            if duplicate_rows.count() > 0:
                print("Duplicate dates and close prices found after deduplication:")
                duplicate_rows.show()
            else:
                print("No duplicate dates and close prices found.")

            return True, cleanedStockData.orderBy(func.desc("date"))
        except Exception as e:
            # Handle the exception and return False
            print(f"Error Cleaning Stock Data: {e}")
            return False, None
    
    def clean_stock_technical_view(self):
        try:
            drop_views = ['DROP MATERIALIZED VIEW IF EXISTS stock_technical_view;']
            return self.stock_market_operator.execute_sql(drop_views)
        except Exception as e:
            # Handle the exception and return False
            print(f"Error Cleaning Stock Techincal View: {e}")
            return False
    
    def alter_tables_to_og_structures(self):
        alter_statements = [
            # Altered data types for MovingAverages table
            """
            ALTER TABLE "MovingAverages"
            ALTER COLUMN "cal_id" TYPE UUID USING "transaction_id"::UUID,
            ALTER COLUMN "transaction_id" TYPE UUID USING "transaction_id"::UUID,
            ALTER COLUMN "stock_id" TYPE UUID USING "stock_id"::UUID,
            ALTER COLUMN "ticker_symbol" TYPE VARCHAR,
            ALTER COLUMN "date" TYPE DATE,
            ALTER COLUMN "5_days_sma" TYPE FLOAT,
            ALTER COLUMN "20_days_sma" TYPE FLOAT,
            ALTER COLUMN "50_days_sma" TYPE FLOAT,
            ALTER COLUMN "200_days_sma" TYPE FLOAT,
            ALTER COLUMN "5_days_ema" TYPE FLOAT,
            ALTER COLUMN "20_days_ema" TYPE FLOAT,
            ALTER COLUMN "50_days_ema" TYPE FLOAT,
            ALTER COLUMN "200_days_ema" TYPE FLOAT;
            """,
            # Altered data types for BoillingerBands table
            """
            ALTER TABLE "BoillingerBands"
            ALTER COLUMN "cal_id" TYPE UUID USING "transaction_id"::UUID,
            ALTER COLUMN "transaction_id" TYPE UUID USING "transaction_id"::UUID,
            ALTER COLUMN "stock_id" TYPE UUID USING "stock_id"::UUID,
            ALTER COLUMN "ticker_symbol" TYPE VARCHAR,
            ALTER COLUMN "date" TYPE DATE,
            ALTER COLUMN "5_upper_band" TYPE FLOAT,
            ALTER COLUMN "20_upper_band" TYPE FLOAT,
            ALTER COLUMN "50_upper_band" TYPE FLOAT,
            ALTER COLUMN "200_upper_band" TYPE FLOAT,
            ALTER COLUMN "5_lower_band" TYPE FLOAT,
            ALTER COLUMN "20_lower_band" TYPE FLOAT,
            ALTER COLUMN "50_lower_band" TYPE FLOAT,
            ALTER COLUMN "200_lower_band" TYPE FLOAT;
            """,

            # Altered data types for RelativeIndexes table
            """
            ALTER TABLE "RelativeIndexes"
            ALTER COLUMN "cal_id" TYPE UUID USING "transaction_id"::UUID,
            ALTER COLUMN "transaction_id" TYPE UUID USING "transaction_id"::UUID,
            ALTER COLUMN "stock_id" TYPE UUID USING "stock_id"::UUID,
            ALTER COLUMN "ticker_symbol" TYPE VARCHAR,
            ALTER COLUMN "date" TYPE DATE,
            ALTER COLUMN "14_days_rsi" TYPE FLOAT,
            ALTER COLUMN "20_days_rsi" TYPE FLOAT,
            ALTER COLUMN "50_days_rsi" TYPE FLOAT,
            ALTER COLUMN "200_days_rsi" TYPE FLOAT;
            """
        ]

        alter_key_constrains_statements = [
            # MovingAverages
            'ALTER TABLE "MovingAverages" ADD CONSTRAINT "pk_MovingAverages_cal_id" PRIMARY KEY ("cal_id");',
            'ALTER TABLE "MovingAverages" ADD CONSTRAINT "fk_MovingAverages_transaction_id" FOREIGN KEY ("transaction_id") REFERENCES "Stocks"("transaction_id") ON DELETE CASCADE;',
            'ALTER TABLE "MovingAverages" ADD CONSTRAINT "fk_MovingAverages_stock_id_ticker_symbol" FOREIGN KEY ("stock_id", "ticker_symbol") REFERENCES "CompanyInformation"("stock_id", "ticker_symbol");',

            # BoillingerBands
            'ALTER TABLE "BoillingerBands" ADD CONSTRAINT "pk_BoillingerBands_cal_id" PRIMARY KEY ("cal_id");',
            'ALTER TABLE "BoillingerBands" ADD CONSTRAINT "fk_BoillingerBands_transaction_id" FOREIGN KEY ("transaction_id") REFERENCES "Stocks"("transaction_id") ON DELETE CASCADE;',
            'ALTER TABLE "BoillingerBands" ADD CONSTRAINT "fk_BoillingerBands_stock_id_ticker_symbol" FOREIGN KEY ("stock_id", "ticker_symbol") REFERENCES "CompanyInformation"("stock_id", "ticker_symbol");',

            # RelativeIndexes
            'ALTER TABLE "RelativeIndexes" ADD CONSTRAINT "pk_RelativeIndexes_cal_id" PRIMARY KEY ("cal_id");',
            'ALTER TABLE "RelativeIndexes" ADD CONSTRAINT "fk_RelativeIndexes_transaction_id" FOREIGN KEY ("transaction_id") REFERENCES "Stocks"("transaction_id") ON DELETE CASCADE;',
            'ALTER TABLE "RelativeIndexes" ADD CONSTRAINT "fk_RelativeIndexes_stock_id_ticker_symbol" FOREIGN KEY ("stock_id", "ticker_symbol") REFERENCES "CompanyInformation"("stock_id", "ticker_symbol");'
        ]

        # List of SQL statements for indexes
        index_sql_statements = [
            'CREATE INDEX IF NOT EXISTS idx_ma_stock_id_ticker_symbol_date ON "MovingAverages"("stock_id", "ticker_symbol", "date");',
            'CREATE INDEX IF NOT EXISTS idx_ma_date ON "MovingAverages"("date");',
            'CREATE INDEX IF NOT EXISTS idx_bb_stock_id_ticker_symbol_date ON "BoillingerBands"("stock_id", "ticker_symbol", "date");',
            'CREATE INDEX IF NOT EXISTS idx_bb_date ON "BoillingerBands"("date");',
            'CREATE INDEX IF NOT EXISTS idx_ri_stock_id_ticker_symbol_date ON "RelativeIndexes"("stock_id", "ticker_symbol", "date");',
            'CREATE INDEX IF NOT EXISTS idx_ri_date ON "RelativeIndexes"("date");',
        ]

        sql_statements = alter_statements + alter_key_constrains_statements + index_sql_statements
        # Execute SQL statements
        return self.stock_market_operator.execute_sql(sql_statements)

    def print_statements_by_status(self, process_name, status):
        if status:
            print(f'>>> {process_name} process is complete.')
        else:
            print(f'Error: {process_name} process is not complete.')
            
    def insert_data_to_db_table(self, data, table_name, mode):
        try:
            # Write the DataFrame to the database table
            data.write.jdbc(self.db_properties['url'], table_name, mode=mode, properties=self.db_properties)
            return True
        except Exception as e:
            # Handle the exception and return False
            print(f"Error inserting data to table: {e}")
            return False
        

    def calculate_moving_averages(self, cleaned_stock_data_df, periods, update_mode=None):
        try:
            if not update_mode:
                # Define a UDF to generate UUIDs
                @F.udf(StringType())
                def generate_uuid():
                    return str(uuid.uuid4())

                round_to_decimal = self.round_to_decimal

                # Generate a UUID for each row
                # cleanedStockData = cleanedStockData.withColumn("cal_id", F.lit(str(uuid.uuid4())))
                cleanedStockData = cleaned_stock_data_df.withColumn("cal_id", generate_uuid())

                def calculate_ema(data, alpha):
                    ema = data[0]
                    for i in range(1, len(data)):
                        ema = alpha * data[i] + (1 - alpha) * ema
                    return ema

                calculate_ema_udf = F.udf(lambda data, alpha: float(calculate_ema(data, alpha)), FloatType())

                alpha_values = [2 / (p + 1) for p in periods]

                partition_cols = ["stock_id", "ticker_symbol"]

                windows = [Window().partitionBy(partition_cols).orderBy(F.desc("date")).rowsBetween(0, p - 1) for p in periods]

                # Calculate simple moving averages
                for p in periods:
                    cleanedStockData = cleanedStockData.withColumn(f"{p}_days_sma", F.round(F.avg("close").over(windows[periods.index(p)]), 2))

                # Calculate exponential moving averages using UDF
                for p, alpha in zip(periods, alpha_values):
                    cleanedStockData = cleanedStockData.withColumn(f"{p}_days_ema", F.round(calculate_ema_udf(F.collect_list("close").over(windows[periods.index(p)]), F.lit(alpha)), round_to_decimal))

                # Show the result
                moving_averages_data = cleanedStockData.select(['cal_id','transaction_id',"stock_id", "ticker_symbol",'date'] + [f"{p}_days_sma" for p in periods] + [f"{p}_days_ema" for p in periods]).orderBy(F.desc("date"))
                # moving_averages_data.show()

                # Schema and table name
                table_name = '"MovingAverages"'

                # Write the DataFrame to the database table
                self.insert_data_to_db_table(moving_averages_data,table_name,"overwrite")

                return True, cleanedStockData.orderBy(F.desc("date"))
        except Exception as e:
            # Handle the exception and return False
            print(f"Error calculating moving averages: {e}")
            return False, None


    def calculate_boillinger_bands(self, stock_data_df, bollinger_periods, update_mode=None):
        try:
            if not update_mode:

                partition_cols = ["stock_id", "ticker_symbol"]

                # Check if the necessary EMA columns exist
                ema_columns_exist = all(f"{p}_days_ema" in stock_data_df.columns for p in bollinger_periods)

                if not ema_columns_exist:
                    print("Error: Exponential Moving Average columns are missing.")
                    return False, None
                
                # Define the windows for Bollinger Bands
                windows = [Window().partitionBy(partition_cols).orderBy(F.desc("date")).rowsBetween(0, p - 1) for p in bollinger_periods]

                # Reuse the existing EMA values for Bollinger Bands
                for p in bollinger_periods:
                    upper_band_col = F.col(f"{p}_days_ema") + (2 * F.stddev("close").over(windows[bollinger_periods.index(p)]))
                    lower_band_col = F.col(f"{p}_days_ema") - (2 * F.stddev("close").over(windows[bollinger_periods.index(p)]))

                    stock_data_df = stock_data_df.withColumn(f"{p}_upper_band", F.round(upper_band_col, self.round_to_decimal))
                    stock_data_df = stock_data_df.withColumn(f"{p}_lower_band", F.round(lower_band_col, self.round_to_decimal))

                # Show the result
                selected_columns = ['cal_id','transaction_id',"stock_id", "ticker_symbol",'date'] + [f"{p}_upper_band" for p in bollinger_periods] + [f"{p}_lower_band" for p in bollinger_periods]
                boilling_bands_data = stock_data_df.select(selected_columns).orderBy(F.desc("date"))
                # boilling_bands_data.show()

                # Schema and table name
                table_name = '"BoillingerBands"'

                # Write the DataFrame to the database table
                self.insert_data_to_db_table(boilling_bands_data,table_name,"overwrite")
                return True, stock_data_df.orderBy(F.desc("date"))
        except Exception as e:
            # Handle the exception and return False
            print(f"Error calculating boillinger bands: {e}")
            return False, None


    def calculate_rsi_seperate_period(self, stock_data_df, period):
        try:
            n = period
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
            
            # Calculate RSI and round to 2 decimal places
            rsi = 100 - (100 / (1 + rs))
            
            return F.round(rsi, self.round_to_decimal)
        except Exception as e:
            # Handle the exception and return False
            print(f"Error calculating relative index for seperate period {period}: {e}")
            return None


    def calculate_relative_indexes(self, stock_data_df, rsi_periods, update_mode=None):
        try:
            if not update_mode:
            
                # List of RSI periods
                rsi_periods = [14, 20, 50, 200]

                # Calculate and add RSI columns to the DataFrame for each period
                for n in rsi_periods:
                    column_name = f"{n}_days_rsi"
                    stock_data_df = stock_data_df.withColumn(column_name, self.calculate_rsi_seperate_period(stock_data_df, n))

                # Show the result
                result_columns = ['cal_id','transaction_id',"stock_id", "ticker_symbol",'date'] + [f"{n}_days_rsi" for n in rsi_periods]
                relative_indexes_data = stock_data_df.select(result_columns).orderBy(F.desc("date"))
                # relative_indexes_data.show()

                # Schema and table name
                table_name = '"RelativeIndexes"'

                # Write the DataFrame to the database table
                self.insert_data_to_db_table(relative_indexes_data,table_name,"overwrite")
                return True, stock_data_df.orderBy(F.desc("date"))

        except Exception as e:
            # Handle the exception and return False
            print(f"Error calculating moving averages: {e}")
            return False, None


    def analyze_stock_market_data(self, update_mode=None):
        if not update_mode:
            stock_data_df = self.get_stock_data("Stocks")

            if stock_data_df is not None:
                print(f'Stocks with Latest Data')
                status, stocks_exist = self.get_stocks_exist(stock_data_df)
                if status and stocks_exist:
                    stocks_exist.show()
                print(f'=======================')
                

                print(f'Handling Missing Values and Deduplication....')
                (status, cleaned_stock_df) = self.clean_stock_data(stock_data_df)
                self.print_statements_by_status("Cleaning process is done",status)
                print(f'=============================================')
                
                print(f'Clean or Drop Stock Technical View to Update...')
                status = self.clean_stock_technical_view()
                self.print_statements_by_status("Clean Stock Technical View", status)
                print(f'===============================================')
                

                print(f'Calculating Moving Averages....')
                periods = [5, 20, 50, 200]
                status, stock_techincal_data = self.calculate_moving_averages(cleaned_stock_df,periods)
                self.print_statements_by_status("Calculating Moving Averages", status)
                print(f'===============================')
                

                print(f'Calculating BoilingerBands....')
                periods = [5, 20, 50, 200]
                (status, stock_techincal_data) = self.calculate_boillinger_bands(stock_techincal_data,periods)
                self.print_statements_by_status("Calculating Boilinger Bands", status)
                print(f'==============================')
                

                print(f'Calculating Relative Indexes....')
                rsi_periods = [14, 20, 50, 200]
                (status, stock_techincal_data) = self.calculate_relative_indexes(stock_techincal_data,rsi_periods)
                self.print_statements_by_status("Calculating Relative Indexes", status)
                print(f'================================')
                

                print(f'Update Tables to Original Structures After Overwriten process....')
                self.print_statements_by_status("Update Table to OG Structures", self.alter_tables_to_og_structures())
                print(f'=================================================================')
                


def analyze_stock_market():
    stock_market_operator = StockMarketOperator(default_spark)
    stock_market_operator.create_connection_pool()
    stock_analyzer = StockAnalyzerSQL(stock_market_operator)
    stock_analyzer.analyze_stock_market_data()
    stock_market_operator.create_or_refresh_materialized_view_with_partition()
    

# analyze_stock_market()



