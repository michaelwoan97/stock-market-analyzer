import asyncio
import asyncpg
from datetime import datetime, timedelta
import json
import os
import uuid
import copy
from altair import Data
from matplotlib import ticker
import pandas as pd
import psycopg2
import yfinance

from psycopg2 import sql, pool 
from finance import fetch_stock_data_from_url, PriceMovement
from dotenv import load_dotenv

from spark_processor import process_stock_data_with_spark
from asyncpg.pool import create_pool 
from database import StockData

# Load the environment variables from the .env file
load_dotenv()



# ---- Non-Async Operations ----
# ============================================================================
# ============================================================================

class StockMarketOperator:
    def __init__(self, minconn=5, maxconn=10):
        self.db_params = {
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD'),
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'database': os.environ.get('DB_NAME')
        }
        self.pool = None
        self.minconn = minconn
        self.maxconn = maxconn
    
    # Function to create a connection pool
    def create_connection_pool(self):
        try:
            self.pool = pool.SimpleConnectionPool(
                minconn=self.minconn,
                maxconn=self.maxconn,
                **self.db_params
            )
        except psycopg2.OperationalError as e:
            print(f"Error creating the connection pool: {e}")
            raise e
        
    # Function to get a connection from the pool
    def get_connection_from_pool(self):

        try:
            if not self.pool:
                raise RuntimeError("Connection pool is not available.")
            
            connection = self.pool.getconn()
            return connection
        except psycopg2.OperationalError as e:
            print(f"Error getting a connection from the pool: {e}")
            raise e
    
    def close_connection_pool(self):
        """
        Close the connection pool.
        """
        if self.pool:
            self.pool.closeall()
    
    def close_connection(self, connection):
        """
        Close the connection.
        """
        if self.pool and connection:
            self.pool.putconn(connection)

    # get stocks exist in Stocks table
    def get_stocks_ticker_id_exist(self):
        connection = None

        try:
            
            # Acquire a connection from the pool
            connection = self.get_connection_from_pool()

            query = "SELECT DISTINCT stock_id, ticker_symbol FROM \"Stocks\""
            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()

            # Fetch the results using fetchall
            stocks_info = [
                {'stock_id': stock_id, 'ticker_symbol': ticker_symbol}
                for stock_id, ticker_symbol in results
            ]

            return stocks_info

        except Exception as e:
            print(f"Error getting stock ticker & its id from the database: {e}")

        finally:
            # Release the connection back to the pool
            self.close_connection(connection)

    # check if stock missing data 
    def check_missing_dates_of_stock_data(self, stock_id, ticker_symbol):
        connection = None
        try:
            
            connection = self.get_connection_from_pool()
            
            # Assuming "date" is the column name and "Stocks" is the table name
            query = f'SELECT MAX("date") FROM "Stocks" WHERE stock_id = \'{stock_id}\' AND ticker_symbol = \'{ticker_symbol}\';'

            with connection.cursor() as cursor:
                cursor.execute(query)
                latest_date = cursor.fetchone()[0]

            # If latest_date is None, there are no records for the given stock
            if latest_date is not None:
                # Convert the latest_date to a Python datetime object
                latest_date = datetime.combine(latest_date, datetime.min.time())

                # Get the current date
                current_date = datetime.now()

                # Calculate the difference in days
                difference_in_days = (current_date - latest_date).days

                print(f"Latest date for stock {ticker_symbol}: {latest_date}, Current date: {datetime.now()}, Difference in days: {(datetime.now() - datetime.combine(latest_date, datetime.min.time())).days}") if latest_date is not None else print(f"No records found for stock {ticker_symbol}")

                return difference_in_days
            else:
                # Handle the case when there are no records for the given stock
                print(f"No records found for stock {ticker_symbol}")
                return None

        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error getting latest date for stock {ticker_symbol}: {error}")
            return None
        finally:
            # Release the connection back to the pool
            self.close_connection(connection)

    # insert stock data to db 
    def insert_stock_data_into_db(self, stock_data):
        connection = None
        
        try:
            connection = self.get_connection_from_pool()
            total_rowcount = 0  # Initialize a variable to track the total rowcount

            with connection.cursor() as cursor:
                for data_point in stock_data.data:
                    query = """
                        INSERT INTO "Stocks" ("transaction_id", "stock_id", "ticker_symbol", "date", "low", "open", "high", "volume", "close")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(query, (str(data_point.transaction_id), stock_data.stock_id, stock_data.ticker_symbol, data_point.date, data_point.low, data_point.open_price, data_point.high, data_point.volume, data_point.close))
                    
                    # Add the rowcount of the current execute to the total rowcount
                    total_rowcount += cursor.rowcount
            
            # Check if any rows were affected
            if total_rowcount > 0:
                print(f"Total rows inserted for ticker symbol {stock_data.ticker_symbol}: {total_rowcount}")
                connection.commit()
            else:
                print(f"No rows were affected or ticker symbol {stock_data.ticker_symbol}. Possible duplicate or failed insert.")
                connection.rollback()

            return total_rowcount  # Return the total rowcount

        except Exception as e:
            if connection:
                connection.rollback()
            print(f"Error inserting stock data into the database: {e}")
            return 0  # Return 0 in case of an error
        finally:
            self.close_connection(connection)

    def update_missing_stock_data(self, stockTickerId):
        stock_id = stockTickerId['stock_id']
        ticker_symbol = stockTickerId['ticker_symbol']

        conn = None

        try:
            # get connection
            conn = self.get_connection_from_pool()

            # Calculate the missing dates using the check_missing_dates_of_stock_data function
            missing_dates = self.check_missing_dates_of_stock_data(stock_id, ticker_symbol)

            if missing_dates:
                # Create a Ticker object for the stock
                stock = yfinance.Ticker(ticker_symbol)

                arr_stock_data = []

                # Get historical data for the stock for the current day
                hist_data = stock.history(period=f"{missing_dates}d")

                # Check if data is available before formatting
                if not hist_data.empty:
                    for index, row in hist_data.iterrows():
                        # Generate a UUID for the 'transaction_id' column
                        transaction_id = uuid.uuid4()
                        price_movement = PriceMovement(
                            transaction_id=transaction_id,
                            date=pd.to_datetime(index, format='%Y-%m-%d').strftime('%Y-%m-%d'),
                            low=float(row['Low']),
                            open_price=float(row['Open']),
                            volume=int(row['Volume']),
                            high=float(row['High']),
                            close=float(row['Close'])
                        )

                        arr_stock_data.append(price_movement)

                    update_stock_info = StockData(stock_id, ticker_symbol, None, arr_stock_data)
                    self.insert_stock_data_into_db(update_stock_info)

                    return update_stock_info
                else:
                    print(f"Failed to fetch data for symbol {ticker_symbol}.")

                    # Return None or handle the case as needed
                    return None
            else:
                print("You are up to date!! Yayyy")
                return None

        except Exception as e:
            # Handle any exceptions that might occur
            print(f"An error occurred: {str(e)}")
            # Optionally, you can log the exception or perform additional actions
            return None

        finally:
            # Ensure that the database connection is closed in the finally block
            self.close_connection(conn)

# ---- Async Operations ----
# ============================================================================
# ============================================================================
class AsyncStockMarketOperator:
    def __init__(self, spark, minconn=5, maxconn=10, max_queries=500):
        # Connection parameters for the PostgreSQL server
        self.async_db_params = {
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD'),
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'database': os.environ.get('DB_NAME'),
            'min_size': minconn,  # Minimum number of connections in the pool (adjust as needed)
            'max_size': maxconn,  # Maximum number of connections in the pool (adjust as needed)
            'max_queries': max_queries,  # Maximum number of queries a connection can execute before being released (adjust as needed)
        }
        self.spark = spark
        self.pool = None
        self.stock_technical_view = 'stock_technical_view'

    async def create_connection_pool(self):
        """
        Create an asyncpg connection pool.
        """
        try:
            self.pool = await asyncpg.create_pool(**self.async_db_params)
        except asyncpg.PostgresError as e:
            print(f"Error creating connection pool: {e}")
            # Handle the error or re-raise it
            raise e

    async def create_connection(self):
        """
        Create a database connection using the existing pool.
        """
        if not self.pool:
            raise RuntimeError("Connection pool is not available.")

        connection = await self.pool.acquire()
        return self.pool, connection

    async def close_connection_pool(self):
        """
        Close the asyncpg connection pool.
        """
        if self.pool:
            await self.pool.close()

    async def execute_query(self, query, *args):
        """
        Execute a query using an asyncpg connection from the pool.
        """
        async with self.pool.acquire() as connection:
            result = await connection.fetch(query, *args)
            return result
    
    async def check_company_exists(self, ticker_symbol, country):
        """
        Check if a company exists in the database based on the ticker symbol and country.
        """
        try:
            async with self.pool.acquire() as connection:
                query = "SELECT \"stock_id\" FROM \"CompanyInformation\" WHERE \"ticker_symbol\" = $1 AND \"country\" = $2;"
                result = await connection.fetch(query, ticker_symbol, country)

            stock_ids = [row['stock_id'] for row in result]
            return stock_ids
        except asyncpg.PostgresError as e:
            print(f"Error checking company existence: {e}")
            # Handle the error or re-raise it
            raise e

    async def stock_data_exists(self, stock_id, ticker_symbol, start_date=None, end_date=None):

        """
        Check if stock data exists in the database based on stock ID, ticker symbol, and optional date range.
        """
        try:
            async with self.pool.acquire() as connection:
                query = """
                    SELECT
                        "transaction_id",
                        "stock_id",
                        "ticker_symbol",
                        "date",
                        "close",
                        "volume"
                    FROM "Stocks"
                    WHERE "stock_id" = $1 AND "ticker_symbol" = $2
                """

                # Convert optional date range conditions to date objects
                if start_date is not None:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                    query += ' AND "date" >= $3 '
                if end_date is not None:
                    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
                    query += ' AND "date" <= $4 '

                # Execute the query with parameters
                if start_date is not None and end_date is not None:
                    result = await connection.fetch(query, stock_id, ticker_symbol, start_date, end_date)
                else:
                    result = await connection.fetch(query, stock_id, ticker_symbol)

                # Convert the result set to a list of dictionaries
                result_list = [dict(row) for row in result]

            return result_list
        except asyncpg.PostgresError as e:
            print(f"Error checking stock data existence: {e}")
            # Handle the error or re-raise it
            raise e
    
    # view_date_range table
    async def create_view_date_range_table(self):
        """
        Create the views_date_ranges table if it doesn't exist.
        """
        try:
            async with self.pool.acquire() as connection:
                # Check if the table exists
                table_exists = await connection.fetchval("SELECT to_regclass('public.views_date_ranges')")

                if not table_exists:
                    # If it doesn't exist, create the table
                    create_table_sql = """
                        CREATE TABLE public.views_date_ranges (
                            view_name VARCHAR(255) PRIMARY KEY,
                            start_date DATE,
                            end_date DATE
                        );
                    """
                    await connection.execute(create_table_sql)

        except Exception as e:
            print(f"Error creating views_date_ranges table: {e}")

    async def get_date_range_for_view(self):
        """
        Get the date range for the stock_technical_view from view_date_ranges table.

        Returns:
        - Tuple[str, str]: Start date and end date of the view, or (None, None) if the view is not found.
        """
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    async with connection.cursor() as cursor:
                        await cursor.execute(
                            "SELECT start_date, end_date FROM public.views_date_ranges WHERE view_name = $1;",
                            (self.stock_technical_view,)
                        )
                        result = await cursor.fetchone()
                        return result  # This will be (start_date, end_date) or None if the view is not found

        except asyncpg.exceptions.PostgresError as e:
            print(f"Error fetching date range for view {self.stock_technical_view}: {e}")
            return None, None

    async def update_view_date_range(self, start_date, end_date):
        """
        Update or insert the date range for the stock_technical_view.

        Parameters:
        - start_date (str): Start date of the view.
        - end_date (str): End date of the view.
        """
        try:
            async with self.pool.acquire() as connection:
                # Update or insert the date range for the given view
                upsert_sql = """
                    INSERT INTO public.views_date_ranges (view_name, start_date, end_date)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (view_name) DO UPDATE
                    SET start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date;
                """
                await connection.execute(upsert_sql, self.stock_technical_view, start_date, end_date)

        except Exception as e:
            print(f"Error updating views_date_ranges table: {e}")

    async def check_date_range_overlap(self, existing_start, existing_end, requested_start, requested_end):
        """
        Check if there is an overlap between two date ranges.

        Parameters:
        - existing_start (str): Start date of the existing range.
        - existing_end (str): End date of the existing range.
        - requested_start (str): Start date of the requested range.
        - requested_end (str): End date of the requested range.

        Returns:
        - bool: True if there is an overlap, False otherwise.
        """
        try:
            existing_start = datetime.strptime(existing_start, "%Y-%m-%d")
            existing_end = datetime.strptime(existing_end, "%Y-%m-%d")
            requested_start = datetime.strptime(requested_start, "%Y-%m-%d")
            requested_end = datetime.strptime(requested_end, "%Y-%m-%d")
        except ValueError as e:
            # Handle the case where date conversion fails
            print(f"Error converting date: {e}")
            return False  # Indicate failure due to date conversion error
        
        # Check if there is an overlap between two date ranges
        return existing_start <= requested_end and existing_end >= requested_start
    
    # join table for technical data
    async def get_stock_technical_data_from_tables(self, stock_id, start_date=None, end_date=None):
        try:
            formatted_start_date = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
            formatted_end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

            data = []

            query = """
                SELECT
                    S."stock_id",
                    S."ticker_symbol",
                    S."date",
                    S."close",
                    MA."5_days_sma" AS "ma_5_days_sma",
                    MA."20_days_sma" AS "ma_20_days_sma",
                    MA."50_days_sma" AS "ma_50_days_sma",
                    MA."200_days_sma" AS "ma_200_days_sma",
                    MA."5_days_ema" AS "ma_5_days_ema",
                    MA."20_days_ema" AS "ma_20_days_ema",
                    MA."50_days_ema" AS "ma_50_days_ema",
                    MA."200_days_ema" AS "ma_200_days_ema",
                    BB."5_upper_band" AS "bb_5_upper_band",
                    BB."5_lower_band" AS "bb_5_lower_band",
                    BB."20_upper_band" AS "bb_20_upper_band",
                    BB."20_lower_band" AS "bb_20_lower_band",
                    BB."50_upper_band" AS "bb_50_upper_band",
                    BB."50_lower_band" AS "bb_50_lower_band",
                    BB."200_upper_band" AS "bb_200_upper_band",
                    BB."200_lower_band" AS "bb_200_lower_band",
                    RI."14_days_rsi",
                    RI."20_days_rsi",
                    RI."50_days_rsi",
                    RI."200_days_rsi"
                FROM
                    "Stocks" S
                INNER JOIN
                    "MovingAverages" MA ON S."stock_id" = MA."stock_id" AND S."date" = MA."date"
                INNER JOIN
                    "BoillingerBands" BB ON S."stock_id" = BB."stock_id" AND S."date" = BB."date"
                INNER JOIN
                    "RelativeIndexes" RI ON S."stock_id" = RI."stock_id" AND S."date" = RI."date"
            """

            if start_date is not None:
                query += 'WHERE S."date" >= $1 '
            if end_date is not None:
                query += 'AND S."date" <= $2 '

            query += 'AND S."stock_id" = $3 '

            query += 'ORDER BY S."date" ASC;'

            if start_date is not None and end_date is not None:
                result = await self.pool.fetch(query, formatted_start_date, formatted_end_date, stock_id)
            else:
                result = await self.pool.fetch(query, stock_id)

            data = [dict(row) for row in result]

            return data

        except Exception as e:
            print(f"Error fetching technical data: {e}")
            return None

    # views
    async def check_stock_exists_in_view(self, stock_id, ticker_symbol):
        try:
            query = """
                SELECT 1 
                FROM stock_technical_view 
                WHERE stock_id = $1 AND ticker_symbol = $2
                LIMIT 1;
            """
            result = await self.pool.fetch(query, stock_id, ticker_symbol)

            return len(result) > 0

        except Exception as e:
            print(f"Error checking if stock exists in the view: {e}")
            return False
        
    async def create_or_refresh_materialized_view_with_partition(self):
        try:
            # Calculate start and end dates dynamically (e.g., 10 years from now)
            current_date = datetime.now()
            start_date = current_date - timedelta(days=365 * 10)
            end_date = current_date
    
            async with self.pool.transaction():
                await self.create_view_date_range_table()
    
                view_exists = await self.pool.fetchval(
                    "SELECT 1 FROM pg_matviews WHERE matviewname = 'stock_technical_view'"
                )
    
                if not view_exists:
                    create_view_sql = """
                        CREATE MATERIALIZED VIEW stock_technical_view AS
                        SELECT
                            S.stock_id,
                            S.ticker_symbol,
                            S.date,
                            S.close,
                            MA."5_days_sma" AS ma_5_days_sma,
                            MA."20_days_sma" AS ma_20_days_sma,
                            MA."50_days_sma" AS ma_50_days_sma,
                            MA."200_days_sma" AS ma_200_days_sma,
                            MA."5_days_ema" AS ma_5_days_ema,
                            MA."20_days_ema" AS ma_20_days_ema,
                            MA."50_days_ema" AS ma_50_days_ema,  
                            MA."200_days_ema" AS ma_200_days_ema,
                            BB."5_upper_band" AS bb_5_upper_band,
                            BB."5_lower_band" AS bb_5_lower_band,
                            BB."20_upper_band" AS bb_20_upper_band,
                            BB."20_lower_band" AS bb_20_lower_band,
                            BB."50_upper_band" AS bb_50_upper_band,
                            BB."50_lower_band" AS bb_50_lower_band,
                            BB."200_upper_band" AS bb_200_upper_band,
                            BB."200_lower_band" AS bb_200_lower_band,
                            RI."14_days_rsi",
                            RI."20_days_rsi",
                            RI."50_days_rsi",
                            RI."200_days_rsi"
                        FROM
                            "Stocks" S
                        INNER JOIN
                            "MovingAverages" MA ON S.stock_id = MA.stock_id::uuid AND S.date = MA.date
                        INNER JOIN
                            "BoillingerBands" BB ON S.stock_id = BB.stock_id::uuid AND S.date = BB.date
                        INNER JOIN
                            "RelativeIndexes" RI ON S.stock_id = RI.stock_id::uuid AND S.date = RI.date
                        WHERE
                            S.date >= $1 AND S.date <= $2
                        ORDER BY
                            S.date DESC;
                    """
    
                    await self.pool.execute(create_view_sql, start_date, end_date)
    
                    create_indexes_sql = """
                        CREATE INDEX idx_materialized_view_combined ON stock_technical_view(stock_id, ticker_symbol, date);
                        CREATE INDEX idx_materialized_view_date ON stock_technical_view(date);
                    """
                    await self.pool.execute(create_indexes_sql)
    
                else:
                    refresh_view_sql = "REFRESH MATERIALIZED VIEW stock_technical_view;"
                    await self.pool.execute(refresh_view_sql)
    
                await self.update_view_date_range('stock_technical_view', start_date, end_date)
    
        except Exception as e:
            print(f"Error: {e}")
    
    async def get_stock_technical_data_from_view(self, stock_id, start_date, end_date):
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

            select_data_sql = """
                SELECT
                    "date",
                    "close",
                    "ma_5_days_sma",
                    "ma_20_days_sma",
                    "ma_50_days_sma",
                    "ma_200_days_sma",
                    "ma_5_days_ema",
                    "ma_20_days_ema",
                    "ma_50_days_ema",
                    "ma_200_days_ema",
                    "bb_5_upper_band",
                    "bb_5_lower_band",
                    "bb_20_upper_band",
                    "bb_20_lower_band",
                    "bb_50_upper_band",
                    "bb_50_lower_band",
                    "bb_200_upper_band",
                    "bb_200_lower_band",
                    "14_days_rsi",
                    "20_days_rsi",
                    "50_days_rsi",
                    "200_days_rsi"
                FROM
                    stock_technical_view
                WHERE
                    "date" >= $1 AND "date" <= $2 and "stock_id" = $3
                ORDER BY
                    "date" DESC;
            """
            result = await self.pool.fetch(select_data_sql, start_date, end_date, stock_id)

            result_list = [dict(row) for row in result]

            return result_list

        except asyncpg.PostgresError as e:
            print(f"Error: {e}")
            raise e


    # insert data to "Stocks" table
    async def insert_stock_data_table(self, stock_data):
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    total_rowcount = 0

                    for data_point in stock_data.data:
                        data_point_date = datetime.strptime(data_point.date, '%Y-%m-%d')
                        query = """
                            INSERT INTO "Stocks" ("transaction_id", "stock_id", "ticker_symbol", "date", "low", "open", "high", "volume", "close")
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
                        """

                        await connection.execute(
                            query,
                            *(
                                data_point.transaction_id,
                                stock_data.stock_id,
                                stock_data.ticker_symbol,
                                data_point_date,
                                data_point.low,
                                data_point.open_price,
                                data_point.high,
                                data_point.volume,
                                data_point.close,
                            ),
                        )
                        
                        total_rowcount += 1

                    if total_rowcount > 0:
                        print(f"Total rows inserted into Stocks table: {total_rowcount}")
                    else:
                        print("No rows were affected. Possible duplicate or failed insert into Stocks table.")

        except Exception as e:
            print(f"Error inserting stock data into the database: {e}")

    # ---- Insert Operations into technical data tables ----
    async def insert_moving_averages(self, moving_averages_data):
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    total_rowcount = 0
                    for data_point in moving_averages_data:
                        query = """
                            INSERT INTO "MovingAverages" ("cal_id", "transaction_id", "stock_id", "ticker_symbol", "date",
                                                        "5_days_sma", "20_days_sma", "50_days_sma", "200_days_sma",
                                                        "5_days_ema", "20_days_ema", "50_days_ema", "200_days_ema")
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
                        """
                        await connection.execute(
                            query,
                            *(
                                data_point['cal_id'],
                                data_point['transaction_id'],
                                data_point['stock_id'],
                                data_point['ticker_symbol'],
                                data_point['date'],
                                data_point['ma_5_days_sma'],
                                data_point['ma_20_days_sma'],
                                data_point['ma_50_days_sma'],
                                data_point['ma_200_days_sma'],
                                data_point['ma_5_days_ema'],
                                data_point['ma_20_days_ema'],
                                data_point['ma_50_days_ema'],
                                data_point['ma_200_days_ema'],
                            ),
                        )

                        total_rowcount += 1

                    if total_rowcount > 0:
                        print(f"Total rows inserted into MovingAverages table: {total_rowcount}")
                    else:
                        print("No rows were affected. Possible duplicate or failed insert into MovingAverages table.")

        except Exception as e:
            print(f"Error inserting moving averages data into the database: {e}")

    async def insert_boillinger_bands(self, boillinger_bands_data):
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    total_rowcount = 0
                    for data_point in boillinger_bands_data:
                        query = """
                            INSERT INTO "BoillingerBands" ("cal_id", "transaction_id", "stock_id", "ticker_symbol", "date",
                                                        "5_upper_band", "20_upper_band", "50_upper_band", "200_upper_band",
                                                        "5_lower_band", "20_lower_band", "50_lower_band", "200_lower_band")
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
                        """
                        await connection.execute(
                            query,
                            *(
                                data_point['cal_id'],
                                data_point['transaction_id'],
                                data_point['stock_id'],
                                data_point['ticker_symbol'],
                                data_point['date'],
                                data_point['bb_5_upper_band'],
                                data_point['bb_20_upper_band'],
                                data_point['bb_50_upper_band'],
                                data_point['bb_200_upper_band'],
                                data_point['bb_5_lower_band'],
                                data_point['bb_20_lower_band'],
                                data_point['bb_50_lower_band'],
                                data_point['bb_200_lower_band'],
                            ),
                        )

                        total_rowcount += 1

                    if total_rowcount > 0:
                        print(f"Total rows inserted into BoillingerBands table: {total_rowcount}")
                    else:
                        print("No rows were affected. Possible duplicate or failed insert into BoillingerBands table.")

        except Exception as e:
            print(f"Error inserting Boillinger Bands data into the database: {e}")

    async def insert_relative_indexes(self, relative_indexes_data):
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    total_rowcount = 0 
                    for data_point in relative_indexes_data:
                        query = """
                            INSERT INTO "RelativeIndexes" ("cal_id", "transaction_id", "stock_id", "ticker_symbol", "date",
                                                        "14_days_rsi", "20_days_rsi", "50_days_rsi", "200_days_rsi")
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
                        """
                        await connection.execute(
                            query,
                            *(
                                data_point['cal_id'],
                                data_point['transaction_id'],
                                data_point['stock_id'],
                                data_point['ticker_symbol'],
                                data_point['date'],
                                data_point['14_days_rsi'],
                                data_point['20_days_rsi'],
                                data_point['50_days_rsi'],
                                data_point['200_days_rsi'],
                            ),
                        )
                        total_rowcount += 1

                    if total_rowcount > 0:
                        print(f"Total rows inserted into RelativeIndexes table: {total_rowcount}")
                    else:
                        print("No rows were affected. Possible duplicate or failed insert into RelativeIndexes table.")

        except Exception as e:
            print(f"Error inserting Relative Indexes data into the database: {e}")

    # ---- Async Insert Operations to Stocks and Techincal Tables (if have) ----
    # insert new stock data with both price movements and techincal_data (optional)
    async def insert_data(self, stock_data, technical_data=None):
        try:
            async with self.pool.acquire() as connection:
                async with connection.transaction():
                    await self.insert_stock_data_table(stock_data)

                    # Check if technical_data is not None and insert into respective tables
                    if technical_data:
                        await self.insert_technical_data(technical_data)
        except Exception as e:
            print(f"Error during async_insert_data_async: {e}")


    # Extract necessary data and call the respective async insert functions
    async def insert_technical_data(self, technical_data):
        try:
            moving_averages_data = technical_data[['cal_id', 'transaction_id', 'stock_id', 'ticker_symbol', 'date',
                                                'ma_5_days_sma', 'ma_20_days_sma', 'ma_50_days_sma', 'ma_200_days_sma',
                                                'ma_5_days_ema', 'ma_20_days_ema', 'ma_50_days_ema', 'ma_200_days_ema']].toPandas().to_dict('records')

            boillinger_bands_data = technical_data[['cal_id', 'transaction_id', 'stock_id', 'ticker_symbol', 'date',
                                                    'bb_5_upper_band', 'bb_20_upper_band', 'bb_50_upper_band', 'bb_200_upper_band',
                                                    'bb_5_lower_band', 'bb_20_lower_band', 'bb_50_lower_band', 'bb_200_lower_band']].toPandas().to_dict('records')

            relative_indexes_data = technical_data[['cal_id', 'transaction_id', 'stock_id', 'ticker_symbol', 'date',
                                                    '14_days_rsi', '20_days_rsi', '50_days_rsi', '200_days_rsi']].toPandas().to_dict('records')

            await self.insert_moving_averages(moving_averages_data)
            await self.insert_boillinger_bands(boillinger_bands_data)
            await self.insert_relative_indexes(relative_indexes_data)

        except Exception as e:
            print(f"Error during async_insert_technical_data: {e}")

    async def view_exists(self, view_name):
        """
        Check if a view exists in the database.

        Parameters:
        - view_name: Name of the view to check

        Returns:
        - True if the view exists, False otherwise
        """
        try:
            async with self.pool.acquire() as connection:
                # Use the async_check_view_exists function to check if the view exists
                query = f"SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_name = '{view_name}')"
                result = await connection.fetch(query)

                return result
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error checking view existence: {error}")
            return False
        
    async def process_stock_data(self, ticker_symbol, country, start_date, end_date, technical_requested):
        spark = self.spark

        try:
            result = None
            
            # Check if the company exists and get the stock_ids
            stock_ids = await self.check_company_exists(ticker_symbol, country)

            if stock_ids:
                # Use the first stock_id retrieved
                stock_id = stock_ids[0]
                
                # Use stock_data_exists function to check if data exists in table for the given stock_id, ticker_symbol, and date range
                data_exists = await self.stock_data_exists(stock_id, ticker_symbol, start_date, end_date)

                if data_exists:
                    # Data exists
                    if not technical_requested:
                        # If technical_requested is False, return the stock data
                        
                        result = {
                            "stock_id": stock_id, 
                            "ticker_symbol": ticker_symbol, 
                            "country": country, 
                            "data": data_exists
                        }
                    else:
                        # Check if the view exists before attempting to retrieve data from it
                        view_name = f"stock_technical_view"  

                        if not await self.view_exists(view_name):
                            # If data is not found in the view, try to get it from tables
                            technical_data_from_tables = await self.get_stock_technical_data_from_tables(stock_id, start_date, end_date)

                            if technical_data_from_tables:
                                print(f'{ticker_symbol} has techincal data from joining tables')
                                
                                # If data is found in tables, use it
                                result = {"stock_id": stock_id, "ticker_symbol": ticker_symbol, "country": country, "technical": technical_data_from_tables}
                            else:
                                print(f'Techincal data is not available for {ticker_symbol} from Calcualtion tables!')
                            
                                result = {
                                    "stock_id": stock_id, 
                                    "ticker_symbol": ticker_symbol, 
                                    "country": country, 
                                    "data": data_exists
                                }
                        else:
                            # Try to get technical data from a view
                            technical_data_from_view = await self.get_stock_technical_data_from_view(stock_id, start_date, end_date)

                            if technical_data_from_view:
                                # If data is found in the view, use it
                                technical_data = technical_data_from_view

                                # Print the DataFrame
                                print(f'{ticker_symbol} has Technical Data from View')
                                
                                result = {
                                    "stock_id": stock_id, 
                                    "ticker_symbol": ticker_symbol, 
                                    "country": country, 
                                    "technical": technical_data
                                }
                            else:
                                # Handle the case where the view is empty or data retrieval fails
                                print(f"No data found in the view {view_name}.")

                                # If data is not found in the view, try to get it from tables
                                technical_data_from_tables = await self.get_stock_technical_data_from_tables(stock_id, start_date, end_date)

                                if technical_data_from_tables:
                                    print(f'{ticker_symbol} has techincal data from joining tables')
                                    
                                    # If data is found in tables, use it
                                    result = {"stock_id": stock_id, "ticker_symbol": ticker_symbol, "country": country, "technical": technical_data_from_tables}
                                else:
                                    print(f'Techincal data is not available for {ticker_symbol} from Calcualtion tables!')
                                    result = {
                                        "stock_id": stock_id, 
                                        "ticker_symbol": ticker_symbol, 
                                        "country": country, 
                                        "data": data_exists
                                    }     
                else:
                    # Stock data does not exist in the database
                    print(f"No stock data found for {ticker_symbol} with stock_id {stock_id} in the database. Need to fetch data.")
                    query_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}?symbol={ticker_symbol}&period1=0&period2=9999999999&interval=1d&includePrePost=true&events=div%2Csplit"

                    # Fetch stock data using the query_url and store it in a StockData object
                    arr_stock_data_history = fetch_stock_data_from_url(query_url)
                    stock_data = StockData(stock_id, ticker_symbol, country, data=arr_stock_data_history)
                    
                    # Check if stock_data is not empty before proceeding with Spark processing
                    if not stock_data.data:
                        print("Error: Stock data is empty.")
                    else:

                        # to use in async task later for inserting
                        stock_price_movements = copy.deepcopy(stock_data)
                        technical_data = None
                        
                        if not technical_requested:
                            # If technical_requested is False, return the fetched stock data
                            filter_data = [entry for entry in stock_data.data if start_date <= entry.date <= end_date]
                            stock_data.data = filter_data
                            result = stock_data.to_dict()

                        else:

                            # Process stock data with Spark
                            technical_data, filtered_technical_data = process_stock_data_with_spark(spark, stock_data, start_date, end_date)

                            if filtered_technical_data:
                                # Display unfiltered technical data
                                print("Filtered Technical Data:")

                                # Extract the columns you need
                                stock_id = filtered_technical_data.select("stock_id").first()[0]
                                ticker_symbol = filtered_technical_data.select("ticker_symbol").first()[0]

                                # List of columns to exclude from the final list of dictionaries
                                exclude_columns = ["stock_id", "ticker_symbol"]

                                # Remove the columns from the DataFrame
                                filtered_technical_data = filtered_technical_data.drop(*exclude_columns)
                                filtered_technical_data.show()
                                # Convert DataFrame to list of dictionaries
                                technical_data_list = filtered_technical_data.toPandas().to_dict('records')
                            
                                # Create the final dictionary
                                result = {"stock_id": stock_id, "ticker_symbol": ticker_symbol, "country": country, "technical": technical_data_list}

                            else:
                                print("Error: Unable to process stock data with Spark.")
                                
                                filter_data = [entry for entry in stock_data.data if start_date <= entry.date <= end_date]
                                stock_data.data = filter_data
                                result = stock_data.to_dict()   
                            
                        # inserting price movement async 
                        print(f'{ticker_symbol} is being inserted to the table')
                    
                        loop = asyncio.get_event_loop()
                        if technical_data:
                            loop.create_task(self.insert_data(stock_price_movements, technical_data))
                        else:
                            loop.create_task(self.insert_data(stock_price_movements))

                        print(f'Continue while inserting')
            else:
                # Company does not exist, you may want to handle this case accordingly
                print("Company does not exist. Handle this case accordingly.")
                result = None
            
            return result

        except (Exception, psycopg2.DatabaseError) as error:
            # Handle database errors
            print(f"Database error: {error}")
            return None

     
