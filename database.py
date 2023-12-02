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
from psycopg2 import sql 
from finance import fetch_stock_data_from_url, PriceMovement
from dotenv import load_dotenv

from spark_processor import process_stock_data_with_spark
from asyncpg.pool import create_pool 

# Load the environment variables from the .env file
load_dotenv()


# ---- Non-Async Operations ----
# ============================================================================
# ============================================================================


#Connection parameters for the PostgreSQL server
db_params = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'database': os.environ.get('DB_NAME')
}


# ========== StockData ==========
    
# Define a class to represent stock data
class StockData:
    def __init__(self, stock_id, ticker_symbol, country, data=None):
        self.stock_id = stock_id
        self.ticker_symbol = ticker_symbol
        self.country = country
        self.data = data if data else []

    def __str__(self):
        return f"Stock ID: {self.stock_id}, Ticker Symbol: {self.ticker_symbol}, Data: {self.data}"

    def read_properties(self):
        properties = vars(self)
        for key, value in properties.items():
            if key == 'data' and isinstance(value, dict):
                print("Data:")
                for data_key, data_value in value.items():
                    print(f"  {data_key}: {data_value}")
            else:
                print(f"{key}: {value}")

    def to_dict(self):
        return {
            'stock_id': str(self.stock_id),
            'ticker_symbol': self.ticker_symbol,
            'country': self.country,
            'data': [price_movement.to_dict() for price_movement in self.data]
        }

# Function to create a database connection with error handling
def create_connection():
    try:
        return psycopg2.connect(**db_params)
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e.with_traceback}")
        # You can choose to handle the error or re-raise it here
        raise e

def execute_sql(connection, sql_statements):
    try:
        with connection.cursor() as cursor:
            for sql_statement in sql_statements:
                cursor.execute(sql_statement)
        connection.commit()
        print("SQL statements executed successfully.")
    except Exception as e:
        print(f"Error: Unable to execute SQL statements. {e}")

def get_stocks_ticker_id_exist():
    connection = create_connection()
    cursor = connection.cursor()
    try:
        query = "SELECT DISTINCT stock_id, ticker_symbol FROM \"Stocks\""
        cursor.execute(query)

        stocks_info = []
        results = cursor.fetchall()

        for result in results:
            stock_id, ticker_symbol = result
            stock={
                'stock_id': stock_id,
                'ticker_symbol': ticker_symbol
            }
            stocks_info.append(stock)
        
        return stocks_info
    except Exception as e:
        print(f"Error geting stock ticker & its id from the database: {e}")
    finally:
        cursor.close()

# Function to fetch stock data from the database
def fetch_stock_data_history_from_db(connection, stock_id, ticker_symbol):
    cursor = connection.cursor()
    query = """
    SELECT "transaction_id", "date", "low", "open", "high", "volume", "close"
    FROM "Stocks"
    WHERE "stock_id" = %s AND "ticker_symbol" = %s
    ORDER BY "date" ASC;
    """
    cursor.execute(query, (str(stock_id[0]), ticker_symbol))

    # Fetch all rows and store them as a list of dictionaries
    data = []
    for row in cursor.fetchall():
        transaction_id, date, low, open, high, volume, close = row
        data.append(PriceMovement(transaction_id, date, low, open, high, volume, close))

    cursor.close()
    return data

# insert stock data to db 
def insert_stock_data_into_db(connection, stock_data):
    cursor = connection.cursor()
    try:
        total_rowcount = 0  # Initialize a variable to track the total rowcount

        for data_point in stock_data.data:
            query = """
                INSERT INTO "Stocks" ("transaction_id", "stock_id", "ticker_symbol", "date", "low", "open", "high", "volume", "close")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(query, (data_point.transaction_id, stock_data.stock_id, stock_data.ticker_symbol, data_point.date, data_point.low, data_point.open_price, data_point.high, data_point.volume, data_point.close))
            
            # Add the rowcount of the current execute to the total rowcount
            total_rowcount += cursor.rowcount
        
        # Check if any rows were affected
        if total_rowcount > 0:
            print(f"Total rows inserted: {total_rowcount}")
            connection.commit()
        else:
            print("No rows were affected. Possible duplicate or failed insert.")
            connection.rollback()

        return total_rowcount  # Return the total rowcount

    except Exception as e:
        connection.rollback()
        print(f"Error inserting stock data into the database: {e}")
        return 0  # Return 0 in case of an error
    finally:
        cursor.close()
        

def process_stock(ticker_symbol, country):
    arr_stock_data_history = []
    stock_data = ''
    connection = create_connection()

    stock_id = check_company_exists(connection, ticker_symbol, country)

    if stock_id:
        print(f"Company with ticker symbol {ticker_symbol} in {country} exists in the database with stock_id: {stock_id}")
        try:
            if not stock_data_exists(connection, stock_id, ticker_symbol):
                print(f"No stock data found for {ticker_symbol} with stock_id {stock_id} in the database. Need to fetch data.")
                query_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}?symbol={ticker_symbol}&period1=0&period2=9999999999&interval=1d&includePrePost=true&events=div%2Csplit"

                # Fetch stock data using the query_url and store it in a StockData object
                arr_stock_data_history = fetch_stock_data_from_url(query_url)
                stock_data = StockData(stock_id, ticker_symbol, country, data=arr_stock_data_history)

                # Insert the fetched stock data into the database
                insert_stock_data_into_db(connection, stock_data)

                print(f"Stock data for {ticker_symbol} with stock_id {stock_id} fetched from Yahoo Finance and inserted into the database.")
            else:
                # Fetch stock data from the database and store it in a StockData object
                # arr_stock_data_history will contain the transaction_id in this scenario
                arr_stock_data_history = fetch_stock_data_history_from_db(connection, stock_id, ticker_symbol)
                stock_data = StockData(str(stock_id[0]), ticker_symbol, country, data=arr_stock_data_history)
                print(f"Stock data for {ticker_symbol} with stock_id {stock_id} already exists in the database.")
        except Exception as e:
            print(f"An error occurred while processing stock data: {e}")
    
    
    # Close the database connection
    connection.close()
    return stock_data

def filter_stock_data_by_date_range(stock_data_history, start_date, end_date):
    """
    Filter stock data based on a date range.

    Parameters:
    - stock_data_history: List of dictionaries representing stock data
    - start_date: Start date of the range (format: 'YYYY-MM-DD')
    - end_date: End date of the range (format: 'YYYY-MM-DD')

    Returns:
    - List of dictionaries representing filtered stock data
    """
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        filtered_data = [
            entry for entry in stock_data_history
            if start_date <= datetime.strptime(str(entry.date), '%Y-%m-%d') <= end_date
        ]

        return filtered_data

    except ValueError as e:
        print(f"Error parsing date: {e}")
        return None


def update_stock_data_daily(stockTickerIds):

    combined_data = []

    for stock in stockTickerIds:
        stock_id = stock['stock_id']
        ticker_symbol = stock['ticker_symbol']

        # Create a Ticker object for the stock
        stock = yfinance.Ticker(ticker_symbol)

        # Get daily historical data for the stock
        hist_data = stock.history(period="1d")  # Daily data for the past 1 day
        
        # Check if data is available before formatting
        if not hist_data.empty:
            # formatted_data = {
            #     'stock_id': stock_id,
            #     'ticker_symbol': ticker_symbol,
            #     'data':[]
            # }

            # for data in hist_data:
            #     print(data)
            #     # daily_data = {
            #     #     # 'date': data.index[-1].strftime('%Y-%m-%d'),  # Get the date of the latest data point
            #     #     'low': float(data['Low'].iloc[-1]),  # Get the latest low price
            #     #     'open': float(data['Open'].iloc[-1]),  # Get the latest open price
            #     #     'volume': int(data['Volume'].iloc[-1]),  # Get the latest volume
            #     #     'high': float(data['High'].iloc[-1]),  # Get the latest high price
            #     #     'close': float(data['Close'].iloc[-1]),  # Get the latest closing price
            #     # }
            #     formatted_data['data'].append(data)
            
            arr_stock_data = []
            stock_data = {
                'date': pd.to_datetime(hist_data.index[-1]).strftime('%Y-%m-%d'),  # Get the date of the latest data point
                'low': float(hist_data['Low'].iloc[-1]),  # Get the latest low price
                'open': float(hist_data['Open'].iloc[-1]),  # Get the latest open price
                'volume': int(hist_data['Volume'].iloc[-1]),  # Get the latest volume
                'high': float(hist_data['High'].iloc[-1]),  # Get the latest high price
                'close': float(hist_data['Close'].iloc[-1]),  # Get the latest closing price
            }
            arr_stock_data.append(stock_data)
            stock = StockData(stock_id, ticker_symbol, None, data=arr_stock_data)

            combined_data.append(stock)
        else:
            print(f"Failed to fetch data for symbol {stock}! No daily trading data available for today.")

    return combined_data

def save_stock_data_to_csv(stock_data, ticker_symbol, output_dir):
    # Convert the stock data to a Pandas DataFrame
    df = pd.DataFrame(stock_data)

    # Define the path to save the CSV file
    output_path = os.path.join(output_dir, f"{ticker_symbol}.csv")

    # Save the DataFrame to CSV
    df.to_csv(output_path, index=False)

def get_stocks_data_available():
    # Assuming you have a function to get the list of stocks with their IDs and ticker symbols
    stocks_info = get_stocks_ticker_id_exist()

    # Assuming you have a function to create a database connection
    connection = create_connection()

    # Specify the output directory for saving CSV files
    output_directory = "./data-processing/stocks-data"

    for stock_info in stocks_info:
        stock_id = stock_info['stock_id']
        ticker_symbol = stock_info['ticker_symbol']

        # Fetch stock data from the database
        stock_data = fetch_stock_data_history_from_db(connection, stock_id, ticker_symbol)

        # Save stock data to a CSV file
        save_stock_data_to_csv(stock_data, ticker_symbol, output_directory)

    # Close the database connection
    connection.close()


def get_stocks_data_combined_to_csv():
    stocks_info = []
    # Assuming you have a function to get the list of stocks with their IDs and ticker symbols
    stocks_info = get_stocks_ticker_id_exist()

    # Create an empty DataFrame to store combined stock data
    combined_stock_data = pd.DataFrame()

    # Assuming you have a function to create a database connection
    connection = create_connection()

    for stock_info in stocks_info:
        stock_id = stock_info['stock_id']
        ticker_symbol = stock_info['ticker_symbol']

        # Fetch stock data from the database
        stock_data_list = fetch_stock_data_history_from_db(connection, stock_id, ticker_symbol)

        # Convert the list to a DataFrame
        stock_data = pd.DataFrame(stock_data_list)

        # Add stock ID and ticker symbol columns to the DataFrame
        stock_data['stock_id'] = stock_id
        stock_data['ticker_symbol'] = ticker_symbol
        print(stock_data)
        # Reorder columns
        stock_data = stock_data[['transaction_id', 'stock_id', 'ticker_symbol', 'date', 'low', 'open', 'high', 'volume', 'close']]

        # Combine the stock data with the existing DataFrame
        combined_stock_data = pd.concat([combined_stock_data, stock_data], ignore_index=True)

    # Specify the output file path for saving the combined CSV file
    output_file_path = "./data-processing/stocks-data/combined-stocks-data.csv"

    # Save the combined stock data to a CSV file
    combined_stock_data.to_csv(output_file_path, index=False)

    # Close the database connection
    connection.close()

# ========== User ==========
# This section contains functions and classes related to user data.

class User:
    def __init__(self, user_id, username, password, email, first_name, last_name, date_of_birth, join_date):
        self.user_id = user_id
        self.username = username
        self.password = password
        self.email = email
        self.first_name = first_name
        self.last_name = last_name
        self.date_of_birth = date_of_birth
        self.join_date = join_date
def create_user(username, password, email, first_name, last_name, date_of_birth):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()
        
        # Create a cursor object to interact with the database
        cur = conn.cursor()
        
        # Define the SQL INSERT statement to add a new user
        insert_query = """
            INSERT INTO "Users" (username, password, email, first_name, last_name, date_of_birth, join_date)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE)  -- Use CURRENT_DATE for join_date
            RETURNING user_id
        """
        
        # Execute the INSERT statement with the user's information
        cur.execute(insert_query, (username, password, email, first_name, last_name, date_of_birth))
        
        # Get the user_id of the newly created user
        user_id = cur.fetchone()[0]
        
        # Commit the transaction and close the cursor and connection
        conn.commit()
        cur.close()
        conn.close()
        
        return user_id
    except Exception as e:
        # Handle any exceptions that may occur during user creation
        raise e

def find_user_by_username(username):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL query to search for a user by username
        query = """
            SELECT * FROM "Users" WHERE username = %s
        """
        cursor.execute(query, (username,))

        # Fetch the user data if a match is found
        user_data = cursor.fetchone()

        # Close the cursor and the database connection
        cursor.close()
        conn.close()
        if user_data:
            # Convert user data into a User object
            user = User(*user_data)
            print(user)
            return user
        else:
            return None

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while searching for a user:", error)
        return None

def find_user_by_id(user_id):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL query to search for a user by user_id
        query = """
            SELECT * FROM "Users" WHERE user_id = %s
        """
        cursor.execute(query, (user_id,))

        # Fetch the user data if a match is found
        user_data = cursor.fetchone()

        # Close the cursor and the database connection
        cursor.close()
        conn.close()

        if user_data:
            # Convert user data into a User object or dictionary
            # Depending on your preferred data structure
            user = User(user_data[0], user_data[1], user_data[2], user_data[3], user_data[4], user_data[5], user_data[6], user_data[7])
            return user
        else:
            return None

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while searching for a user:", error)
        return None


# ========== Watchlist ==========
# This section contains functions and classes related to watchlists.
 
class Watchlist:
    def __init__(self, watchlist_id, user_id, watchlist_name, stocks=[]):
        self.watchlist_id = watchlist_id
        self.user_id = user_id
        self.watchlist_name = watchlist_name
        self.stocks = stocks if stocks else []

    def add_stock(self, arr_stock_info):
        for stock in arr_stock_info:
            self.stocks.append({'stock_id': stock['stock_id'], 'ticker_symbol': stock['ticker_symbol']})
    
    def print_info(self):
        print(f"Watchlist ID: {self.watchlist_id}")
        print(f"User ID: {self.user_id}")
        print(f"Watchlist Name: {self.watchlist_name}")
        print("Stocks:")
        for stock in self.stocks:
            print(f"Stock ID: {stock['stock_id']}, Ticker Symbol: {stock['ticker_symbol']}")
    
    def to_dict(self):
        return {
            'watchlist_id': self.watchlist_id,
            'user_id': self.user_id,
            'watchlist_name': self.watchlist_name,
            'stocks': self.stocks
        }
def find_watchlist(user_id, watchlist_name):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL query to search for a watchlist by user_id and watchlist_name
        query = """
            SELECT * FROM "Watchlist" WHERE user_id = %s AND watchlist_name = %s
        """
        cursor.execute(query, (user_id, watchlist_name))

        # Fetch the watchlist data if a match is found
        watchlist_data = cursor.fetchone()

        # Close the cursor and the database connection
        cursor.close()
        conn.close()

        if watchlist_data:
            # Convert watchlist data into a Watchlist object
            watchlist = Watchlist(*watchlist_data)
            return watchlist
        else:
            return None

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while searching for a watchlist:", error)
        return None

def find_watchlist_by_id(watchlist_id):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object
        cursor = conn.cursor()

        # Define the SQL query to retrieve the watchlist by watchlist_id
        query = """
            SELECT watchlist_id, user_id, watchlist_name
            FROM "Watchlist"
            WHERE watchlist_id = %s
        """
        
        cursor.execute(query, (watchlist_id,))
        watchlist_data = cursor.fetchone()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        if watchlist_data:
            watchlist_id, user_id, watchlist_name = watchlist_data
            return {
                "watchlist_id": watchlist_id,
                "user_id": user_id,
                "watchlist_name": watchlist_name
            }
        else:
            return None

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while finding watchlist by ID:", error)
        return None

def get_watchlist(user_id):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object with named cursors
        cursor = conn.cursor()

        # Define the SQL query to retrieve the user's watchlist(s)
        query = """
            SELECT watchlist_id, user_id, watchlist_name
            FROM "Watchlist"
            WHERE user_id = %s
        """
        cursor.execute(query, (user_id,))

        # Fetch the watchlist data if any matches are found
        watchlists_data = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # If there are no watchlists, return an empty list
        if not watchlists_data:
            return []

        # Convert the result to a list of dictionaries
        watchlists = []
        for data in watchlists_data:
            watchlist = Watchlist(*data)
            watchlist.add_stock(get_stocks_in_watchlist(watchlist.watchlist_id))
            watchlists.append(watchlist)

        return watchlists

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while retrieving watchlists:", error)
        return None

def get_stocks_in_watchlist(watchlist_id):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object
        cursor = conn.cursor()

        # Define the SQL query to retrieve the stocks in a specific watchlist
        query = """
            SELECT stock_id, ticker_symbol
            FROM "StocksInWatchlist"
            WHERE watchlist_id = %s
        """
        cursor.execute(query, (watchlist_id,))

        # Fetch the stock data if any matches are found
        stocks_data = cursor.fetchall()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # If there are no stocks, return an empty list
        if not stocks_data:
            return []

        # Convert the result to a list of dictionaries
        stocks = []
        for data in stocks_data:
            stock = {
                'stock_id': data[0],
                'ticker_symbol': data[1]
            }
            stocks.append(stock)

        return stocks

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while retrieving stocks in watchlist:", error)
        return None

def create_watchlist(user_id, watchlist_name):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Generate a UUID for the watchlist_id
        watchlist_id = str(uuid.uuid4())

        # Define the SQL INSERT statement to add a new watchlist
        insert_query = """
            INSERT INTO "Watchlist" (watchlist_id, user_id, watchlist_name)
            VALUES (%s, %s, %s)
        """

        # Execute the INSERT statement with the user's information
        cursor.execute(insert_query, (watchlist_id, user_id, watchlist_name))

        # Commit the transaction and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()

        return watchlist_id

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating a watchlist:", error)
        raise error

def add_stock_to_watchlist(watchlist_id, stock_id, ticker_symbol):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL INSERT statement to add a stock to a watchlist
        insert_query = """
            INSERT INTO "StocksInWatchlist" (watchlist_id, stock_id, ticker_symbol)
            VALUES (%s, %s, %s)
        """

        # Execute the INSERT statement with the stock information
        cursor.execute(insert_query, (watchlist_id, stock_id, ticker_symbol))

        # Commit the transaction and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while adding a stock to the watchlist:", error)
        raise error

def delete_watchlist(watchlist_id):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL DELETE statement to remove a watchlist
        delete_query = """
            DELETE FROM "Watchlist"
            WHERE watchlist_id = %s
        """

        # Execute the DELETE statement with the watchlist_id
        cursor.execute(delete_query, (watchlist_id,))

        # Commit the transaction and close the cursor and connection
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while deleting a watchlist:", error)
        raise error

def update_watchlist_info(watchlist_id, new_watchlist_name):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL UPDATE statement to modify the watchlist's information
        update_query = """
            UPDATE "Watchlist"
            SET watchlist_name = %s
            WHERE watchlist_id = %s
        """

        # Execute the UPDATE statement with the new data
        cursor.execute(update_query, (new_watchlist_name, watchlist_id))

        # Commit the transaction to save the changes
        conn.commit()

        # Close the cursor and the database connection
        cursor.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while updating watchlist:", error)
        raise error
    
def update_watchlist_stocks_info(watchlist_id, updated_stocks):
    try:
        # Establish a connection to the PostgreSQL database
        conn = create_connection()

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Define the SQL DELETE statement to remove all existing stocks in the watchlist
        delete_query = """
            DELETE FROM "StocksInWatchlist"
            WHERE watchlist_id = %s
        """

        # Execute the DELETE statement to remove existing stocks
        cursor.execute(delete_query, (watchlist_id,))

        # Define the SQL INSERT statement to add updated stocks to the watchlist
        insert_query = """
            INSERT INTO "StocksInWatchlist" (watchlist_id, stock_id, ticker_symbol)
            VALUES (%s, %s, %s)
        """

        # Execute the INSERT statement to add the updated stocks
        for stock in updated_stocks:
            cursor.execute(insert_query, (watchlist_id, stock['stock_id'], stock['ticker_symbol']))

        # Commit the transaction to save the changes
        conn.commit()

        # Close the cursor and the database connection
        cursor.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while updating watchlist stocks:", error)
        raise error

def get_transaction_ids_and_dates(stock_id, ticker_symbol):
    connection = create_connection()
    cursor = connection.cursor()
    query = """
    SELECT "transaction_id", "date"
    FROM "Stocks"
    WHERE "stock_id" = %s AND "ticker_symbol" = %s
    """
    cursor.execute(query, (stock_id, ticker_symbol))

    # Fetch all rows as a list of tuples
    results = cursor.fetchall()

    cursor.close()
    connection.close()

    return results

def fetch_moving_averages_data_from_db(stock_id, ticker_symbol):
    data = []
    connection = None
    cursor = None

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
        SELECT "cal_id", "transaction_id", "date", "5_days_sma", "20_days_sma",
            "50_days_sma", "200_days_sma", "5_days_ema", "20_days_ema",
            "50_days_ema", "200_days_ema"
        FROM "MovingAverages"
        WHERE "stock_id" = %s AND "ticker_symbol" = %s
        ORDER BY "date" ASC;
        """

        cursor.execute(query, (str(stock_id), str(ticker_symbol)))

        # Fetch all rows and append them to the result
        for row in cursor.fetchall():
            cal_id, transaction_id, date, sma_5, sma_20, sma_50, sma_200, ema_5, ema_20, ema_50, ema_200 = row
            data.append({
                "cal_id": cal_id,
                "transaction_id": transaction_id,
                "date": date,  # Change "Date" to "date"
                "5_days_sma": sma_5,
                "20_days_sma": sma_20,
                "50_days_sma": sma_50,
                "200_days_sma": sma_200,
                "5_days_ema": ema_5,
                "20_days_ema": ema_20,
                "50_days_ema": ema_50,
                "200_days_ema": ema_200,
            })

    except Exception as e:
        print(f"Error fetching moving averages data: {e}")
        data = []  # Set data to an empty list in case of an error

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()

    return data

def fetch_boillinger_bands_data_from_db(stock_id, ticker_symbol):
    data = []
    connection = None
    cursor = None

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
        SELECT "cal_id", "transaction_id", "date", "5_upper_band", "20_upper_band",
            "50_upper_band", "200_upper_band", "5_lower_band", "20_lower_band",
            "50_lower_band", "200_lower_band"
        FROM "BoillingerBands"
        WHERE "stock_id" = %s AND "ticker_symbol" = %s
        ORDER BY "date" ASC;
        """

        cursor.execute(query, (str(stock_id), str(ticker_symbol)))

        # Fetch all rows and append them to the result
        for row in cursor.fetchall():
            cal_id, transaction_id, date, upper_5, upper_20, upper_50, upper_200, lower_5, lower_20, lower_50, lower_200 = row
            data.append({
                "cal_id": cal_id,
                "transaction_id": transaction_id,
                "date": date,
                "5_upper_band": upper_5,
                "20_upper_band": upper_20,
                "50_upper_band": upper_50,
                "200_upper_band": upper_200,
                "5_lower_band": lower_5,
                "20_lower_band": lower_20,
                "50_lower_band": lower_50,
                "200_lower_band": lower_200,
            })

    except Exception as e:
        print(f"Error fetching Boillinger Bands data: {e}")
        data = []  # Set data to an empty list in case of an error

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()

    return data


def fetch_relative_indexes_data_from_db(stock_id, ticker_symbol):
    data = []
    connection = None
    cursor = None

    try:
        connection = create_connection()
        cursor = connection.cursor()

        query = """
        SELECT "cal_id", "transaction_id", "date", "14_days_rsi", "20_days_rsi",
            "50_days_rsi", "200_days_rsi"
        FROM "RelativeIndexes"
        WHERE "stock_id" = %s AND "ticker_symbol" = %s
        ORDER BY "date" ASC;
        """

        cursor.execute(query, (str(stock_id), str(ticker_symbol)))

        # Fetch all rows and append them to the result
        for row in cursor.fetchall():
            cal_id, transaction_id, date, rsi_14, rsi_20, rsi_50, rsi_200 = row
            data.append({
                "cal_id": cal_id,
                "transaction_id": transaction_id,
                "date": date,
                "14_days_rsi": rsi_14,
                "20_days_rsi": rsi_20,
                "50_days_rsi": rsi_50,
                "200_days_rsi": rsi_200,
            })

    except Exception as e:
        print(f"Error fetching Relative Indexes data: {e}")
        data = []  # Set data to an empty list in case of an error

    finally:
        if cursor is not None:
            cursor.close()

        if connection is not None:
            connection.close()

    return data


# ---- Async Operations ----
# ============================================================================
# ============================================================================
# Connection parameters for the PostgreSQL server
async_db_params = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'database': os.environ.get('DB_NAME'),
    'min_size': 5,  # Minimum number of connections in the pool (adjust as needed)
    'max_size': 10,  # Maximum number of connections in the pool (adjust as needed)
    'max_queries': 500,  # Maximum number of queries a connection can execute before being released (adjust as needed)
}

# Function to create a database connection pool
async def async_create_connection_pool():
    try:
        pool = await create_pool(**async_db_params)
        return pool
    except asyncpg.PostgresError as e:
        print(f"Error creating connection pool: {e}")
        # Handle the error or re-raise it
        raise e

# Function to create a database connection
async def async_create_connection():
    pool = await async_create_connection_pool()
    if pool:
        connection = await pool.acquire()
        return pool, connection
    else:
        raise RuntimeError("Connection pool is not available.")

# Function to check if a company exists in the database
async def async_check_company_exists(connection, ticker_symbol, country):
    try:
        # SQL query to check for the existence of a company and get stock_id based on ticker symbol and country
        query = "SELECT \"stock_id\" FROM \"CompanyInformation\" WHERE \"ticker_symbol\" = $1 AND \"country\" = $2;"
        result = await connection.fetch(query, ticker_symbol, country)

        stock_ids = [row['stock_id'] for row in result]
        return stock_ids
    except asyncpg.PostgresError as e:
        print(f"Error checking company existence: {e}")
        # Handle the error or re-raise it
        raise e

async def async_stock_data_exists(connection, stock_id, ticker_symbol, start_date=None, end_date=None):
    try:
        # Select specific columns for the given stock_id, ticker_symbol, and date range
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
            query += 'AND "date" >= $3 '
        if end_date is not None:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            query += 'AND "date" <= $4 '

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


async def get_date_range_for_view(view_name, connection):
    try:
        async with connection.transaction():
            async with connection.cursor() as cursor:
                await cursor.execute("SELECT start_date, end_date FROM view_date_ranges WHERE view_name = %s;", (view_name,))
                result = await cursor.fetchone()
                return result  # This will be (start_date, end_date) or None if the view_name is not found
    except asyncpg.exceptions.PostgresError as e:
        print(f"Error fetching date range for view {view_name}: {e}")
        return None

async def get_view_date_range(conn, view_name):
    try:
        async with conn.transaction():
            async with conn.cursor() as cursor:
                # Retrieve the date range for the given view
                await cursor.execute(
                    "SELECT start_date, end_date FROM public.views_date_ranges WHERE view_name = %s",
                    (view_name,)
                )
                date_range = await cursor.fetchone()

                if date_range:
                    start_date, end_date = date_range
                    formatted_start_date = start_date.strftime("%Y-%m-%d") if start_date else None
                    formatted_end_date = end_date.strftime("%Y-%m-%d") if end_date else None
                    return formatted_start_date, formatted_end_date
                else:
                    return None, None
    except asyncpg.exceptions.PostgresError as e:
        print(f"Error fetching date range for view {view_name}: {e}")
        return None, None

async def async_create_view_date_range_table(conn):
    try:
        # Check if the table exists
        table_exists = await conn.fetchval("SELECT to_regclass('public.views_date_ranges')")

        if not table_exists:
            # If it doesn't exist, create the table
            create_table_sql = """
                CREATE TABLE public.views_date_ranges (
                    view_name VARCHAR(255) PRIMARY KEY,
                    start_date DATE,
                    end_date DATE
                );
            """
            await conn.execute(create_table_sql)

    except Exception as e:
        print(f"Error creating views_date_ranges table: {e}")

async def async_update_view_date_range(conn, view_name, start_date, end_date):
    try:
        # Update or insert the date range for the given view
        upsert_sql = """
            INSERT INTO public.views_date_ranges (view_name, start_date, end_date)
            VALUES ($1, $2, $3)
            ON CONFLICT (view_name) DO UPDATE
            SET start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date;
        """
        await conn.execute(upsert_sql, view_name, start_date, end_date)

    except Exception as e:
        print(f"Error updating views_date_ranges table: {e}")

def check_date_range_overlap(existing_start, existing_end, requested_start, requested_end):
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

async def async_get_stock_technical_data_from_tables(connection, stock_id, start_date=None, end_date=None):
    try:
        # Format start_date and end_date if they are provided
        formatted_start_date = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
        formatted_end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

        data = []

        # Construct the query with optional date range conditions
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

        # Add optional date range conditions
        if start_date is not None:
            query += 'WHERE S."date" >= $1 '
        if end_date is not None:
            query += 'AND S."date" <= $2 '

        # Add condition to filter by stock_id
        query += 'AND S."stock_id" = $3 '

        query += 'ORDER BY S."date" ASC LIMIT 10;'

        # Execute the query with parameters
        if start_date is not None and end_date is not None:
            result = await connection.fetch(query, formatted_start_date, formatted_end_date, stock_id)
        else:
            result = await connection.fetch(query, stock_id)

        # Fetch all rows as a list of dictionaries
        columns = [desc[0] for desc in result.description]
        data = [dict(row) for row in result]

        return data

    except Exception as e:
        print(f"Error fetching technical data: {e}")
        return None

async def async_create_or_refresh_materialized_view_with_partition(conn):
    try:
        # Calculate start and end dates dynamically (e.g., 10 years from now)
        current_date = datetime.now()
        start_date = current_date - timedelta(days=365 * 10)
        end_date = current_date
    
        # Open a cursor to perform database operations
        async with conn.transaction():
            # Create the views_date_ranges table if it doesn't exist
            await async_create_view_date_range_table(conn)

            # Check if the materialized view exists
            view_exists = await conn.fetchval(
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
                        S.date >= '{}' AND S.date <= '{}'
                    ORDER BY
                        S.date DESC;
                """.format(start_date, end_date)

                await conn.execute(create_view_sql)

                # Create indexes
                create_indexes_sql = """
                    CREATE INDEX idx_materialized_view_combined ON stock_technical_view(stock_id, ticker_symbol, date);
                    CREATE INDEX idx_materialized_view_date ON stock_technical_view(date);
                """
                await conn.execute(create_indexes_sql)

            else:
                # If it exists, refresh the materialized view
                refresh_view_sql = "REFRESH MATERIALIZED VIEW stock_technical_view;"
                await conn.execute(refresh_view_sql)

        # Update the date range for the view
        await async_update_view_date_range(conn, 'stock_technical_view', start_date, end_date)

    except Exception as e:
        print(f"Error: {e}")


async def async_get_stock_technical_data_from_view(connection, stock_id, start_date, end_date):
    try:
        # Convert date strings to datetime.date objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

        # Select data from stock_technical_view based on the date range
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
        result = await connection.fetch(select_data_sql, start_date, end_date, stock_id)

        # Convert the result set to a list of dictionaries
        result_list = [dict(row) for row in result]

        return result_list    

    except asyncpg.PostgresError as e:
        print(f"Error: {e}")
        # Handle the error or re-raise it
        raise e

async def async_check_stock_exists_in_view(conn, stock_id, ticker_symbol):
    try:
        # SQL query to check if the stock exists in the materialized view
        query = """
            SELECT 1 
            FROM stock_technical_view 
            WHERE stock_id = $1 AND ticker_symbol = $2
            LIMIT 1;
        """
        result = await conn.fetch(query, stock_id, ticker_symbol)

        return len(result) > 0

    except Exception as e:
        print(f"Error checking if stock exists in the view: {e}")
        return False  # Return False in case of an error


async def async_insert_data_async(connection, stock_data):
    try:
        async with connection.transaction():
            cursor = connection.cursor()
            total_rowcount = 0

            for data_point in stock_data.data:
                query = """
                    INSERT INTO "Stocks" ("transaction_id", "stock_id", "ticker_symbol", "date", "low", "open", "high", "volume", "close")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                await cursor.execute(
                    query,
                    (
                        data_point.transaction_id,
                        stock_data.stock_id,
                        stock_data.ticker_symbol,
                        data_point.date,
                        data_point.low,
                        data_point.open_price,
                        data_point.high,
                        data_point.volume,
                        data_point.close,
                    ),
                )
                
                total_rowcount += cursor.rowcount

            if total_rowcount > 0:
                print(f"Total rows inserted: {total_rowcount}")
            else:
                print("No rows were affected. Possible duplicate or failed insert.")

    except Exception as e:
        print(f"Error inserting stock data into the database: {e}")

async def async_insert_data(connection, stock_data):
    try:
        await async_insert_data_async(connection, stock_data)
    except Exception as e:
        print(f"Error during async_insert_data: {e}")



async def process_stock_data(spark, ticker_symbol, country, start_date, end_date, technical_requested):
    try:
        result = None
        # Get a database connection from the pool
        pool, connection = await async_create_connection()
        
        try:
            # Check if the company exists and get the stock_ids
            stock_ids = await async_check_company_exists(connection, ticker_symbol, country)

            if stock_ids:
                # Use the first stock_id retrieved
                stock_id = stock_ids[0]

                # Use stock_data_exists function to check if data exists in table for the given stock_id, ticker_symbol, and date range
                data_exists = await async_stock_data_exists(connection, stock_id, ticker_symbol, start_date, end_date)

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
                        # Try to get technical data from a view
                        technical_data_from_view = await async_get_stock_technical_data_from_view(connection, stock_id, start_date, end_date)

                        if technical_data_from_view:
                            # If data is found in the view, use it
                            technical_data = technical_data_from_view

                            # Print the DataFrame
                            print(f'{ticker_symbol} has Technical Data from View')
                            
                            result = {
                                "stock_id": stock_id, 
                                "ticker_symbol": ticker_symbol, 
                                "country": country, 
                                "technical_view": technical_data
                            }
                        else:
                            # If data is not found in the view, try to get it from tables
                            technical_data_from_tables = await async_get_stock_technical_data_from_tables(connection, stock_id, start_date, end_date)

                            if technical_data_from_tables:
                                # If data is found in tables, use it
                                result = technical_data_from_tables
                            else:
                                # drop that stock in tables and recalculate everything again for that stock only
                                # If no data is found in both view and tables, print a message
                                print("Will implement data processing function!!!!!!!")
                                # result = perform_data_processing(ticker_symbol, country, start_date, end_date, stock_id)
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
                        # inserting price movement async 
                        # print(f'{ticker_symbol} is being inserted to the table')
                        # stock_price_movements = copy.deepcopy(stock_data)
                    
                        # loop = asyncio.get_event_loop()
                        # loop.create_task(async_insert_data(stock_price_movements))

                        # print(f'Continue while inserting')

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
                                filtered_technical_data.show()

                                # Extract the columns you need
                                stock_id = filtered_technical_data.select("stock_id").first()[0]
                                ticker_symbol = filtered_technical_data.select("ticker_symbol").first()[0]

                                # List of columns to exclude from the final list of dictionaries
                                exclude_columns = ["stock_id", "ticker_symbol"]

                                # Remove the columns from the DataFrame
                                filtered_technical_data = filtered_technical_data.drop(*exclude_columns)

                                # Convert DataFrame to list of dictionaries
                                technical_data_list = filtered_technical_data.toPandas().to_dict('records')

                                # Create the final dictionary
                                result = {"stock_id": stock_id, "ticker_symbol": ticker_symbol, "technical": technical_data_list}

                            else:
                                print("Error: Unable to process stock data with Spark.")
            else:
                # Company does not exist, you may want to handle this case accordingly
                print("Company does not exist. Handle this case accordingly.")
                result = None
        finally:
            # Release the connection back to the pool
            await pool.release(connection)
        return result

    except (Exception, psycopg2.DatabaseError) as error:
        # Handle database errors
        print(f"Database error: {error}")
        return None