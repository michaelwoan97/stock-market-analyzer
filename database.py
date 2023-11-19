import json
import os
import uuid
import pandas as pd
import psycopg2
import yfinance
from finance import fetch_stock_data_from_url  # Import the function to fetch stock data from a URL
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Connection parameters for the PostgreSQL server
db_params = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT'),
    'database': os.environ.get('DB_NAME'),
}

# ========== StockData ==========
# This section contains functions and classes related to stock data.

# Define a class to represent stock data
class StockData:
    def __init__(self, stock_id, ticker_symbol, country, transaction_id=str(uuid.uuid4()), data=None):
        self.transaction_id = transaction_id
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
# Function to create a database connection with error handling
def create_connection():
    try:
        return psycopg2.connect(**db_params)
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e.with_traceback}")
        # You can choose to handle the error or re-raise it here
        raise e

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

# Function to check if a company exists in the database
def check_company_exists(connection, ticker_symbol, country):
    cursor = connection.cursor()
    # SQL query to check for the existence of a company and get stock_id based on ticker symbol and country
    query = "SELECT \"stock_id\" FROM \"CompanyInformation\" WHERE \"ticker_symbol\" = %s AND \"country\" = %s;"
    cursor.execute(query, (ticker_symbol, country))
    
    stock_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return stock_ids

# Function to check if stock data exists in the database
def stock_data_exists(connection, stock_id, ticker_symbol):
    
    cursor = connection.cursor()
    # Check if stock data exists in the Stocks table for the given stock_id and ticker_symbol
    query = """
    SELECT EXISTS(
        SELECT 1
        FROM "Stocks"
        WHERE "stock_id" = %s AND "ticker_symbol" = %s
    );
    """
    cursor.execute(query, (stock_id, ticker_symbol))
    exists = cursor.fetchone()[0]
    cursor.close()
    return exists

# Function to fetch stock data from the database
def fetch_stock_data_from_db(connection, stock_id, ticker_symbol):
    cursor = connection.cursor()
    query = """
    SELECT "transaction_id", "date", "low", "open", "high", "volume", "close"
    FROM "Stocks"
    WHERE "stock_id" = %s AND "ticker_symbol" = %s
    ORDER BY "date" ASC;
    """
    cursor.execute(query, (stock_id, ticker_symbol))

    # Fetch all rows and store them as a list of dictionaries
    data = []
    for row in cursor.fetchall():
        transaction_id, date, low, open, high, volume, close = row
        data.append({
            "transaction_id": transaction_id,
            "date": date, 
            "low": low,
            "open": open,
            "high": high,
            "volume": volume,
            "close": close,
        })

    cursor.close()
    return data

def insert_stock_data_into_db(connection, stock_data):
    cursor = connection.cursor()
    try:
        for data_point in stock_data.data:
            # Generate a UUID for the watchlist_id
            transaction_id = str(uuid.uuid4())
            
            query = """
                    INSERT INTO "Stocks" ("transaction_id", "stock_id", "ticker_symbol", "date", "low", "open", "high", "volume", "close")
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
            cursor.execute(query, (transaction_id, stock_data.stock_id, stock_data.ticker_symbol, data_point['date'], data_point['low'], data_point['open'], data_point['high'], data_point['volume'], data_point['close']))
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Error inserting stock data into the database: {e}")
    finally:
        cursor.close()
        

def process_stock(ticker_symbol, country, stock_data_objects):

    connection = create_connection()

    stock_ids = check_company_exists(connection, ticker_symbol, country)

    if stock_ids:
        print(f"Company with ticker symbol {ticker_symbol} in {country} exists in the database with stock_id: {stock_ids}")

        # Assume stock_ids is a list of stock_id values
        for stock_id in stock_ids:
            try:
                if not stock_data_exists(connection, stock_id, ticker_symbol):
                    print(f"No stock data found for {ticker_symbol} with stock_id {stock_id} in the database. Need to fetch data.")
                    query_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}?symbol={ticker_symbol}&period1=0&period2=9999999999&interval=1d&includePrePost=true&events=div%2Csplit"

                    # Fetch stock data using the query_url and store it in a StockData object
                    stock_data = fetch_stock_data_from_url(query_url)
                    stock_data_objects.append(
                        StockData(stock_id, ticker_symbol, country, data=stock_data)
                    )

                    # Insert the fetched stock data into the database
                    for stock_data in stock_data_objects:
                        # Ensure that stock_data_objects contains StockData instances
                        insert_stock_data_into_db(connection, stock_data)

                    print(f"Stock data for {ticker_symbol} with stock_id {stock_id} fetched from Yahoo Finance and inserted into the database.")
                else:
                    # Fetch stock data from the database and store it in a StockData object
                    stock_data = fetch_stock_data_from_db(connection, stock_id, ticker_symbol)
                    stock_data_objects.append(
                        StockData(stock_id, ticker_symbol, country, data=stock_data)
                    )
                    print(f"Stock data for {ticker_symbol} with stock_id {stock_id} already exists in the database.")
            except Exception as e:
                print(f"An error occurred while processing stock data: {e}")

    # Close the database connection
    connection.close()


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
        stock_data = fetch_stock_data_from_db(connection, stock_id, ticker_symbol)

        # Save stock data to a CSV file
        save_stock_data_to_csv(stock_data, ticker_symbol, output_directory)

    # Close the database connection
    connection.close()


def get_stocks_data_combined_to_csv():
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
        stock_data_list = fetch_stock_data_from_db(connection, stock_id, ticker_symbol)

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

# ============================================================================

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
