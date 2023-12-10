import sys
import uuid

import openpyxl
sys.path.append("..")

from asyncpg import connect
import psycopg2
import os 
from dotenv import load_dotenv

# load the environment variables from the .env file
load_dotenv()

# Connection parameters for the PostgreSQL server
default_db_params = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),  # Change to your database server host
    'port': os.environ.get('DB_PORT'),  # Change to your database server port
}

# SQL statements to create tables (with "IF NOT EXISTS" clause)
create_tables_sql = [
    """
    -- Existing Users table
    CREATE TABLE IF NOT EXISTS "Users" (
        "user_id" SERIAL,
        "username" VARCHAR,
        "password" VARCHAR,
        "email" VARCHAR,
        "first_name" VARCHAR,
        "last_name" VARCHAR,
        "date_of_birth" DATE,
        "join_date" DATE,
        PRIMARY KEY ("user_id")
    )
    """,
    """
    -- Existing CompanyInformation table
    CREATE TABLE IF NOT EXISTS "CompanyInformation" (
        "stock_id" UUID UNIQUE,
        "ticker_symbol" VARCHAR UNIQUE,
        "country" VARCHAR,
        "company_name" VARCHAR,
        "exchange" VARCHAR,
        "category_name" VARCHAR,
        PRIMARY KEY ("stock_id", "ticker_symbol")
    )
    """,
    """
    -- Existing Watchlist table
    CREATE TABLE IF NOT EXISTS "Watchlist" (
        "watchlist_id" UUID,
        "user_id" INT,
        "watchlist_name" VARCHAR,
        PRIMARY KEY ("watchlist_id"),
        CONSTRAINT "FK_Watchlist.user_id"
        FOREIGN KEY ("user_id")
            REFERENCES "Users"("user_id")
    )
    """,
    """
    -- Existing StocksInWatchlist table
    CREATE TABLE IF NOT EXISTS "StocksInWatchlist" (
        "id" SERIAL,
        "watchlist_id" UUID,
        "stock_id" UUID,
        "ticker_symbol" VARCHAR,
        PRIMARY KEY ("id"),
        CONSTRAINT "FK_StocksInWatchlist.watchlist_id"
        FOREIGN KEY ("watchlist_id")
            REFERENCES "Watchlist"("watchlist_id")
            ON DELETE CASCADE,
        CONSTRAINT "FK_StocksInWatchlist.stock_id"
        FOREIGN KEY ("stock_id")
            REFERENCES "CompanyInformation"("stock_id"),
        CONSTRAINT "FK_StocksInWatchlist.ticker_symbol"
        FOREIGN KEY ("ticker_symbol")
            REFERENCES "CompanyInformation"("ticker_symbol")
    )
    """,
    """
    -- Existing Stocks table
    CREATE TABLE IF NOT EXISTS "Stocks" (
        "transaction_id" UUID UNIQUE,
        "stock_id" UUID,
        "ticker_symbol" VARCHAR,
        "date" DATE,
        "low" FLOAT,
        "open" FLOAT,
        "high" FLOAT,
        "volume" BIGINT,
        "close" FLOAT,
        PRIMARY KEY ("transaction_id"),
        CONSTRAINT "UC_Stocks_transaction_date" UNIQUE ("transaction_id", "date"),
        CONSTRAINT "FK_Stocks.stock_id"
        FOREIGN KEY ("stock_id")
            REFERENCES "CompanyInformation"("stock_id"),
        CONSTRAINT "FK_Stocks.ticker_symbol"
        FOREIGN KEY ("ticker_symbol")
            REFERENCES "CompanyInformation"("ticker_symbol")
    )
    """,
    """
    -- New MovingAverages table with constraints
    CREATE TABLE IF NOT EXISTS "MovingAverages" (
        "cal_id" UUID UNIQUE,
        "transaction_id" UUID,
        "stock_id" UUID,
        "ticker_symbol" VARCHAR,
        "date" DATE,
        "5_days_sma" FLOAT,
        "20_days_sma" FLOAT,
        "50_days_sma" FLOAT,
        "200_days_sma" FLOAT,
        "5_days_ema" FLOAT,
        "20_days_ema" FLOAT,
        "50_days_ema" FLOAT,
        "200_days_ema" FLOAT,
        PRIMARY KEY ("cal_id"),
        FOREIGN KEY ("transaction_id")
            REFERENCES "Stocks"("transaction_id") ON DELETE CASCADE,
        FOREIGN KEY ("stock_id", "ticker_symbol")
            REFERENCES "CompanyInformation"("stock_id", "ticker_symbol")
    )
    """,
    """
    -- New BoillingerBands table with constraints
    CREATE TABLE IF NOT EXISTS "BoillingerBands" (
        "cal_id" UUID UNIQUE,
        "transaction_id" UUID,
        "stock_id" UUID,
        "ticker_symbol" VARCHAR,
        "date" DATE,
        "5_upper_band" FLOAT,
        "20_upper_band" FLOAT,
        "50_upper_band" FLOAT,
        "200_upper_band" FLOAT,
        "5_lower_band" FLOAT,
        "20_lower_band" FLOAT,
        "50_lower_band" FLOAT,
        "200_lower_band" FLOAT,
        PRIMARY KEY ("cal_id"),
        FOREIGN KEY ("transaction_id")
            REFERENCES "Stocks"("transaction_id") ON DELETE CASCADE,
        FOREIGN KEY ("stock_id", "ticker_symbol")
            REFERENCES "CompanyInformation"("stock_id", "ticker_symbol")
    )
    """,
    """
    -- New RelativeIndexes table with constraints
    CREATE TABLE IF NOT EXISTS "RelativeIndexes" (
        "cal_id" UUID UNIQUE,
        "transaction_id" UUID,
        "stock_id" UUID,
        "ticker_symbol" VARCHAR,
        "date" DATE,
        "14_days_rsi" FLOAT,
        "20_days_rsi" FLOAT,
        "50_days_rsi" FLOAT,
        "200_days_rsi" FLOAT,
        PRIMARY KEY ("cal_id"),
        FOREIGN KEY ("transaction_id")
            REFERENCES "Stocks"("transaction_id") ON DELETE CASCADE,
        FOREIGN KEY ("stock_id", "ticker_symbol")
            REFERENCES "CompanyInformation"("stock_id", "ticker_symbol")
    )
    """,
    ## Index for Stocks table
    """
    CREATE INDEX IF NOT EXISTS "idx_stocks_date" ON "Stocks"("date")
    """,
    ## Indexes for MovingAverages table
    """
    CREATE INDEX IF NOT EXISTS "idx_ma_stock_id_ticker_symbol_date" ON "MovingAverages"("stock_id", "ticker_symbol", "date")
    """,
    """
    CREATE INDEX IF NOT EXISTS "idx_ma_date" ON "MovingAverages"("date")
    """,
    ## Indexes for BoillingerBands table
    """
    CREATE INDEX IF NOT EXISTS "idx_bb_stock_id_ticker_symbol_date" ON "BoillingerBands"("stock_id", "ticker_symbol", "date")
    """,
    """
    CREATE INDEX IF NOT EXISTS "idx_bb_date" ON "BoillingerBands"("date")
    """,
    ## Indexes for RelativeIndexes table
    """
    CREATE INDEX IF NOT EXISTS "idx_ri_stock_id_ticker_symbol_date" ON "RelativeIndexes"("stock_id", "ticker_symbol", "date")
    """,
    """
    CREATE INDEX IF NOT EXISTS "idx_ri_date" ON "RelativeIndexes"("date")
    """,
]

class SqlDatabaseOperator:
    def __init__(self, db_name ,db_params=None):
        self.sql_db_name = 'postgres'
        self.db_name = db_name
        self.db_params = db_params if db_params else default_db_params
        self.connection = None
    
    # Function to create a database connection with error handling
    def create_connection(self, db_name=None):
        status = False
        # if db_name use the default sql database which is postgres
        if not db_name:
            db_name = self.get_sql_db_name()

        try:
            self.connection = psycopg2.connect(**self.db_params, database=db_name)
            if not self.connection:
                print(f'Error: Cannot create {db_name} connection')
            else:
                status = True
                print(f'>>> {db_name} connection is ready')
            return status
        except psycopg2.OperationalError as e:
            print(f"Error connecting to the database: {e}")
            return status
        
    def get_db_name(self):
        return self.db_name
    
    def set_db_name(self, new_db_name):
        try:
            self.db_name = new_db_name
            return True
        except Exception as e:
            print(f'Error Cannot change name for the new database: {e}')
            return False

    def get_sql_db_name(self):
        return self.sql_db_name

    def get_connection(self, db_name=None):
        if not self.connection:
            # if db_name use the default sql database which is postgres
            if not db_name:
                db_name = self.get_sql_db_name()
            status = self.create_connection(db_name)
        else:
            status = True

        return status, self.connection
    
    def close_connection(self, db_connection=None):
        try:
            if not db_connection:
                db_connection = self.connection

            if db_connection:
                dbname_start = db_connection.dsn.find('dbname=') + len('dbname=')
                dbname_end = db_connection.dsn.find(' ', dbname_start)
                dbname = db_connection.dsn[dbname_start:dbname_end]
                db_connection.close()
                print(f'>>> Connection to database {dbname} is closed')
            else:
                print('>>> Connection is not existed')

            return True
        except Exception as e:
            print(f"Error closing database connection: {e}")
            return False

    def execute_sql(self, sql_statements):
        status, connection = self.get_connection()
        cursor = None
        if status and connection:
            try:
                # Set autocommit mode to True
                connection.autocommit = True

                with connection.cursor() as cursor:
                    for sql_statement in sql_statements:
                        cursor.execute(sql_statement)

                # Reset autocommit mode back to False (optional)
                connection.autocommit = False

                print("SQL statements executed successfully.")
            except Exception as e:
                connection.rollback()
                print(f"Error: Unable to execute SQL statements. {e}")
            finally:
                # Close the cursor explicitly (optional)
                if cursor:
                    cursor.close()

    def create_db(self):
        status, postgres_connection = self.get_connection()
        
        # before creating need to make sure db is exist
        if status and postgres_connection:
            try:
                # Set autocommit mode to True
                postgres_connection.autocommit = True
                cur = postgres_connection.cursor()

                print(f"Check if {self.get_db_name()} database exists....")
                # Check if the database already exists
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (self.get_db_name(),))
                if not cur.fetchone():
                    # Create the new database
                    cur.execute(f"CREATE DATABASE {self.get_db_name()};")
                    print(f">>> Database '{self.get_db_name()}' created.")

                else:
                    print(f">>> Database '{self.get_db_name()}' already exists.")
                print(f"========================================================")

                # Set autocommit mode back to False (optional)
                postgres_connection.autocommit = False

                # close postgres connection
                self.close_connection(postgres_connection)

                print(f"Get new connection to access {self.get_db_name()} database...")
                # create new connection for stock market database
                status = self.create_connection(self.get_db_name())
            except Exception as e:
                print(f"Error: Unable to create {self.get_db_name()}. {e}")

    
    def create_tables_schemas(self, tables_sql_statements):
        status, connection = self.get_connection()
    
        if status and connection:
            try:
                with connection.cursor() as cur:
                    # Execute SQL statements to create tables
                    for sql_statement in tables_sql_statements:
                        cur.execute(sql_statement)

                    print(">>> Tables and schemas created successfully.")
                connection.commit()
            except Exception as e:
                connection.rollback()
                print(f"Exception: Unable to create tables and shemas. {e}")
        else:
            print(f"Error: Unable to create tables and shemas.")
    
    def create_companies_info_from_csv(self):
        sheet_name = "Stock"
        # XLSX file path
        xlsx_file_path = 'Assets/Yahoo Ticker Symbols - September 2017.xlsx' 
        workbook = None
        cursor = None
        conn = None

        try:
            
            # Load the workbook
            workbook = openpyxl.load_workbook(xlsx_file_path, read_only=True)
            sheet = workbook[sheet_name]
            counts = 0

            # Create a connection to the database
            status, conn = self.get_connection()
            if status and conn:
                with conn.cursor() as cursor:
                    print(f'Starting to insert company info.......')
                    # Iterate through rows in the sheet and insert data into the table
                    for row in sheet.iter_rows(values_only=True):
                        # Extract data from the row
                        ticker, name, exchange, category_name, country = row[:5]

                        # Generate a UUID for the "stock_id" column
                        stock_id = str(uuid.uuid4())  # Cast UUID to text

                        # Set default values for missing fields
                        if not ticker:
                            ticker = "UNKNOWN"
                        if not name:
                            name = "UNKNOWN"
                        if not exchange:
                            exchange = "UNKNOWN"
                        if not category_name:
                            category_name = "UNKNOWN"
                        if not country:
                            country = "UNKNOWN"

                        # Define the SQL INSERT statement
                        sql = """
                        INSERT INTO "CompanyInformation" ("stock_id", "ticker_symbol", "company_name", "exchange", "category_name", "country")
                        VALUES (%s, %s, %s, %s, %s, %s);
                        """

                        # Execute the SQL statement
                        cursor.execute(sql, (stock_id, ticker, name, exchange, category_name, country))

                        counts += 1

                # Commit the changes
                conn.commit()
                print(f'>>> Total {counts} recors are inserted to CompanyInformation table.')
        except Exception as e:
            if conn:
                conn.rollback()
            print("Error:", e)
        finally:
            # Close the workbook and database connection
            if workbook:
                    workbook.close()
            
    def create_stock_market(self):
        print(f"Drop {self.get_db_name()} if exist to re-create.....")
        self.execute_sql(['DROP DATABASE IF EXISTS stockmarket'])
        print("===========================================================")

        self.create_db()
        
        self.create_tables_schemas(create_tables_sql)
        
        self.create_companies_info_from_csv()
            
def test_class_func():
    stock_market = SqlDatabaseOperator("stockmarket")
    
    stock_market.create_stock_market()

test_class_func()





# # Establish a connection to the default 'postgres' database to create a new database
# try:
#     conn = psycopg2.connect(**db_params, database='postgres')
#     if conn:
#         # Disable autocommit to avoid starting a transaction
#         conn.autocommit = False


# except psycopg2.OperationalError as e:
#     print(f"Error: {e}")
#     exit()


# # Create a cursor
# cur = conn.cursor()

# # Check if the database already exists
# cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
# if not cur.fetchone():
#     try:
#         # Create the new database
#         cur.execute(f"CREATE DATABASE {db_name};")
#         print(f"Database '{db_name}' created.")
#     except psycopg2.DatabaseError as e:
#         print(f"Error creating the database: {e}")
# else:
#     print(f"Database '{db_name}' already exists.")

# # Re-enable autocommit
# conn.autocommit = False

# # Close the connection to the default 'postgres' database
# cur.close()
# conn.close()

# # Connect to the newly created database
# try:
#     conn = psycopg2.connect(**db_params, database=db_name)
# except psycopg2.OperationalError as e:
#     print(f"Error: {e}")
#     exit()

# # Create a cursor
# cur = conn.cursor()

# # Execute SQL statements to create tables
# for sql_statement in create_tables_sql:
#     cur.execute(sql_statement)

# print("Tables and schemas created successfully.")

# # Commit changes and close the connection
# conn.commit()
# cur.close()
# conn.close()
