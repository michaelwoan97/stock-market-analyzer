import psycopg2
import os 
from dotenv import load_dotenv

# load the environment variables from the .env file
load_dotenv()

# Connection parameters for the PostgreSQL server
db_params = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),  # Change to your database server host
    'port': os.environ.get('DB_PORT'),  # Change to your database server port
}


# Database name to be created
db_name = 'stockmarket'

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
        "date" DATE UNIQUE,
        "low" FLOAT,
        "open" FLOAT,
        "high" FLOAT,
        "volume" BIGINT,
        "close" FLOAT,
        PRIMARY KEY ("transaction_id","date"),
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
        "cal_id" UUID,
        "transaction_id" UUID,
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
            REFERENCES "Stocks"("transaction_id"),
        FOREIGN KEY ("date")
            REFERENCES "Stocks"("date")
    )
    """,
    """
    -- New BoillingerBands table with constraints
    CREATE TABLE IF NOT EXISTS "BoillingerBands" (
        "cal_id" UUID,
        "transaction_id" UUID,
        "date" DATE,
        "5_upper_bands" FLOAT,
        "20_upper_bands" FLOAT,
        "50_upper_bands" FLOAT,
        "200_upper_bands" FLOAT,
        "5_lower_bands" FLOAT,
        "20_lower_bands" FLOAT,
        "50_lower_bands" FLOAT,
        "200_lower_bands" FLOAT,
        PRIMARY KEY ("cal_id"),
        FOREIGN KEY ("transaction_id")
            REFERENCES "Stocks"("transaction_id"),
        FOREIGN KEY ("date")
            REFERENCES "Stocks"("date")
    )
    """,
    """
    -- New RelativeIndexes table with constraints
    CREATE TABLE IF NOT EXISTS "RelativeIndexes" (
        "cal_id" UUID,
        "transaction_id" UUID,
        "date" DATE,
        "14_days_rsi" FLOAT,
        "20_days_rsi" FLOAT,
        "50_days_rsi" FLOAT,
        "200_days_rsi" FLOAT,
        PRIMARY KEY ("cal_id"),
        FOREIGN KEY ("transaction_id")
            REFERENCES "Stocks"("transaction_id"),
        FOREIGN KEY ("date")
            REFERENCES "Stocks"("date")
    )
    """
]


# Establish a connection to the default 'postgres' database to create a new database
try:
    conn = psycopg2.connect(**db_params, database='postgres')
except psycopg2.OperationalError as e:
    print(f"Error: {e}")
    exit()

# Create a cursor
cur = conn.cursor()

# Establish a connection to the default 'postgres' database to create a new database
try:
    conn = psycopg2.connect(**db_params, database='postgres')
except psycopg2.OperationalError as e:
    print(f"Error: {e}")
    exit()

# Disable autocommit to avoid starting a transaction
conn.autocommit = True

# Create a cursor
cur = conn.cursor()

# Check if the database already exists
cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
if not cur.fetchone():
    try:
        # Create the new database
        cur.execute(f"CREATE DATABASE {db_name};")
        print(f"Database '{db_name}' created.")
    except psycopg2.DatabaseError as e:
        print(f"Error creating the database: {e}")
else:
    print(f"Database '{db_name}' already exists.")

# Re-enable autocommit
conn.autocommit = False

# Close the connection to the default 'postgres' database
cur.close()
conn.close()

# Connect to the newly created database
try:
    conn = psycopg2.connect(**db_params, database=db_name)
except psycopg2.OperationalError as e:
    print(f"Error: {e}")
    exit()

# Create a cursor
cur = conn.cursor()

# Execute SQL statements to create tables
for sql_statement in create_tables_sql:
    cur.execute(sql_statement)

print("Tables and schemas created successfully.")

# Commit changes and close the connection
conn.commit()
cur.close()
conn.close()
