
import openpyxl
import psycopg2
import os 
import uuid
import sys
sys.path.append('./')

from dotenv import load_dotenv
from psycopg2.extras import execute_values


# load the environment variables from the .env file
load_dotenv()

# XLSX file path
xlsx_file_path = 'Assets/Yahoo Ticker Symbols - September 2017.xlsx'  # Replace with the path to your XLSX file

# Connection parameters for the PostgreSQL server
db_params = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),  
    'port': os.environ.get('DB_PORT'),  
    'database': 'stockmarket'
}

# Specify the sheet name
sheet_name = "Stock"

try:
    # Load the workbook
    workbook = openpyxl.load_workbook(xlsx_file_path, read_only=True)
    sheet = workbook[sheet_name]

    # Create a connection to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

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

         # Print feedback for each row
        print(f"Processing row: Ticker={ticker}, Name={name}, Exchange={exchange}, Category Name={category_name}, Country={country}")

        # Define the SQL INSERT statement
        sql = """
        INSERT INTO "CompanyInformation" ("stock_id", "ticker_symbol", "company_name", "exchange", "category_name", "country")
        VALUES (%s, %s, %s, %s, %s, %s);
        """

        # Execute the SQL statement
        cursor.execute(sql, (stock_id, ticker, name, exchange, category_name, country))

    # Commit the changes
    conn.commit()

except Exception as e:
    print("Error:", e)
finally:
    # Close the workbook and database connection
    if 'workbook' in locals():
        workbook.close()
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()


