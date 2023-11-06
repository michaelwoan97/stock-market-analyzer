import argparse
import concurrent.futures
import psycopg2

from database import process_stock

def main():
    # Define the command-line arguments
    parser = argparse.ArgumentParser(description='Check for the existence of companies based on ticker symbols and country.')
    parser.add_argument('country', help='Country for the companies (e.g., "USA").')
    parser.add_argument('ticker_symbols', nargs='+', help='List of ticker symbols (e.g., "AAPL GOOGL MSFT")')

    args = parser.parse_args()
    ticker_symbols = args.ticker_symbols
    country = args.country

    # Initialize an empty list to store query URLs and stock data objects
    query_urls = []
    stock_data_objects = []

    # Ensure ticker_symbols are provided
    if not ticker_symbols:
        print("Error: You must provide at least one ticker symbol.")
        return

    try:
        # Create an empty list to collect the results
        results = []

        # Create a thread pool
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # List of tasks to submit
            tasks = [(ticker, country, query_urls, stock_data_objects) for ticker in ticker_symbols]

            # Submit tasks for concurrent execution
            futures = [executor.submit(process_stock, *task) for task in tasks]

            # Wait for all tasks to complete
            for future in concurrent.futures.as_completed(futures):
                # Retrieve the result of each thread
                result = future.result()
                
                # Append the result to the list
                results.append(result)
        
        # Print stock data for the first three data points
        for stock_data in stock_data_objects:
            print(f"Stock data for {stock_data.ticker_symbol} in {stock_data.country} with stock_id {stock_data.stock_id}:")
            for data_point in stock_data.data[:3]:  # Loop through the first three data points
                print(data_point)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")

if __name__ == '__main__':
    main()
