from database import add_stock_to_watchlist, create_watchlist, delete_watchlist, fetch_moving_averages_data_from_db, fetch_technical_data, find_watchlist, find_user_by_id, get_stocks_data_combined_to_csv, get_transaction_ids_and_dates,get_watchlist, update_stock_data_daily, update_watchlist_info, update_watchlist_stocks_info,get_stocks_data_available
import concurrent.futures
from multiprocessing import Manager
# # test remove watchlist
# watchlist_id_to_delete = 'e95b2790-5aa7-4c34-97ed-24cab1d7e94f'
# delete_watchlist(watchlist_id_to_delete)

# # Test find_watchlist
# user_id = 6
# result = get_watchlist(user_id)

# watchlist_name = 'tech'
# found_watchlist = find_watchlist(user_id, watchlist_name)
# if found_watchlist:
#     print("Found watchlist:", found_watchlist)
# else:
#     print("Watchlist not found")

# # Test create_watchlist
# new_watchlist_name = 'tech'
# new_watchlist_id = create_watchlist(user_id, new_watchlist_name)
# print("New watchlist ID:", new_watchlist_id)

# # # Test add_stock_to_watchlist
# # watchlist_id = new_watchlist_id
# # stock_id = 'd01fc26c-b673-4cf0-a340-1d5e3d138601'
# # ticker_symbol = 'GOOG'
# # add_stock_to_watchlist(watchlist_id, stock_id, ticker_symbol)
# # print("Stock added to watchlist successfully")

# # # Test add_stock_to_watchlist
# watchlist_id = new_watchlist_id
# stock_id = 'ff015d99-f026-40d8-9850-db352dbbabd3'
# ticker_symbol = 'MSFT'
# add_stock_to_watchlist(watchlist_id, stock_id, ticker_symbol)
# print("Stock added to watchlist successfully")

# # update watchlist
# watchlist_id = "420ebf1f-bf13-462e-9228-d5e12459fb0a"  # Replace with the actual watchlist_id
# new_watchlist_name = "tech"
# update_watchlist(watchlist_id, new_watchlist_name)

# # update Watchlist
# Example usage
# watchlist_id = "420ebf1f-bf13-462e-9228-d5e12459fb0a"  # Replace with the actual watchlist_id
# updated_stocks = [
#     {"stock_id": "0000d08b-5713-4caf-abd4-19093b636f15", "ticker_symbol": "FENR.L"},
#     {"stock_id": "0001a7af-b739-4525-9c16-f73d837f3d4a", "ticker_symbol": "CGV.SG"},
# ]
# update_watchlist_stocks(watchlist_id, updated_stocks)

# Test when data is available
# stock_ticker_ids = [
#     {'stock_id': 1, 'ticker_symbol': 'AAPL'},
#     {'stock_id': 2, 'ticker_symbol': 'GOOG'},
# ]
# update_stock_data_daily(stock_ticker_ids)


# # Step 1: Assuming 'GOOG' is a valid ticker symbol and '2d8ed716-545d-400c-ae24-e56f31fd709f' is a valid stock_id
# stock_id = '2d8ed716-545d-400c-ae24-e56f31fd709f'
# ticker_symbol = 'GOOG'

# # Step 2: Get a list of tuples containing transaction_id and date
# transaction_info_list = get_transaction_ids_and_dates(stock_id, ticker_symbol)
# print(len(transaction_info_list))

# # Step 3: Fetch Moving Averages data using a thread pool
# moving_averages_data = []

# def thread_worker(tuple_info):
#     result = fetch_moving_averages_data_from_db(tuple_info)
#     moving_averages_data.append(result)

# # Use a ThreadPoolExecutor for concurrent execution
# with concurrent.futures.ThreadPoolExecutor() as executor:
#     executor.map(thread_worker, transaction_info_list)

# print(len(moving_averages_data))
# Print the result
# for record in moving_averages_data:
#     print(record)


# # Step 1: Assuming 'GOOG' is a valid ticker symbol and '2d8ed716-545d-400c-ae24-e56f31fd709f' is a valid stock_id
# stock_id = '2d8ed716-545d-400c-ae24-e56f31fd709f'
# ticker_symbol = 'GOOG'

# # Step 2: Get a list of tuples containing transaction_id and date
# transaction_info_list = get_transaction_ids_and_dates(stock_id, ticker_symbol)
# print(len(transaction_info_list))

# # Step 3: Fetch Moving Averages data using a process pool with shared list
# manager = Manager()
# moving_averages_data = manager.list()

# def process_worker(tuple_info):
#     result = fetch_moving_averages_data_from_db(tuple_info)
#     moving_averages_data.append(result)

# # Use a ProcessPoolExecutor for concurrent execution
# with concurrent.futures.ProcessPoolExecutor() as executor:
#     executor.map(process_worker, transaction_info_list)

# # Convert the shared list to a regular list for further processing
# moving_averages_data = list(moving_averages_data)

# print(len(moving_averages_data))



# Step 1: Assuming 'GOOG' is a valid ticker symbol and '2d8ed716-545d-400c-ae24-e56f31fd709f' is a valid stock_id
# stock_id = '2d8ed716-545d-400c-ae24-e56f31fd709f'
# ticker_symbol = 'GOOG'

# # Step 2: Get a list of tuples containing transaction_id and date
# transaction_info_list = fetch_moving_averages_data_from_db(stock_id, ticker_symbol)
# print(len(transaction_info_list))

# # Print the result
# for record in transaction_info_list:
#     print(record)

# Replace these with actual stock_id and ticker_symbol values from your database
test_stock_id = "d30dc554-d8e3-4b65-a516-19129f46e798"
test_ticker_symbol = "AAPL"
test_start_date = "2020-11-22"
test_end_date = "2023-11-16"
# Call the function
result = fetch_technical_data(test_stock_id, test_ticker_symbol, test_start_date, test_end_date)

# Print the result
# print(result)
