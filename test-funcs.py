from database import add_stock_to_watchlist, create_watchlist, delete_watchlist, find_watchlist, find_user_by_id, get_stocks_data_combined_to_csv,get_watchlist, update_stock_data_daily, update_watchlist_info, update_watchlist_stocks_info,get_stocks_data_available

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

print("Processing stock data and save files!!!")
get_stocks_data_combined_to_csv()