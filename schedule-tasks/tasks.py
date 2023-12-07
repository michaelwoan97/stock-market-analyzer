import sys
sys.path.append('../')


from stock_market_operator import StockMarketOperator


from celery import Celery

from celery.schedules import crontab
import concurrent.futures


broker_url = 'pyamqp://guest@localhost//'
result_backend = 'rpc://'


app = Celery('my_stock_project', broker=broker_url, backend=result_backend)

# Define your timezone and result serialization format
app.conf.timezone = 'UTC'
app.conf.result_serializer = 'json'

@app.task
def task_update_stock_data_daily():
    """
    Task to update stock market data for existing stocks.

    This function connects to the database, retrieves information about existing stocks,
    and updates their data using concurrent threads.

    It prints a message when the update is completed or an error occurs.

    Note: Make sure to have the necessary functions (create_connection_pool, get_stocks_ticker_id_exist,
    and update_missing_stock_data) defined and imported in your code.
    """
    minconn = 5
    maxconn = 10
    stock_market_operator = StockMarketOperator(minconn,maxconn)
    
    try:
    
        # Connect to the database
        stock_market_operator.create_connection_pool()

        stocks_info = stock_market_operator.get_stocks_ticker_id_exist()

        # check whether stocks exist
        if len(stocks_info):
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(lambda stock: stock_market_operator.update_missing_stock_data(stock), stocks_info)
            
            print("Finished Updating Stock Market Data!!!!")
            
    except Exception as e:
        print(f"Error: {e}")

    finally:
        if stock_market_operator:
            stock_market_operator.close_connection_pool()
        else:
            print(f"Somethings went wrong!!!")

