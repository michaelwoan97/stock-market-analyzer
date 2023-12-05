import sys
sys.path.append('../')

from celery import Celery
from database import async_get_stocks_ticker_id_exist, async_create_connection_pool
from celery.schedules import crontab

broker_url = 'pyamqp://guest@localhost//'
result_backend = 'rpc://'


app = Celery('my_stock_project', broker=broker_url, backend=result_backend)

# Define your timezone and result serialization format
app.conf.timezone = 'UTC'
app.conf.result_serializer = 'json'

@app.task
async def task_update_stock_data_daily():
    # create connection pool
    # get stock ids
    # create tasks with pool, and each stock in the array to update
    # then print the result
    
    # pool = await async_create_connection_pool()

    # try:
    #     stocks = await async_get_stocks_ticker_id_exist(pool)
        

    #     # # Create tasks to update stock data asynchronously
    #     # tasks = [async_update_stock_data_daily(stock) for stock in stocks]
    #     # results = await asyncio.gather(*tasks)

    #     # # Insert results into the database
    #     # async with async_create_connection() as connection:
    #     #     for data in results:
    #     #         await async_insert_stock_data_into_db(connection, data)

    # finally:
    #     # Close the async connection pool
    #     if pool:
    #         await pool.close()
