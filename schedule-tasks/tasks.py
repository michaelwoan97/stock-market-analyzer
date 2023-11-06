import sys
sys.path.append('../')

from celery import Celery
from database import get_stocks_ticker_id_exist, insert_stock_data_into_db, create_connection, update_stock_data_daily
from celery.schedules import crontab

broker_url = 'pyamqp://guest@localhost//'
result_backend = 'rpc://'


app = Celery('my_stock_project', broker=broker_url, backend=result_backend)

# Define your timezone and result serialization format
app.conf.timezone = 'UTC'
app.conf.result_serializer = 'json'

@app.task
def task_update_stock_data_daily():
    stocks = get_stocks_ticker_id_exist()
    print(stocks)
    results = update_stock_data_daily(stocks)
    for data in results:
        connection = create_connection()
        insert_stock_data_into_db(connection,data)
