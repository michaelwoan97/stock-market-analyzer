from datetime import timedelta
from celery import Celery
from celery.schedules import crontab


app = Celery('my_stock_project', broker='pyamqp://guest@localhost//', backend='rpc://')

# Configure the schedule for Celery Beat (scheduler)
app.conf.beat_schedule = {
    'task_update_stock_data_daily': {
        'task': 'tasks.task_update_stock_data_daily',  # Path to your Celery task
        'schedule': timedelta(seconds=20),  # Run every 20 seconds
        # 'schedule': crontab(minute=15, hour=16, day_of_week='1-5'),  # 4:15 PM, Mon-Fri
    },
}

# Define your timezone and result serialization format
app.conf.timezone = 'America/Toronto'
app.conf.result_serializer = 'json'
