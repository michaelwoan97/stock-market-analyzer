from datetime import datetime
import json
import uuid
import pandas as pd
import yfinance as yf
import urllib.request

# Define a class to represent price movement data
class PriceMovement:
    def __init__(self, transaction_id, date, low, open_price, high, volume, close):
        self.transaction_id = transaction_id
        self.date = date
        self.low = low
        self.open_price = open_price
        self.high = high
        self.volume = volume
        self.close = close

    def __str__(self):
        return f"PriceMovement(transaction_id={self.transaction_id}, date={self.date}, low={self.low}, " \
               f"open={self.open_price}, high={self.high}, volume={self.volume}, close={self.close})"

    def to_dict(self):
        return {
            'transaction_id': str(self.transaction_id),
            'date': self.date,
            'low': self.low,
            'open': self.open_price,
            'high': self.high,
            'volume': self.volume,
            'close': self.close
        }
    
def fetch_stock_data_from_url(query_url):
    try:
        # Open a connection to the URL
        with urllib.request.urlopen(query_url) as url:
            # Parse the JSON data from the URL
            parsed = json.loads(url.read().decode())

            # Extract the 'timestamp' data and convert it to the 'date' format
            date_list = [datetime.utcfromtimestamp(int(i)).strftime('%Y-%m-%d') for i in parsed['chart']['result'][0]['timestamp']]

            # Extract other data points (low, open, volume, high, close)
            low = parsed['chart']['result'][0]['indicators']['quote'][0]['low']
            open_price = parsed['chart']['result'][0]['indicators']['quote'][0]['open']
            volume = parsed['chart']['result'][0]['indicators']['quote'][0]['volume']
            high = parsed['chart']['result'][0]['indicators']['quote'][0]['high']
            close = parsed['chart']['result'][0]['indicators']['quote'][0]['close']

            # Create an array to store PriceMovement instances
            price_movements = []

            # Combine the extracted data into PriceMovement instances
            for i in range(len(date_list)):
                # Generate a UUID for the 'transaction_id' column
                transaction_id = uuid.uuid4()

                price_movement = PriceMovement(
                    transaction_id=transaction_id,
                    date=date_list[i],
                    low=low[i],
                    open_price=open_price[i],
                    volume=volume[i],
                    high=high[i],
                    close=close[i]
                )
                price_movements.append(price_movement)

            # Convert the array of PriceMovement instances to a list and return it
            return price_movements

    except Exception as e:
        print(f"Error fetching data from Yahoo Finance: {e}")
        return None
    

