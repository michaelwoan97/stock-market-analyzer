from datetime import datetime
import json
import pandas as pd
import yfinance as yf
import urllib.request

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

            # Combine the extracted data into a DataFrame
            df = pd.DataFrame({'date': date_list, 'low': low, 'open': open_price, 'volume': volume, 'high': high, 'close': close})

            # Drop duplicates based on 'date'
            df = df.drop_duplicates(subset=['date'])
            
            # Convert the 'date' column to datetime objects with the correct format
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

            # Sort the DataFrame by the 'date' column in ascending order
            df = df.sort_values(by='date')

            # Format the 'date' column back to "YYYY-MM-DD"
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')

            # Convert the DataFrame to a list of dictionaries and return it
            return df.to_dict(orient='records')
    except Exception as e:
        print(f"Error fetching data from Yahoo Finance: {e}")
        return None
    

