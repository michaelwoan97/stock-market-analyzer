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

            # Extract the 'timestamp' data and convert it to the 'Date' format
            Date = []
            for i in parsed['chart']['result'][0]['timestamp']:
                Date.append(datetime.utcfromtimestamp(int(i)).strftime('%d-%m-%Y'))

            # Extract other data points (low, open, volume, high, close)
            Low = parsed['chart']['result'][0]['indicators']['quote'][0]['low']
            Open = parsed['chart']['result'][0]['indicators']['quote'][0]['open']
            Volume = parsed['chart']['result'][0]['indicators']['quote'][0]['volume']
            High = parsed['chart']['result'][0]['indicators']['quote'][0]['high']
            Close = parsed['chart']['result'][0]['indicators']['quote'][0]['close']

            # Combine the extracted data into a list of tuples
            data = list(zip(Date, Low, Open, Volume, High, Close))

            # Create a DataFrame from the list of tuples
            df = pd.DataFrame(data, columns=['date', 'low', 'open', 'volume', 'high', 'close'])

            # Convert the 'Date' column to datetime objects with the correct format
            df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')

            # Sort the DataFrame by the 'Date' column in ascending order
            df = df.sort_values(by='date')

            # Format the 'Date' column back to "YYYY-MM-DD"
            df['date'] = df['date'].dt.strftime('%Y-%m-%d')

            # Convert the DataFrame to a list of dictionaries and return it
            return df.to_dict(orient='records')
    except Exception as e:
        print(f"Error fetching data from Yahoo Finance: {e}")
        return None
    

