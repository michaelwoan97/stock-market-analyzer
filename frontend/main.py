import numpy as np
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go

# Function to make an API request
def api_request(url, data, headers):
    try:
        response = requests.post(url, json=data, headers=headers)
        return response.json(), response.status_code
    except Exception as e:
        return {"error": str(e)}, 500

# Streamlit App
st.title("Companies Data App")

# Sidebar with input fields
st.sidebar.header("Input Parameters")

# Country input
country = "USA"

# Color dictionary
colors = {
    '200_sma': 'blue',
    '50_sma': 'green',
    '200_ema': 'orange',
    '50_ema': 'purple',
    'close': 'red'  
}


# Ticker symbols input with default value
ticker_symbols = st.sidebar.text_input("Enter Ticker Symbols (comma-separated)", value="AAPL")

# Default date range is set to 10 years from now
default_end_date = pd.to_datetime('today')  # End date is today
default_start_date = default_end_date - pd.DateOffset(years=10)  # Start date is 10 years ago

# Date range picker for chart with default value
date_range = st.sidebar.date_input("Select Date Range", value=(default_start_date, default_end_date))

# Checkbox for fetching technical analysis data
get_technical_analysis_data = st.sidebar.checkbox("Get Technical Analysis Data")

# Button to trigger API request
if st.sidebar.button("Get Companies Data"):
    headers = {"Content-Type": "application/json"}

    # API request for companies data
    api_url = "http://localhost:5000/get_stock_data"
    data = {
        "start_date": date_range[0].strftime('%Y-%m-%d'),
        "end_date": date_range[1].strftime('%Y-%m-%d'),
        "technical": False,
        "data": [
            {
                "country": country,
                "ticker_symbol": [symbol.strip() for symbol in ticker_symbols.split(",")][0],
            }
        ]
    }
    
    if not get_technical_analysis_data:
        
        # Make the API request
        response_data, status_code = api_request(api_url, data, headers)

        # Check if the request was successful (status code 200)
        if status_code == 200:
            results = response_data.get("results", [])
            stock_data = results[0]

            expander = st.expander(f"{stock_data['ticker_symbol']}")
            expander.write(f"Stock id: {stock_data['stock_id']}")
            expander.write(f"Country: {stock_data['country']}")
            
            # Iterate through the results
            for result in results:

                # Fetch stock prices data
                dates = [pd.to_datetime(entry['date']).strftime('%Y-%m-%d') for entry in result['data']]
                close_prices = [entry['close'] for entry in result['data']]
                
                # Create DataFrame with 'stock_id', 'ticker_symbol', 'Date', and 'Close Prices' columns
                stock_prices_df = pd.DataFrame({
                    'date': pd.to_datetime(dates),
                    'close_prices': close_prices
                })
                # Create a Plotly figure
                fig = go.Figure()

                # Add a scatter plot for close prices
                fig.add_trace(go.Scatter(x=stock_prices_df['date'], y=stock_prices_df['close_prices'], mode='lines', name='Close Prices'))

                # Update layout with title and axis labels
                fig.update_layout(title=f"Stock Prices",
                                xaxis_title='Date', yaxis_title='Close Prices')

                # Use plotly_chart to display the chart in the Streamlit app
                expander.plotly_chart(fig, use_container_width=True, height=400)
        else:
            print(response_data)
    # Fetch technical analysis data if the checkbox is selected
    else:
        # API request for technical analysis
        data['technical'] = True

        # Make the API request
        technical_analysis_response, status_code = api_request(api_url, data, headers)

        # Check if the technical analysis response is not empty
        if status_code == 200:
            results = technical_analysis_response.get("results", [])
            stock_data = results[0]

            expander = st.expander(f"{stock_data['ticker_symbol']}")
            expander.write(f"Stock id: {stock_data['stock_id']}")
            expander.write(f"Country: {stock_data['country']}")

            if stock_data['technical_view']:
                # Extract relevant columns from each entry in "technical_view"
                data_list = []
                for entry in stock_data['technical_view']:
                    data_list.append({
                        "date": pd.to_datetime(entry["date"]),
                        "close": entry["close"],
                        "50_sma": entry["ma_50_days_sma"],
                        "200_sma": entry["ma_200_days_sma"],
                        "50_ema": entry["ma_50_days_ema"],
                        "200_ema": entry["ma_200_days_ema"]
                    })
                
                # Create DataFrame from the list of dictionaries
                df = pd.DataFrame(data_list)

                # Display the resulting DataFrame
                print(df)
                # Create a Plotly figure
                fig = go.Figure()

                # Add traces with specified colors
                for column in df.columns[1:]:  # Exclude 'date' column
                    if column in colors:
                        fig.add_trace(go.Scatter(x=df['date'], y=df[column], mode='lines', name=column, line=dict(color=colors[column])))

                # Update layout with title and axis labels
                fig.update_layout(title="Stock Prices and Moving Averages",
                                xaxis_title='Date', yaxis_title='Price')

                # Display the chart using plotly_chart
                expander.plotly_chart(fig, use_container_width=True)
            else:
                print("Dont have data in view")
        else: 
            print(technical_analysis_response)

