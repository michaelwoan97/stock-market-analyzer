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

# Function to display technical analysis chart
def display_technical_analysis(stock_prices_df, technical_analysis_results, result, date_range):
    # Filter data within the specified date range
    stock_prices_df = stock_prices_df[
        (stock_prices_df['date'] >= np.datetime64(date_range[0])) & (stock_prices_df['date'] <= np.datetime64(date_range[1]))
    ]

    # Create an empty DataFrame to store moving averages data
    result_df = pd.DataFrame()

    # Iterate through technical analysis results
    for analysis_result in technical_analysis_results:
        for analysis_type in analysis_result['technical_analysis']:
            for entry in analysis_type['data']:
                if analysis_type['type'] == 'moving_averages':
                    # Exclude 'cal_id' and 'stock_id' from selected columns
                    columns_to_select = [key for key in entry.keys() if key not in ['cal_id', 'stock_id']]

                    # Use a dictionary comprehension to create a DataFrame with key-value pairs
                    entry_df = pd.DataFrame({
                        key: [entry[key]] if key != 'date' else [pd.to_datetime(entry[key])]
                        for key in columns_to_select
                    })

                    # Concatenate the entry_df to the result_df
                    result_df = pd.concat([result_df, entry_df], ignore_index=True)

    # Merge stock prices and moving averages data
    chart_df = pd.merge(stock_prices_df, result_df, on='date', how='inner')

    # Create a Plotly figure
    fig = go.Figure()

    # Add stock price line with enhanced attributes
    fig.add_trace(go.Scatter(x=chart_df['date'], y=chart_df['close_prices'], mode='lines', name='Close Prices',
                             line=dict(color='red', width=2, dash='solid')))

    # Define colors for each trace
    colors = {
        '200_days_sma': 'blue',
        '50_days_sma': 'green',
        '200_days_ema': 'orange',
        '50_days_ema': 'purple'
    }

    # Add traces with specified colors
    for column in result_df.columns:
        if column in colors:
            fig.add_trace(go.Scatter(x=chart_df['date'], y=chart_df[column], mode='lines', name=column, line=dict(color=colors[column])))

    # Update layout with title and axis labels
    fig.update_layout(title=f"{result['ticker_symbol']} - Stock Prices and Moving Averages",
                      xaxis_title='Date', yaxis_title='Price')

    return fig  # Return the combined chart

# Streamlit App
st.title("Companies Data App")

# Sidebar with input fields
st.sidebar.header("Input Parameters")

# Country input
country = "USA"

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
    # API request for companies data
    api_url = "http://localhost:5000/get_companies_data"
    headers = {"Content-Type": "application/json"}
    data = {"country": country, "ticker_symbols": [symbol.strip() for symbol in ticker_symbols.split(",")]}

    # Make the API request
    response_data, status_code = api_request(api_url, data, headers)

    # Check if the request was successful (status code 200)
    if status_code == 200:
        results = response_data.get("results", [])

        # Iterate through the results
        for result in results:
            expander = st.expander(f"{result['ticker_symbol']}")
            expander.write(f"Stock id: {result['stock_id']}")
            expander.write(f"Country: {result['country']}")

            # Fetch stock prices data
            dates = [pd.to_datetime(entry['date']).strftime('%Y-%m-%d') for entry in result['data']]
            close_prices = [entry['close'] for entry in result['data']]
            
            # Create DataFrame with 'stock_id', 'ticker_symbol', 'Date', and 'Close Prices' columns
            stock_prices_df = pd.DataFrame({
                'stock_id': [result['stock_id']] * len(dates),
                'ticker_symbol': [result['ticker_symbol']] * len(dates),
                'date': pd.to_datetime(dates),
                'close_prices': close_prices
            })

            # Fetch technical analysis data if the checkbox is selected
            if get_technical_analysis_data:
                # API request for technical analysis
                technical_analysis_url = "http://localhost:5000/get_technical_analysis"
                technical_analysis_data = {"data": [{
                    "stock_id": result["stock_id"],
                    "ticker_symbol": result["ticker_symbol"]
                }]}

                # Make the API request
                technical_analysis_response, _ = api_request(technical_analysis_url, technical_analysis_data, headers)

                # Check if the technical analysis response is not empty
                if technical_analysis_response:
                    technical_analysis_results = technical_analysis_response.get("results", [])
                    combined_chart = display_technical_analysis(stock_prices_df, technical_analysis_results, result, date_range)
                    expander.plotly_chart(combined_chart, use_container_width=True, height=400)

            else:
                # Display stock prices chart directly within the specified date range
                stock_prices_within_range = stock_prices_df[
                    (stock_prices_df['date'] >= np.datetime64(date_range[0])) & (stock_prices_df['date'] <= np.datetime64(date_range[1]))
                ]
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=stock_prices_within_range['date'], y=stock_prices_within_range['close_prices'], mode='lines', name='Close Prices'))
                fig.update_layout(title=f"{result['ticker_symbol']} - Stock Prices",
                                  xaxis_title='Date', yaxis_title='Price')
                expander.plotly_chart(fig, use_container_width=True, height=400)

            expander.write("-" * 50)
