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
st.markdown(
    """
    <style>
        .header {
            background-color: #3498db;
            padding: 1rem;
            color: #fff;
            font-size: 2rem;
            text-align: center;
        }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("<p class='header'>Stock Insight Hub</p>", unsafe_allow_html=True)

# Disclaimer
st.markdown(
    """
    **Disclaimer:** This application provides insights into stock data based on technical analysis. 
    I am not a financial advisor, and the information presented here is for educational and informational 
    purposes only. Please consult with a qualified financial professional before making any investment decisions.
    """
)

# Sidebar with input fields
st.sidebar.header("S&P 500 only Edition")

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
ticker_symbols = st.sidebar.text_input("Enter Ticker Symbol", value="AAPL")

# Default date range is set to 10 years from now
default_end_date = pd.to_datetime('today')  # End date is today
default_start_date = default_end_date - pd.DateOffset(years=10)  # Start date is 10 years ago

# Date range picker for chart with default value
date_range = st.sidebar.date_input("Select Date Range", value=(default_start_date, default_end_date))

# Checkbox for fetching technical analysis data
get_technical_analysis_data = st.sidebar.checkbox("Get Technical Analysis Data")

# Function to fetch stock data from the API
def get_stock_data(api_url, data, headers):
    response_data, status_code = api_request(api_url, data, headers)

    if status_code == 200:
        return response_data.get("results", [])
    else:
        print(response_data)
        return None

# Function to draw stock prices chart
def draw_stock_prices_chart(df):
    # Create a Plotly figure
    fig = go.Figure()

    # Add a scatter plot for close prices with a red line
    fig.add_trace(go.Scatter(x=df['date'], y=df['close_prices'], mode='lines', name='Close Prices', line=dict(color='red')))

    # Update layout with title and axis labels
    fig.update_layout(title=f"Stock Prices",
                      xaxis_title='Date', yaxis_title='Close Prices')

    # Use plotly_chart to display the chart in the Streamlit app
    st.plotly_chart(fig, use_container_width=True, height=400)

# Function to draw technical analysis charts with annotations
def draw_technical_analysis_charts(df):
    # Create a Plotly figure for moving averages
    fig_ma = go.Figure()

    # Add traces for moving averages with specified colors
    for column in df.columns[1:8]:  # Exclude 'date' column
        if column in colors:
            fig_ma.add_trace(go.Scatter(x=df['date'], y=df[column], mode='lines', name=column, line=dict(color=colors[column])))

    # Update layout for moving averages chart
    fig_ma.update_layout(title="Stock Prices and Moving Averages",
                        xaxis_title='Date', yaxis_title='Price')

    # Display the moving averages chart using plotly_chart
    st.plotly_chart(fig_ma, use_container_width=True)

    # Annotation for Moving Averages
    st.write("**Moving Averages**: Moving averages are commonly used technical indicators that smooth out price data to identify trends over different periods. They help in understanding the overall direction of the stock prices.")

    # Create a Plotly figure for Bollinger Bands
    fig_bb = go.Figure()

    # Add traces for Bollinger Bands
    fig_bb.add_trace(go.Scatter(x=df['date'], y=df['20_lower_band'], mode='lines', name='Bollinger Bands (Lower)', line=dict(color='green')))
    fig_bb.add_trace(go.Scatter(x=df['date'], y=df['20_upper_band'], mode='lines', name='Bollinger Bands (Upper)', line=dict(color='red')))
    fig_bb.add_trace(go.Scatter(x=df['date'], y=df['20_ema'], mode='lines', name='20 Days EMA (Middle)', line=dict(color='blue')))

    # Update layout for Bollinger Bands chart
    fig_bb.update_layout(title="Bollinger Bands (20 Days, 20 Days EMA)",
                        xaxis_title='Date', yaxis_title='Price')

    # Display the Bollinger Bands chart using plotly_chart
    st.plotly_chart(fig_bb, use_container_width=True)

    # Annotation for Bollinger Bands
    st.write("**Bollinger Bands**: Bollinger Bands consist of a middle band being an N-period simple moving average (SMA), an upper band at K times an N-period standard deviation above the middle band, and a lower band at K times an N-period standard deviation below the middle band. They are used to identify volatility and potential overbought or oversold conditions.")

    # Create a Plotly figure for RSI
    fig_rsi = go.Figure()

    # Add trace for 20 Days RSI
    fig_rsi.add_trace(go.Scatter(x=df['date'], y=df['20_days_rsi'], mode='lines', name='20 Days RSI', line=dict(color='orange')))

    # Add horizontal lines for overbought and oversold levels
    fig_rsi.add_shape(
        go.layout.Shape(
            type='line',
            x0=df['date'].iloc[0],
            x1=df['date'].iloc[-1],
            y0=70,
            y1=70,
            line=dict(color='red', width=2),
            name='Overbought Level'
        )
    )

    fig_rsi.add_shape(
        go.layout.Shape(
            type='line',
            x0=df['date'].iloc[0],
            x1=df['date'].iloc[-1],
            y0=30,
            y1=30,
            line=dict(color='green', width=2),
            name='Oversold Level'
        )
    )

    # Add text annotations
    fig_rsi.add_annotation(
        go.layout.Annotation(
            x=df['date'].iloc[0],
            y=70,
            xref='x',
            yref='y',
            text='Overbought (RSI > 70)',
            showarrow=True,
            arrowhead=2,
            ax=-40,
            ay=-30
        )
    )

    fig_rsi.add_annotation(
        go.layout.Annotation(
            x=df['date'].iloc[0],
            y=30,
            xref='x',
            yref='y',
            text='Oversold (RSI < 30)',
            showarrow=True,
            arrowhead=2,
            ax=-40,
            ay=30
        )
    )

    # Update layout for the RSI chart
    fig_rsi.update_layout(
        title="Relative Strength Index (RSI)",
        xaxis_title='Date', yaxis_title='RSI',
        annotations=[
            dict(
                x=df['date'].iloc[0],
                y=70,
                xref="x",
                yref="y",
                text="Overbought (RSI > 70)",
                showarrow=True,
                arrowhead=2,
                ax=-40,
                ay=-30
            ),
            dict(
                x=df['date'].iloc[0],
                y=30,
                xref="x",
                yref="y",
                text="Oversold (RSI < 30)",
                showarrow=True,
                arrowhead=2,
                ax=-40,
                ay=30
            )
        ]
    )

    # Display the RSI chart using plotly_chart
    st.plotly_chart(fig_rsi, use_container_width=True)

    # Annotation for RSI
    st.write("**Relative Strength Index (RSI)**: RSI is a momentum indicator that measures the speed and change of price movements. Values above 70 indicate that an asset may be overbought, while values below 30 suggest that it may be oversold.")


# Button to trigger API request
if st.sidebar.button("Get Stock Data"):
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
        # Fetch stock data
        results = get_stock_data(api_url, data, headers)

        if results:
            stock_data = results[0]

            # Display general information with styles
            st.markdown(f"### Ticker Symbol: {stock_data['ticker_symbol']}  \n"
                        f"**Stock ID:** {stock_data['stock_id']}  \n"
                        f"**Country:** {stock_data['country']}", unsafe_allow_html=True)

            # Fetch stock prices data
            if 'data' in stock_data:
                dates = [pd.to_datetime(entry['date']).strftime('%Y-%m-%d') for entry in stock_data['data']]
                close_prices = [entry['close'] for entry in stock_data['data']]

                # Create DataFrame with 'stock_id', 'ticker_symbol', 'Date', and 'Close Prices' columns
                stock_prices_df = pd.DataFrame({
                    'date': pd.to_datetime(dates),
                    'close_prices': close_prices
                })

                # Draw stock prices chart
                draw_stock_prices_chart(stock_prices_df)
            else:
                st.warning(f"No stock data for {stock_data['ticker_symbol']}!!!")
        else:
            st.warning("No results")

    # Fetch technical analysis data if the checkbox is selected
    else:
        # API request for technical analysis
        data['technical'] = True

        # Fetch technical analysis data
        results = get_stock_data(api_url, data, headers)

        if results:
            stock_data = results[0]

            # Display general information with styles
            st.markdown(f"### Ticker Symbol: {stock_data['ticker_symbol']}  \n"
                        f"**Stock ID:** {stock_data['stock_id']}  \n"
                        f"**Country:** {stock_data['country']}", unsafe_allow_html=True)
            # Extract relevant columns from each entry in "technical_view"
            data_list = []
            if 'technical' in stock_data:
        
                for entry in stock_data['technical']:
                    data_list.append({
                        "date": pd.to_datetime(entry["date"]),
                        "close": entry["close"],
                        "20_ema": entry["ma_20_days_ema"],
                        "50_sma": entry["ma_50_days_sma"],
                        "200_sma": entry["ma_200_days_sma"],
                        "50_ema": entry["ma_50_days_ema"],
                        "200_ema": entry["ma_200_days_ema"],
                        "20_lower_band": entry["bb_20_lower_band"],
                        "20_upper_band": entry["bb_20_upper_band"],
                        "20_days_rsi": entry["20_days_rsi"]
                    })

                # Create DataFrame from the list of dictionaries
                df = pd.DataFrame(data_list)

                # Draw technical analysis charts
                draw_technical_analysis_charts(df)
            else:
                st.warning("Techincal Analysis Data is not available.")
                stock_data = results[0]

                # Fetch stock prices data
                if 'data' in stock_data:
                    dates = [pd.to_datetime(entry['date']).strftime('%Y-%m-%d') for entry in stock_data['data']]
                    close_prices = [entry['close'] for entry in stock_data['data']]

                    # Create DataFrame with 'stock_id', 'ticker_symbol', 'Date', and 'Close Prices' columns
                    stock_prices_df = pd.DataFrame({
                        'date': pd.to_datetime(dates),
                        'close_prices': close_prices
                    })

                    # Draw stock prices chart
                    draw_stock_prices_chart(stock_prices_df)
                else:
                    st.warning(f"No stock data for {stock_data['ticker_symbol']}!!!")
            
        else:
            st.warning("No results")
