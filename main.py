import argparse
import concurrent.futures
import jwt
import psycopg2
import os
from functools import wraps
from datetime import datetime, timedelta  # Import datetime and timedelta from the datetime module
from flask import Flask, request, jsonify
from database import StockData, delete_watchlist, fetch_boillinger_bands_data_from_db, fetch_moving_averages_data_from_db, fetch_relative_indexes_data_from_db, filter_stock_data_by_date_range, find_watchlist_by_id, process_stock, create_user, find_user_by_username, find_user_by_id, find_watchlist, create_watchlist, add_stock_to_watchlist, get_watchlist, update_watchlist_info, update_watchlist_stocks_info, process_stock_data
from passlib.hash import bcrypt
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from quart import Quart, jsonify, request

# Load the environment variables from the .env file
load_dotenv()

app = Quart(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')

# Enable debug mode
app.debug = True

# Initialize a Spark session
spark = SparkSession.builder.appName("StockDataProcessor").getOrCreate()



def fetch_technical_analysis(stock_id, ticker_symbol):
    moving_averages_data = fetch_moving_averages_data_from_db(stock_id, ticker_symbol)
    boillinger_bands_data = fetch_boillinger_bands_data_from_db(stock_id, ticker_symbol)
    relative_indexes_data = fetch_relative_indexes_data_from_db(stock_id, ticker_symbol)

    technical_analysis = [
        {'type': 'moving_averages', 'data': moving_averages_data},
        {'type': 'boillinger_bands', 'data': boillinger_bands_data},
        {'type': 'relative_indexes', 'data': relative_indexes_data}
    ]

    return technical_analysis


# =================================================================================================================

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        
        if not auth_header:
            return jsonify({'message': 'Token is missing'}), 401

        if not auth_header.startswith('Bearer '):
            return jsonify({'message': 'Invalid token format'}), 401

        token = auth_header[len('Bearer '):]

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Invalid token'}), 401

        return f(data, *args, **kwargs)

    return decorated


@app.route('/greet_user', methods=['GET'])
@token_required
def greet_user():
    data = request.get_json()
    # Extract user-specific data from the token
    user_id = data.get('user_id')  # Access user-specific data from the token

    return jsonify({'message': f'Hello, user with ID {user_id}!'})

# Define a route to handle incoming requests
@app.route('/get_companies_data', methods=['POST'])
def get_companies_data():
    data = request.get_json()

    country = data.get('country')
    ticker_symbols = data.get('ticker_symbols')
    start_date = data.get('start_date')
    end_date = data.get('end_date')

    if not country or not ticker_symbols or not start_date or not end_date:
        return jsonify({'error': 'Country, ticker_symbols, start_date, and end_date must be provided.'}), 400

    try:
        stock_data_result = []

        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks = [(ticker, country) for ticker in ticker_symbols]
            futures = [executor.submit(process_stock, *task) for task in tasks]

            for future in concurrent.futures.as_completed(futures):
                result = future.result()  # StockData object

                # Filter stock data based on the date range received from the request
                filtered_data = filter_stock_data_by_date_range(result.data, start_date, end_date)

                stock_data_result.append({
                    'ticker_symbol': result.ticker_symbol,
                    'country': result.country,
                    'stock_id': result.stock_id,
                    'data': [price_movement.to_dict() for price_movement in filtered_data]
                })

        return jsonify({'results': stock_data_result})

    except (Exception, psycopg2.DatabaseError) as error:
        return jsonify({'error': str(error)}), 500

@app.route('/create_user', methods=['POST'])
def create_user_route():
    data = request.get_json()

    username = data.get('username')
    plain_password = data.get('password')  # Get the plain password from the request
    email = data.get('email')
    first_name = data.get('first_name')
    last_name = data.get('last_name')
    date_of_birth = data.get('date_of_birth')

    if not username or not plain_password or not email or not first_name or not last_name or not date_of_birth:
        return jsonify({'error': 'All fields are required.'}), 400

    try:
        # Hash the plain password using bcrypt
        hashed_password = bcrypt.hash(plain_password)

        # Call the create_user function with the hashed password
        userID = create_user(username, hashed_password, email, first_name, last_name, date_of_birth)
        
        # Generate a JWT
        payload = {
            'user_id': userID,  # Include user-specific claims, e.g., user ID
            'username': username , # Include any other claims you need
            'exp': datetime.utcnow() + timedelta(days=1)  # Token expiration time
        }
        token = jwt.encode(payload, app.config['SECRET_KEY'], algorithm='HS256')

        # Return the JWT along with a success message
        return jsonify({'message': 'User created successfully', 'access_token': token}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Login route
@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': 'Both username and password are required.'}), 400

    # Check user credentials (you should validate against your database)
    user = find_user_by_username(username)

    if not user:
        return jsonify({'error': 'Invalid username or password.'}), 401

    # Generate a JWT token
    token_data = {
        'user_id': user.user_id,
        'username': user.username,
        'exp': datetime.utcnow() + timedelta(days=1)  # Token expiration time
    }
    token = jwt.encode(token_data, app.config['SECRET_KEY'], algorithm='HS256')

    return jsonify({'token': token})

@app.route('/add_to_watchlist', methods=['POST'])
@token_required
def add_to_watchlist(data):
    user_id = data.get('user_id')  # Extract user_id from the token data
    data = request.get_json()
    watchlist_name = data.get('watchlist_name')
    stock_id = data.get('stock_id')
    ticker_symbol = data.get('ticker_symbol')

    # Validate the user
    user = find_user_by_id(user_id)
    if user is None:
        return jsonify({'message': 'User not found'}), 404

    # Check if the watchlist exists for the user
    existing_watchlist = find_watchlist(user_id, watchlist_name)

    if existing_watchlist is None:
        # Watchlist doesn't exist, create it
        watchlist_id = create_watchlist(user_id, watchlist_name)
    else:
        watchlist_id = existing_watchlist.watchlist_id

    # Assuming you have a function to add the stock to the watchlist in your database
    add_stock_to_watchlist(watchlist_id, stock_id, ticker_symbol)

    return jsonify({'message': 'Stock added to watchlist successfully'}), 201

@app.route('/get_watchlist', methods=['GET'])
@token_required
def get_user_watchlist(data):
    user_id = data.get('user_id')
    watchlist = get_watchlist(user_id)
    if watchlist:
        # Convert each Watchlist object to a dictionary
        watchlist_data = [{'watchlist_id': w.watchlist_id, 'watchlist_name': w.watchlist_name, "stocks": w.stocks} for w in watchlist]
        response_data = {
            'user_id': user_id, 
            'Watchlists': watchlist_data
        }
        return jsonify(response_data), 200
    else:
        return jsonify({'message': 'Watchlist not found'}), 404

@app.route('/remove_watchlist', methods=['POST'])
@token_required
def remove_watchlist(data):
    user_id = data.get('user_id')
    watchlist_name = request.get_json().get('watchlist_name') # get from body request

    # Validate the user
    user = find_user_by_id(user_id)
    if user is None:
        return jsonify({'message': 'User not found'}), 404

    # Check if the watchlist exists
    watchlist = find_watchlist(user_id,watchlist_name)
   
    if watchlist is None:
        return jsonify({'message': 'Watchlist not found'}), 404

    # Assuming you have a function to remove the watchlist and its stocks from the database
    if delete_watchlist(watchlist.watchlist_id):
        return jsonify({'message': 'Watchlist removed successfully'}), 200
    else:
        return jsonify({'message': 'Failed to remove watchlist'}), 404

@app.route('/update_watchlist/<watchlist_id>', methods=['PUT'])
@token_required
def update_watchlist(data, watchlist_id):
    user_id = data.get('user_id')
    updated_watchlist_data = request.get_json()

    # Check if the user has permission to update the watchlist
    watchlist = find_watchlist_by_id(watchlist_id)
    if watchlist is None:
        return jsonify({'message': 'Watchlist not found'}), 404
    if watchlist['user_id'] != user_id:
        return jsonify({'message': 'Unauthorized'}), 401

    try:
        # Update the watchlist data
        updated_watchlist_name = updated_watchlist_data.get('watchlist_name')
        update_watchlist_info(watchlist_id, updated_watchlist_name)

        return jsonify({'message': 'Watchlist updated successfully'}), 200

    except Exception as e:
        return jsonify({'message': f'Error updating watchlist: {str(e)}'}), 500

@app.route('/update_watchlist_stocks/<watchlist_id>', methods=['PUT'])
@token_required
def update_watchlist_stocks(data, watchlist_id):
    user_id = data.get('user_id')
    updated_stocks = request.get_json()

    # Check if the user has permission to update the stocks in the watchlist
    watchlist = find_watchlist_by_id(watchlist_id)
    if watchlist is None:
        return jsonify({'message': 'Watchlist not found'}), 404
    if watchlist['user_id'] != user_id:
        return jsonify({'message': 'Unauthorized'}), 401

    try:
        # Update the stocks in the watchlist
        update_watchlist_stocks_info(watchlist_id, updated_stocks)

        return jsonify({'message': 'Stocks in watchlist updated successfully'}), 200

    except Exception as e:
        return jsonify({'message': f'Error updating watchlist stocks: {str(e)}'}), 500


#====================================================================================================
@app.route('/get_stock_data', methods=['POST'])
async def get_stock_data():
    try:
        data = await request.get_json()

        if not isinstance(data, dict) or "data" not in data or "technical" not in data:
            raise ValueError("Invalid request data format. 'data' and 'technical' fields are required.")

        start_date = data.get("start_date")
        end_date = data.get("end_date")
        technical_requested = data.get("technical", False)

        # Handle errors for start_date and end_date extraction
        if start_date is None or end_date is None:
            raise ValueError("Both start_date and end_date are required.")

        stock_data_result = []

        for entry in data["data"]:
            ticker_symbol = entry.get("ticker_symbol")
            country = entry.get("country")

            if country and ticker_symbol:
                result = await process_stock_data(spark, ticker_symbol, country, start_date, end_date, technical_requested)
                stock_data_result.append(result)

        return jsonify({'results': stock_data_result})

    except (Exception, psycopg2.DatabaseError, ValueError) as error:
        return jsonify({'error': str(error)}), 500

    

if __name__ == '__main__':
    app.run()
