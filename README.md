# Stock Data API

# Table of Contents

- [Stock Data API](#stock-data-api)
  - [Overview](#overview)
  - [Demo](#demo)
  - [Getting Started](#getting-started)
  - [Features](#features)
  - [Prerequisites](#prerequisites)
    - [Ubuntu Environment](#ubuntu-environment)
    - [Python (version 3.x)](#python-version-3x)
    - [Apache Spark](#apache-spark)
    - [PostgreSQL](#postgresql)
    - [Other Python Packages](#other-python-packages)
  - [Configuration](#configuration)
  - [Technologies Used](#technologies-used)
  - [Project Structure](#project-structure)
  - [Database Initialization and Running the Stock Data API](#database-initialization-and-running-the-stock-data-api)
    - [Initialize the Database and Run Tests](#initialize-the-database-and-run-tests)
      - [Running the `db_operator.py` Script](#running-the-db_operatorpy-script)
      - [Checking Test Results](#checking-test-results)
    - [Running the Stock Data API](#running-the-stock-data-api-1)
      - [Executing `main.py`](#executing-mainpy)
      - [Accessing the Streamlit Web Application](#accessing-the-streamlit-web-application)
  - [Learning Motive](#learning-motive)
  - [Note: Future Updates](#note-future-updates)


## Overview

The Stock Data API is a backend server designed to handle requests related to stock market data. It provides endpoints for retrieving information about various stocks, including historical stock prices, technical analysis, and more. This API is intended to serve as a foundational component for applications or services that require access to stock market data.

## Demo
![Demo GIF](https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExODhwZG9mb2JjZG0waDU4Y3ptc3NkeGRyOTI0MDNkZ2lueHp0bHNhcCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/7rw3mLf5DV2YIy5t4g/giphy.gif)

## Getting Started

Follow these steps to quickly set up and run the Stock Data API on your local machine.

1. Clone the repository.
2. Install dependencies with `pip install -r requirements.txt`.
3. Initialize the database and run tests: `python database-ops/db_operator.py`.
4. Configure the `.env` file with your credentials.
5. Run the main application: `python main.py`.
6. Access the Streamlit web application: `streamlit run frontend/main.py`.

## Features

- **Get Companies Data:** Retrieve information about multiple companies, including stock prices and basic details.
- **Get Technical Analysis:** Obtain technical analysis data for a specific stock, such as moving averages.

## Prerequisites

Before running the Stock Data API on an Ubuntu environment, ensure that you have the following dependencies installed:

- **Ubuntu Environment:** This project is designed to run on an Ubuntu environment. Ensure that you have Ubuntu installed and configured.
- **Python (version 3.x):** The programming language used for the backend development.
- **Apache Spark:** The project utilizes Apache Spark for distributed data processing. Make sure to install and configure Apache Spark. You can follow the official Apache Spark installation guide: [Apache Spark Installation](https://spark.apache.org/docs/latest/index.html).

- **PostgreSQL:** The project uses PostgreSQL for database storage. Install and set up PostgreSQL. You can download it from [PostgreSQL Downloads](https://www.postgresql.org/download/).

- **Other Python Packages:** The project dependencies are listed in the `requirements.txt` file. Install them using the following command:
    ```bash
    pip install -r requirements.txt
    
## Configuration

To run the Stock Data API, you'll need to set up a configuration file named `.env` in the project root directory. This file contains sensitive information and should not be shared or exposed.

Here's a template for the `.env` file:

```env
# Replace the placeholders with your actual credentials and information

# Google Cloud Service Account Key file path
GOOGLE_APPLICATION_CREDENTIALS='./google-cloud-key.json'

# API Key for stock market data
STOCK_API_KEY='your_stock_api_key'

# Secret key for securing the application
SECRET_KEY='your_secret_key'

# Database configuration
DB_USER='your_database_username'
DB_PASSWORD='your_database_password'
DB_HOST='localhost'
DB_PORT='5432'
DB_NAME='stockmarket'
DB_DRIVER='org.postgresql.Driver'
DB_URL='jdbc:postgresql://${DB_HOST}:${DB_PORT}/${DB_NAME}'
```

## Technologies Used

The Stock Data API leverages the following technologies:

- **Apache Spark:** The project utilizes Apache Spark for distributed data processing. Spark enables efficient analysis of large-scale financial datasets and empowers data transformations.
- **SQL:** Structured Query Language (SQL) is employed for managing and querying data. SQL provides a powerful and standardized way to interact with databases and process data efficiently.

<details>
  <summary><strong>Project Structure</strong></summary>

  The Stock Data API project consists of several important files and directories. Here's a brief explanation of each:

  <details>
    <summary><code>stock_market_operator.py</code></summary>

    This file defines a `StockMarketOperator` class that provides both synchronous and asynchronous functionalities for fetching, processing, and storing stock market data. It uses Yahoo Finance to fetch the data, Apache Spark for processing, and interacts with a PostgreSQL database for storage. The class also supports retrieving technical data from a database view or tables if required. The asynchronous methods allow for non-blocking operations, improving the overall efficiency of the program.
  </details>

  <details>
    <summary><code>spark_processor.py</code></summary>

    This Python file, `spark_processor.py`, contains functions for processing stock market data using Apache Spark. It includes functionalities for cleaning the data, calculating moving averages, Bollinger Bands, and the Relative Strength Index (RSI). The processed data can then be displayed or further used for analysis. The file makes extensive use of PySpark's DataFrame API and SQL functions to perform these operations.
  </details>

  <details>
    <summary><code>data_processing/stock_data_processing.py</code></summary>

    The `spark_processor.py` file is a Python script designed for processing stock market data using Apache Spark. It includes functions for cleaning data, calculating moving averages, Bollinger Bands, and the Relative Strength Index (RSI). The processed data can then be displayed or further used for analysis.
  </details>

  <details>
    <summary><code>database-ops/db_operator.py</code></summary>

    This Python file contains the `SqlDatabaseOperator` class, which provides methods for managing a SQL database. It includes functionalities for establishing a database connection, executing SQL statements, and performing data operations such as creating tables and inserting data from a CSV file.
  </details>

  <details>
    <summary><code>schedule-tasks/</code></summary>

    This directory houses Python files responsible for automating daily tasks. These tasks ensure the Stock Data API stays updated with the latest market information by running scheduled processes for data retrieval and storage maintenance.
  </details>

  <details>
    <summary><code>main.py</code></summary>

    This file contains the main application logic. It includes the Quart application setup, route definitions, and other backend functionalities.
  </details>

  <details>
    <summary><code>frontend/</code></summary>

    This directory houses the Streamlit web application responsible for displaying data from the backend. The Streamlit app offers a user-friendly interface to interact with stock market information.
  </details>

  <details>
    <summary><code>requirements.txt</code></summary>

    Lists all the Python dependencies required to run the Stock Data API. You can install them using `pip install -r requirements.txt`.
  </details>

  <details>
    <summary><code>README.md</code></summary>

    The main documentation file providing an overview of the project, features, prerequisites, technologies used, and project structure.
  </details>

</details>

## Database Initialization and Running the Stock Data API

Before using the Stock Data API, follow these steps to initialize the database, create tables, and run tests.

<details>
<summary><strong>Initialize the Database and Run Tests</strong></summary>

1. Run the `database-ops/db_operator.py` file.

    ```bash
    python database-ops/db_operator.py
    ```

    This script performs the following actions:

    - Drops the existing database if it exists.
    - Creates a new database.
    - Creates tables and schemas.
    - Loads company information from a CSV file.
    - Tests inserting and analyzing stock data.
    - Tests retrieving technical data from a materialized view.
    - Tests inserting and retrieving technical data for another stock.

2. Check the console output for successful test results.

    Successful tests ensure that the database and tables are set up correctly, and the Stock Data API is ready for use.
</details>

<details>
<summary><strong>Running the Stock Data API</strong></summary>

Now that the database is initialized, you can run the Stock Data API using the provided files and directories:

1. Execute the main application logic in `main.py`.

    ```bash
    python main.py
    ```

2. Access the Streamlit web application in the `frontend/` directory for a user-friendly interface to interact with stock market information.

    ```bash
    streamlit run frontend/main.py
    ```

    This launches the frontend application, allowing you to explore stock data through an intuitive web interface.

Feel free to explore the various functionalities of the Streamlit app and enjoy interacting with the stock market information.
</details>

## Learning Motive

This project is primarily crafted for the purpose of learning and practicing data processing techniques and backend development. By incorporating technologies like Apache Spark and SQL, the aim is to gain hands-on experience in managing, analyzing, and serving financial data through a scalable backend API.

## Note: Future Updates

This README provides a high-level overview of the Stock Data API. More detailed information about the project, including advanced usage, setup instructions, and API documentation, will be updated in the future. Stay tuned for the latest features, improvements, and comprehensive project documentation.