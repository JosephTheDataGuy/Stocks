import os
import requests
import json
import psycopg2
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
from Secrets_1 import gather_keys
import pymysql
import pandas as pd
import re

def main():
    #gathering all necessary secrets as variables from seperate file not stored in git.
    Alpha_vantage_API_key, News_API_Key, pg_username, pg_password, pg_host, pg_port, pg_database_name = gather_keys()

    #getting user input to retrieve ticker symbol data from alpha vantage api
    ticker_data, symbol = get_ticker_data(Alpha_vantage_API_key)

    #save ticker_date to downloads folder incase of server connection fail. 
    json_file_path = write_dataframe_to_json(ticker_data, f"{symbol}.json")
    print(f"DateFarame saved to JSON file: {json_file_path}")

    # Connect to PostgreSQL database
    with connect_to_postgres(pg_username, pg_password, pg_host, pg_port, pg_database_name) as connection:
        cursor = connection.cursor()

        # Storing symbol data into PostgreSQL
        store_data_in_postgres(cursor, ticker_data, symbol)

def connect_to_postgres(username, password, host, port, database_name):
    """
    Connects to the PostgreSQL database and returns the connection object.
    """
    connection = psycopg2.connect(user=username,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database_name)
    print("Connected to PostgreSQL database successfully!")
    return connection

def store_data_in_postgres(cursor, df, symbol):
    """
    This function stores the provided pandas DataFrame (`df`) into a table named
    after the ticker symbol (`symbol`) in the connected PostgreSQL database (`cursor`).

    Args:
        cursor: A cursor for the PostgreSQL database.
        df: The pandas DataFrame containing the data to be stored.
        symbol: The ticker symbol to be used for the table name.
    """

    # Generate the table name dynamically
    table_name = f"{symbol}_alpha_vantage"
    try:
        # Check if the table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (table_name,))
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # If the table doesn't exist, create it based on the DataFrame columns
            create_table(cursor, df, table_name)

        # Insert data into the table
        insert_data(cursor, df, table_name)
        print("Data stored successfully in PostgreSQL database!")
    except Exception as error:
        print("Error storing data in PostgreSQL database:", error)

def get_ticker_data(api_key):
    """
    This function gets user input to gather data for a desired ticker symbol using alpha vantage apis.
    Will return the ticker symbol and data gathered from the api as a pandas dataframe.

    Args:
        api_key: api key for the alpha vantage apis.
    """
    symbol = input('Ticker : ')
    outputsize = 'full'
    ts = TimeSeries(key = api_key, output_format='pandas')
    time_interval = input('Choose one of the following numbers corresponding with the desired time intervals: \n1. Daily (premium)\n2. Weekly\n3. Monthly\n4. 1 minute\n5. 5 minutes\n6. 15 minutes\n7. 30 minutes\n8. 60 minutes\n')
    if time_interval == '1':
        data, meta_data = ts.get_daily_adjusted(symbol=symbol,outputsize=outputsize)
    elif time_interval == '2':
        data, meta_data = ts.get_weekly_adjusted(symbol=symbol)
    elif time_interval == '3':
        data, meta_data = ts.get_monthly_adjusted(symbol=symbol)
    elif time_interval == '4':
        data, meta_data = ts.get_intraday(symbol,interval='1min',outputsize=outputsize)
    elif time_interval == '5':
        data, meta_data = ts.get_intraday(symbol,interval='5min',outputsize=outputsize)
    elif time_interval == '6':
        data, meta_data = ts.get_intraday(symbol,interval='15min',outputsize=outputsize)
    elif time_interval == '7':
        data, meta_data = ts.get_intraday(symbol,interval='30min',outputsize=outputsize)
    elif time_interval == '8':
        data, meta_data = ts.get_intraday(symbol,interval='60min',outputsize=outputsize)
    else:
        print('Undefined option selected.')
        return None
    
    # Extract 'Symbol' and 'Time Zone' values from meta_data
    symbol_value = meta_data.get('2. Symbol')
    time_zone_value = meta_data.get('6. Time Zone')

    # Add 'symbol' and 'time_zone' columns to the DataFrame
    data['symbol'] = symbol_value
    data['time_zone'] = time_zone_value
    data.reset_index(inplace=True)
    # Sanitize column names to remove leading digits and whitespace
    # We iterate over each column name in the DataFrame 'data.columns'
    # The regular expression pattern r'^\d+\.\s*' matches any sequence of digits followed by a dot and optional whitespace at the beginning of a string
    # This pattern captures the leading digits and dot at the start of column names, such as '1. open', '2. high', etc.
    # The re.sub() function is used to replace any matches of the pattern with an empty string, effectively removing the leading digits and dot
    # After applying this operation to all column names, we get sanitized column names without the leading digits and dot
    # These sanitized column names are then assigned back to the DataFrame 'state.columns' as to avoid future issues when inserting into MySQL.
    data.columns = [re.sub(r'^\d+\.\s*', '', col).replace(' ', '_') for col in data.columns]

    print(data)
    #debugging print statement to check the type of the state variable
    print("Type of 'data' variable:", type(data))
    return pd.DataFrame(data), symbol

def create_table(cursor, df, table_name):
    """
    Creates a table in the database based on the DataFrame columns.
    """
    # Sample create_table_query example:
    # If df.columns contains ['date', 'open', 'high', 'low', 'close', 'volume'], and the
    # corresponding SQL types are ['VARCHAR(255)', 'FLOAT', 'FLOAT', 'FLOAT', 'FLOAT', 'INT'],
    # then the final create_table_query would look like:
    # "CREATE TABLE symbol_alpha_vantage (date VARCHAR(255), open FLOAT, high FLOAT,
    # low FLOAT, close FLOAT, volume INT)"
    create_table_query = f"CREATE TABLE {table_name} ("
    for column_name in df.columns:
        dtype = df[column_name].dtype
        sql_type = get_sql_type(dtype)
        create_table_query += f"{column_name} {sql_type}, "
    create_table_query = create_table_query[:-2] + ")"
    cursor.execute(create_table_query)
    print(f"Table '{table_name}' created successfully!")

def insert_data(cursor, df, table_name):
    """
    Inserts data into the specified table.
    """
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(df.columns))})"
    cursor.executemany(insert_query, df.values.tolist())

def get_sql_type(dtype):
    if dtype == 'int64':
        return 'INT'
    elif dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'object':
        return 'VARCHAR(255)'  # Adjust the length as needed for object columns
    else:
        return 'VARCHAR(255)'  # Default to VARCHAR for unsupported types

def write_dataframe_to_json(df, file_name):
    """
    Write a pandas DataFrame to a JSON file.

    Args:
        df (pandas.DataFrame): The DataFrame to be written to JSON.
        file_name (str): The name of the JSON file.

    Returns:
        str: The full path to the saved JSON file.
    """
    # Get the path to the Downloads folder
    downloads_folder = os.path.join(os.path.expanduser('~'), 'Downloads')

    # Construct the full file path
    file_path = os.path.join(downloads_folder, file_name)

    # Write the DataFrame to JSON
    df.to_json(file_path, orient='records')

    return file_path

if __name__ == "__main__":
    main()
    