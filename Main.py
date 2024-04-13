import os
import requests
import json
import psycopg2
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
from Secrets_1 import gather_keys
import pymysql

def main():
    #gathering all necessary secrets as variables from seperate file not stored in git.
    Alpha_vantage_API_key, News_API_Key, rds_username, rds_password, rds_endpoint, rds_hostname, rds_database_name = gather_keys()
    
    #connecting to rds database
    try:
        connection = pymysql.connect(host=rds_endpoint,user=rds_username,password=rds_password)
        cursor = connection.cursor()
        print("Connected to MySQL database successfuly!")
    except Exception as error:
        print(error)
    
    #testing connection by displaying mysql version.
    cursor.execute('select version()')
    mysql_version = cursor.fetchone()[0]
    print(f'MySQL version: {mysql_version}')
    
    #getting user input to retrieve ticker symbol data from alpha vantage api
    state, symbol = get_ticker_data(Alpha_vantage_API_key)
    #storing symbol data into rds
    store_data_in_mysql(cursor, state, symbol)

    #committing the transaction and closing the connection
    connection.commit()
    connection.close()

def get_ticker_data(api_key):
    """
    This function gets user input to gather data for a desired ticker symbol using alpha vantage apis.
    Will return the ticker symbol and data gathered from the api as a pandas dataframe.

    Args:
        api_key: api key for the alpha vantage apis.
    """
    symbol = input('Ticker : ')
    outputsize = 'compact'
    ts = TimeSeries(key = api_key, output_format='pandas')
    time_interval = input('Choose one of the following numbers corresponding with the desired time intervals: \n1. Daily (premium)\n2. Weekly\n3. Monthly\n4. 1 minute\n5. 5 minutes\n6. 15 minutes\n7. 30 minutes\n8. 60 minutes\n')
    if time_interval == '1':
        state = ts.get_daily_adjusted(symbol=symbol,outputsize=outputsize)
    elif time_interval == '2':
        state = ts.get_weekly_adjusted(symbol=symbol)
    elif time_interval == '3':
        state = ts.get_monthly_adjusted(symbol=symbol)
    elif time_interval == '4':
        state = ts.get_intraday(symbol,interval='1min',outputsize=outputsize)
    elif time_interval == '5':
        state = ts.get_intraday(symbol,interval='5min',outputsize=outputsize)
    elif time_interval == '6':
        state = ts.get_intraday(symbol,interval='15min',outputsize=outputsize)
    elif time_interval == '7':
        state = ts.get_intraday(symbol,interval='30min',outputsize=outputsize)
    elif time_interval == '8':
        state = ts.get_intraday(symbol,interval='60min',outputsize=outputsize)
    else:
        print('Undefined option selected.')
    return state, symbol

def store_data_in_mysql(cursor, df, symbol):
    """
    This function stores the provided pandas dataframe (`df`) into a table named
    after the ticker symbol (`symbol`) in the connected MySQL database (`cursor`).

    Args:
          cursor: A cursor for the MySQL database.
        df: The pandas dataframe containing the data to be stored.
        symbol: The ticker symbol to be used for the table name.
    """

    # Generate the table name dynamically
    table_name = f"{symbol}_alpha_vantage"

    try:
        # Check if the table exists
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        table_exists = cursor.fetchone() is not None

        if not table_exists:
                # If the table doesn't exist, create it based on the DataFrame columns
                create_table_query = f"CREATE TABLE {table_name} ("
                for column_name, dtype in df.dtypes.iteritems():
                    sql_type = get_sql_type(dtype)
                    create_table_query += f"{column_name} {sql_type}, "
                create_table_query = create_table_query[:-2] + ")"
                cursor.execute(create_table_query)
                print(f"Table '{table_name}' created successfully!")

        # Insert data into the table
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s']*len(df.columns))})"
        cursor.executemany(insert_query, df.values.tolist())
        print("Data stored successfully in MySQL database!")
    except Exception as error:
        print("Error storing data in MySQL database:", error)

def get_sql_type(dtype):
    if dtype == 'int64':
        return 'INT'
    elif dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'object':
        return 'VARCHAR(255)'  # Adjust the length as needed for object columns
    else:
        return 'VARCHAR(255)'  # Default to VARCHAR for unsupported types


if __name__ == "__main__":
    main()
    