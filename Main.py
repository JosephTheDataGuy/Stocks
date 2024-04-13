import os
import requests
import json
import psycopg2
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData
from Secrets_1 import gather_keys

def main():
    #gathering all necessary secrets as variables from seperate file not stored in git.
    Alpha_vantage_API_key, News_API_Key, rds_username, rds_password, rds_endpoint, rds_hostname, rds_database_name = gather_keys()
    
    #connecting to rds database
    try:
        connection_string = f"dbname='{rds_database_name}' user='{rds_username}' password='{rds_password}' host='{rds_endpoint}' port=5432"
        connection = psycopg2.connect(connection_string)
    except Exception as error:
        print(error)
    #getting user input to retrieve ticker symbol data from alpha vantage api
    state, symbol = get_ticker_data(Alpha_vantage_API_key)
    #storing symbol data into rds
    store_data_in_postgres(connection, state, symbol)
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

def store_data_in_postgres(connection, df, symbol):
  """
  This function stores the provided pandas dataframe (`df`) into a table named
  after the ticker symbol (`symbol`) in the connected Postgres database (`connection`).

  Args:
      connection: A psycopg2 connection object to the Postgres database.
      df: The pandas dataframe containing the data to be stored.
      symbol: The ticker symbol to be used for the table name.
  """

  # Generate the table name dynamically
  table_name = f"{symbol}_alpha_vantage"

  # Create the table (if it doesn't exist)
  try:
      cursor = connection.cursor()
      cursor.execute(f"""
          CREATE TABLE IF NOT EXISTS {table_name} (
              {", ".join(df.columns)}
          );
      """)
      connection.commit()
  except Exception as error:
      print(f"Error creating table {table_name}: {error}")
      return

  # Convert dataframe to a list of tuples (suitable for bulk insertion)
  data = df.to_records(listindex=False)

  # Insert data into the table
  try:
      cursor = connection.cursor()
      insert_query = f"INSERT INTO {table_name} VALUES %s"
      cursor.executemany(insert_query, data)
      connection.commit()
      print(f"Data for symbol {symbol} inserted successfully!")
  except Exception as error:
      print(f"Error inserting data into {table_name}: {error}")

if __name__ == "__main__":
    main()
    