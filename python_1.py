import os
import requests
import json
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.fundamentaldata import FundamentalData

Alpha_vantage_API_key = 'FLWYXNGXMO1NLK0S'
News_API_Key = '9d7c8a51273e4ea9ad0bed4b2b0002e2'

def main():
    symbol = input('Ticker : ')
    outputsize = 'compact'
    ts = TimeSeries(key = Alpha_vantage_API_key, output_format='pandas')
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
    print(state)
main()
    