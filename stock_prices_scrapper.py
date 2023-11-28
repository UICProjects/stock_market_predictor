#!/usr/local/bin/python3.11

"""Module to extract data from the web, clean and parse it"""

import requests
import bs4 as bs
from datetime import datetime
from decouple import config
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import yfinance as yf
from pathlib import Path


def get_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find_all('table')[0] 

    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text.strip('\n')
        tickers.append(ticker)
    return tickers

class Init():
    """Class to initialize attributes that are specific to the Alpha Vantage API """

    def __init__(self):
        self.tickers = ["AMZN"]

class HistoricalReturn(Init):
    """Class to get daily historical price from Alpha Vantage, calculate daily return using """

    def __init__(self,start_date_,end_date_,ticker):

        self.ticker = ticker
        self.pd_data = pd.DataFrame()

        self.start_date_ = start_date_
        self.end_date_ = end_date_ 


    def historical_price(self):
        """ Method that get the historical price from the Alpha Vantage API, store and manipulate in a pandas Dataframe
        """

        self.pd_data = yf.download(self.ticker, 
                                   start=self.start_date_, 
                                   end=self.end_date_, 
                                   progress=False)
        if not self.pd_data.empty:
            self.pd_data['Ticker'] = self.ticker
        return self.pd_data

init_ = Init()
file_name = "historical_stock_prices.csv"
filepath = Path.cwd() / "output" / file_name
end_date = datetime.now()
start_date = end_date - timedelta(days=365)  # Last year
formatted_start_date = start_date.strftime('%Y-%m-%d')
formatted_end_date = end_date.strftime('%Y-%m-%d')

tickers = get_tickers()
all_data = []

for ticker in tickers:
    hist_ret = HistoricalReturn(start_date_=formatted_start_date,
                                end_date_=formatted_end_date, ticker=ticker)
    ticker_data = hist_ret.historical_price()
    if not ticker_data.empty:
        all_data.append(ticker_data)

combined_data = pd.concat(all_data)
combined_data.to_csv(filepath)