#!/usr/local/bin/python3.11

import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import json
from decouple import config
from langdetect import detect
import time
import bs4 as bs

def delta_date(start_date,end_date):

    return abs((datetime.strptime(start_date, "%Y-%m-%d") - datetime.strptime(end_date, "%Y-%m-%d")).days)

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
    def __init__(self):

        #initialize value here
        self.start_date = "2023-10-21"
        self.end_date = "2023-10-22"
        self.tickers = ['AMZN']

        self.start_date_ = datetime.strptime(self.start_date, "%Y-%m-%d")  #datetime object
        self.end_date_ = datetime.strptime(self.end_date, "%Y-%m-%d")    #datetime object
        self.delta_date = abs((self.end_date_ - self.start_date_).days) #number of days between 2 dates


        try:
            self.start_date_ > self.end_date_
        except:
            print("'start_date' is after 'end_date'")

        t = (datetime.now()- relativedelta(years=1))
        d= datetime.strptime(self.start_date, "%Y-%m-%d")

        if (datetime.strptime(self.start_date, "%Y-%m-%d") <= (datetime.now()- relativedelta(years=1))) :
            raise Exception("'start_date' is older than 1 year. It doesn't work with the free version of FinHub")


class FinnHub():
    """Class to make API calls to FinnHub"""

    def __init__(self,start_date,end_date,start_date_,end_date_,tickers):
        self.max_call = 60
        self.time_sleep = 60
        self.nb_request = 0
        self.finhub_key = config('FINHUB_KEY')
        self.news_header = ['category', 'datetime','headline','id','image','related','source','summary','url']
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self.ticker_request = tickers 
    
        self.js_data = []

        self.start_date_ = start_date_ 
        self.end_date_ = end_date_
        
        tickers = get_tickers()

        for ticker_ in self.tickers:
            self.js_data.clear()
            self.ticker = ticker_ + '_'
            self.ticker_request = ticker_
            self.req_new()

    def iterate_day(func):
        """ Decorator that makes the API call on FinHub each days between the `self.start_date`
        and `self.end_date` """

        def wrapper_(self):
            delta_date_ = delta_date(self.start_date,self.end_date)
            date_ = self.start_date
            date_obj = self.start_date_

            for item in range(delta_date_ + 1):
                self.nb_request +=1
                func(self,date_)
                date_obj = date_obj + relativedelta(days=1)
                date_  = date_obj.strftime("%Y-%m-%d")
                if self.nb_request == (self.max_call-1):
                    time.sleep(self.time_sleep)
                    self.nb_request=0
        return wrapper_
    
    @iterate_day
    def req_new(self, date_):
        """ Method that makes news request(s) to the Finnhub API"""
        try:
            request_ = requests.get('https://finnhub.io/api/v1/company-news?symbol=' + self.ticker_request + '&from=' +
                                    date_ + '&to=' + date_ + '&token=' + self.finhub_key)
            request_.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
            news_data = request_.json()
            print(news_data)
            self.js_data += news_data
        except requests.RequestException as e:
            print(f"Error while fetching data for date {date_}: {e}")

init_ = Init()
finhub = FinnHub(start_date=init_.start_date, end_date=init_.end_date,start_date_=init_.start_date_ ,
                end_date_ =init_.end_date_, tickers=init_.tickers)
