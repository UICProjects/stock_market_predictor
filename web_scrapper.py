import csv
import finnhub
import os
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import json
from decouple import config
from langdetect import detect
import time
import threading
import bs4 as bs
from concurrent.futures import ThreadPoolExecutor, as_completed


file_lock = threading.Lock()

def get_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find_all('table')[0] 

    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text.strip('\n')
        tickers.append(ticker)
    return tickers

def save_to_csv(news_data, file_name):
    if not news_data:
        print("No data to save.")
        return

    file_path = Path.cwd() / "output" / file_name

    with file_lock:  # Acquire the lock
        with open(file_path, 'a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=news_data[0].keys())
            # Check if file is empty to decide whether to write header
            file.seek(0, 2)  # Move to the end of file
            if file.tell() == 0:  # File is empty
                writer.writeheader()
            writer.writerows(news_data)

    print(f"Data successfully saved to {file_path}")

class Finnhub():
    def __init__(self,start_date,end_date,tickers):
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.finhub_key = config('FINHUB_KEY')
        self.finnhub_client = finnhub.Client(self.finhub_key)

    def fetch_news_for_ticker(self, ticker):
        current_date = self.start_date

        while current_date <= self.end_date:
            formatted_date = current_date.strftime('%Y-%m-%d')
            retry_count = 0
            max_retries = 3
            delay = 5 

            while retry_count < max_retries:
                try:
                    daily_news = self.finnhub_client.company_news(ticker, _from=formatted_date, to=formatted_date)
                    if daily_news:
                        save_to_csv(daily_news, f'news_data_ws.csv')  # Save immediately after fetching
                    print(f"Fetched data for {ticker} on {formatted_date}")
                    break
                except Exception as e:
                    if hasattr(e, 'response') and e.response.status_code == 429:
                        print(f"Rate limit hit. Retrying {ticker} for {formatted_date} after {delay} seconds...")
                        time.sleep(delay)
                        retry_count += 1
                        delay *= 2  
                    else:
                        print(f"Error fetching data for {ticker} on {formatted_date}: {e}")
                        break

            current_date += timedelta(days=1)
            time.sleep(1)  # Respect API rate limits

    def get_news_data(self):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.fetch_news_for_ticker, ticker) for ticker in self.tickers]

            for future in as_completed(futures):
                try:
                    future.result() 
                except Exception as e:
                    print(f"Error in thread: {e}")

tickers = get_tickers() 
end_date = datetime.now() - timedelta(days=202)
start_date = end_date - timedelta(days=300)  # Last year
formatted_start_date = start_date.strftime('%Y-%m-%d')
formatted_end_date = end_date.strftime('%Y-%m-%d')

finhub = Finnhub(start_date=formatted_start_date, end_date=formatted_end_date,tickers=tickers)
finhub.get_news_data()