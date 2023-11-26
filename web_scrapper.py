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
import bs4 as bs

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
    existing_data = []

    if file_path.exists():
        with open(file_path, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            existing_data = [row for row in reader]

    combined_data = list({item['id']: item for item in existing_data + news_data}.values())

    if not combined_data:
        print("No combined data to save.")
        return

    with open(file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=combined_data[0].keys())
        writer.writeheader()
        writer.writerows(combined_data)

    print(f"Data successfully saved to {file_path}")

class Finnhub():
    def __init__(self,start_date,end_date,tickers):
        self.tickers = tickers
        self.start_date = datetime.strptime(start_date, '%Y-%m-%d')
        self.end_date = datetime.strptime(end_date, '%Y-%m-%d')
        self.finhub_key = config('FINHUB_KEY')
        self.finnhub_client = finnhub.Client(self.finhub_key)

    def get_news_data(self):
        for ticker in self.tickers:
            all_news_data = []
            current_date = self.start_date
            while current_date <= self.end_date:
                formatted_date = current_date.strftime('%Y-%m-%d')
                try:
                    daily_news = self.finnhub_client.company_news(ticker, _from=formatted_date, to=formatted_date)
                    all_news_data.extend(daily_news)
                    print(f"Fetched data for {ticker} on {formatted_date}")
                except Exception as e:
                    print(f"Error fetching data for {ticker} on {formatted_date}: {e}")

                current_date += timedelta(days=1)
                time.sleep(1)  # Respect API rate limits

            save_to_csv(all_news_data, f'news_data_ws.csv')

tickers = get_tickers() 
end_date = datetime.now()
start_date = end_date - timedelta(days=1)  # Last year
formatted_start_date = start_date.strftime('%Y-%m-%d')
formatted_end_date = end_date.strftime('%Y-%m-%d')

finhub = Finnhub(start_date=formatted_start_date, end_date=formatted_end_date,tickers=tickers)
finhub.get_news_data()