import pandas as pd
from datetime import datetime, timezone, timedelta
class StockData:
    
    def __init__(self, filename : str = "Data/historical_stock_prices.csv") -> None:
        self.df = pd.read_csv(filename)
        
    def fetch_stock_date_for_date(self, exact: datetime, ticker: str, prefer_before: bool = True) -> dict:
        
        stock_data = self.df[(self.df['Date'] == str(exact.date())) & (self.df['Ticker'] == ticker)]
        
        if stock_data.shape[0] > 0:
            stock_data = stock_data.iloc[0]
            return stock_data.to_dict()
        elif prefer_before:
            td = timedelta(days=-1)
        else:
            td = timedelta(days=1)
        
        tries = 5
        
        for _ in range(tries):
            exact += td
            
            stock_data = self.df[(self.df['Date'] == str(exact.date())) & (self.df['Ticker'] == ticker)]
            
            if stock_data.shape[0] > 0:
                stock_data = stock_data.iloc[0]
                return stock_data.to_dict()
            
        raise ValueError(f"Invalid date: {str(exact.date())} or ticker {ticker}")
        
    def get_delta(self,timestamp: int, ticker: str) -> tuple:
        
        exact = datetime.fromtimestamp(timestamp)
        stock_data_1 = self.fetch_stock_date_for_date(exact, ticker)
        
        close = stock_data_1['Close']
        
        if exact.hour < 14:
            open = stock_data_1['Open']
            return close - open
        else:
            stock_data_2 = self.fetch_stock_date_for_date(exact + timedelta(days=1),ticker, False)
            open = stock_data_2['Open']
            return open - close
        
        
if __name__ == "__main__":
    fd = StockData()
    difference = fd.get_delta(1699742439, "AAPL")
    print(difference)