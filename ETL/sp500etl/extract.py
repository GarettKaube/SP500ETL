import requests
import bs4 as bs
import datetime
import yfinance as yf
import pandas as pd
import os
import logging
from tqdm import tqdm

logger=logging.getLogger("etl")

def extract_sp500_data_daily(out_path):
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)

    tickers = [s.replace('\n', '') for s in tickers]
    start = datetime.datetime(2010,1,1)
    end = datetime.datetime.today()

    data = yf.download(tickers, start=start, end=end)
    data.index = pd.to_datetime(data.index)


    progress_bar = tqdm(
        total=len(data.index.year.unique()), 
        desc=f"Saving data to {out_path}"
    )

    logger.info(f"Saving data to {out_path}")
    for year_ in data.index.year.unique():
        path = os.path.join(out_path, f"{year_}")
        for month_ in data.index.month.unique():
            df = data[(data.index.year == year_) & (data.index.month == month_)]
            os.makedirs(path, exist_ok=True)
            df.to_parquet(os.path.join(out_path, f"{year_}/sp500_{year_}_{month_}.parquet"), index=True)
        progress_bar.update(1)


def main():
    pass

if __name__ == "__main__":
    main()