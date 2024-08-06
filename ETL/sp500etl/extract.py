import requests
import bs4 as bs
import datetime
import yfinance as yf
import pandas as pd
import os
import logging
from tqdm import tqdm
import pandas_datareader.data as web

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

    data = yf.download(tickers, start=start, end=end, interval="1d")
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


def extract_fama_french_five_factors(out_path):
    """ Uses pandas_datareader.data to fetch the Fama French five
    factor data and saves it to parquet
    """
    factor_data = web.DataReader('F-F_Research_Data_5_Factors_2x3', 'famafrench', start='2000')[0].drop('RF', axis=1)
    factor_data.index = factor_data.index.to_timestamp()
    years = factor_data.index.year.unique() 
    for year in years:
        save_data = factor_data.loc[f"{year}"]
        path = f"{out_path}/{year}"
        os.makedirs(path, exist_ok=True)
        save_data.to_parquet(path + "/fama-french-factors.parquet")


# factor_data = factor_data.resample('M').last().div(100)
# factor_data.index.name = 'date'


def main():
    pass

if __name__ == "__main__":
    main()