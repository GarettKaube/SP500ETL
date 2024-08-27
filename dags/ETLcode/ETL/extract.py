import requests
import bs4 as bs
import datetime
import yfinance as yf
import pandas as pd
import os
import logging
from tqdm import tqdm
import pandas_datareader.data as web
try:
    from ETLcode.ETL.utils import save_daily_data
except ModuleNotFoundError:
    from utils import save_daily_data

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

    save_daily_data(data, out_path, "sp500")


def extract_fama_french_five_factors(out_path):
    """ Uses pandas_datareader.data to fetch the Fama French five
    factor data and saves it to parquet
    """
    factor_data = web.DataReader('F-F_Research_Data_5_Factors_2x3_daily', 'famafrench', start='2000')[0].drop('RF', axis=1)
    try:
        factor_data.index = factor_data.index.to_timestamp()
    except AttributeError:
        pass
    
    factor_data = factor_data.div(100)
    factor_data.index.name = 'Date'
    save_daily_data(factor_data, out_path, "fama-french-factors")


def main():
    pass

if __name__ == "__main__":
    main()
