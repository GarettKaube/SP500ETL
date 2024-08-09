import requests
import bs4 as bs
import datetime
import yfinance as yf
import pandas as pd
import os
import logging
from tqdm import tqdm
import pandas_datareader.data as web

def save_daily_data(data, output_path, out_file_name):
    progress_bar = tqdm(
        total=len(data.index.year.unique()), 
        desc=f"Saving data to {output_path}"
    )
    for year_ in data.index.year.unique():
        path = os.path.join(output_path, f"{year_}")
        for month_ in data.index.month.unique():
            df = data[(data.index.year == year_) & (data.index.month == month_)]
            os.makedirs(path, exist_ok=True)
            df.to_parquet(os.path.join(output_path, 
                f"{year_}/{out_file_name}_{year_}_{month_}.parquet"), 
                index=True
            )
        progress_bar.update(1)