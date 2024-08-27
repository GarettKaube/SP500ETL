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
    try:
        
        years = data.index.year.unique()
        months = data.index.month.unique()
    except AttributeError as e:
        years = data.index.get_level_values(1).year.unique()
        months = data.index.get_level_values(1).month.unique()

    n_files= len(years)

    progress_bar = tqdm(
        total=n_files, 
        desc=f"Saving data to {output_path}"
    )
    for year_ in years:
        path = os.path.join(output_path, f"{year_}")
        for month_ in months:
            try:
                df = data[(data.index.year == year_) & (data.index.month == month_)]
            except AttributeError:
                df = data[(data.index.get_level_values(1).year == year_) & (data.index.get_level_values(1).month == month_)]
            os.makedirs(path, exist_ok=True)
            df.to_parquet(os.path.join(output_path, 
                f"{year_}/{out_file_name}_{year_}_{month_}.parquet"), 
                index=True
            )
        progress_bar.update(1)