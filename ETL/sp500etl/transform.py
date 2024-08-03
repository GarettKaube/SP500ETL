import requests
import bs4 as bs
import datetime
import yfinance as yf
import pandas as pd
import os
import logging

from tqdm import tqdm

logger=logging.getLogger("etl")


def read_sp500_data(path):
    import glob
    files = glob.glob(f"{path}/*/*.parquet")
    dfs = [pd.read_parquet(file) for file in files]
    data = pd.concat(dfs, axis=0)
    data.index = pd.to_datetime(data.index)
    return data


def transform_sp500_data(input_path, output_path):
    data = read_sp500_data(input_path)
    data.index = pd.to_datetime(data.index)
    data = data.resample("M").last()
    data = data.stack().reset_index().set_index("Date")
    logger.info(f"Saving processed S&P500 data to {output_path}")

    years = data.index.year.unique()
    progress_bar = tqdm(total=len(years), desc=f"Saving data to {output_path}")

    for year_ in years:
        df = data.loc[str(year_)]
        os.makedirs(os.path.join(output_path, f"{year_}"), exist_ok=True)
        df.to_parquet(os.path.join(output_path, f"{year_}/sp500.parquet"), index=True)
        progress_bar.update(1)


def calculate_cum_return(input_path, output_path):
    data = read_sp500_data(input_path)
    data = data.resample("M").last()
    R = data['Close'].pct_change().dropna(how="all").fillna(0.00).add(1)
    R = R.cumprod().stack().reset_index().set_index("Date")
    R.rename({0:"Cum_return"}, axis=1, inplace=True)
    logger.info(f"Saving processed S&P500 return data to {output_path}")

    years = R.index.year.unique()
    progress_bar = tqdm(total=len(years), desc=f"Saving data to {output_path}")
    for year_ in years:
        df = R.loc[str(year_)]
        os.makedirs(os.path.join(output_path, f"{year_}"), exist_ok=True)
        df.to_parquet(
            os.path.join(output_path, f"{year_}/returns_sp500.parquet"), 
            index=True
        )
        progress_bar.update(1)


def main():
    pass


if __name__ == "__main__":
    main()