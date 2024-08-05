import requests
import bs4 as bs
import datetime
import yfinance as yf
import pandas as pd
import os
import logging
import glob

from tqdm import tqdm

logger=logging.getLogger("etl")


def read_sp500_data(path):
    import glob
    files = glob.glob(f"{path}/*/sp500*.parquet")
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


def join_fama_french_data(sp500_input_path, factor_input_path, output_path):
    sp500_data = glob.glob(f"{sp500_input_path}/*/sp500.parquet")
    sp500_data = pd.concat([pd.read_parquet(file) for file in sp500_data])

    factor_data = glob.glob(f"{factor_input_path}/*/fama-french-factors.parquet")
    factor_data = pd.concat([pd.read_parquet(file) for file in factor_data])
    factor_data = factor_data.resample('M').last().div(100)
    factor_data.index.name = 'Date'

    return_dfs = []
    for lag in [1,3,6,9,12]:
        groups = sp500_data.groupby("Ticker")[["Close"]].apply(
            lambda x: x.pct_change(lag)\
            .add(1)\
            .pow(1/lag)\
            .sub(1)
            )
        groups = groups.rename({"Close": f"Return_{lag}mo"}, axis=1)
        return_dfs.append(groups)
    
    returns = pd.concat(return_dfs, axis=1).reset_index()
    joined = returns.merge(sp500_data.reset_index(), on=["Date", "Ticker"])\
        .set_index('Date')\
        .join(factor_data)
    
    years = joined.index.year.unique()
    for year in years:
        save_data = joined.loc[f"{year}"]
        path = f"{output_path}/{year}"
        os.makedirs(path, exist_ok=True)
        save_data.to_parquet(path + "/sp500_and_fama-french-factors.parquet")


def main():
    pass


if __name__ == "__main__":
    main()