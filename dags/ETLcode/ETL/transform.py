import pandas as pd
import os
import logging
import glob
from tqdm import tqdm
try:
    from ETLcode.ETL.utils import save_daily_data
except ModuleNotFoundError:
    from utils import save_daily_data

logger = logging.getLogger("etl")


def read_sp500_data(path=None, fullpath=None):
    if path is not None or fullpath is not None:
        files = glob.glob(
            f"{path}/*/sp500*.parquet"
        ) if path else glob.glob(fullpath)

        dfs = [pd.read_parquet(file) for file in files]
        data = pd.concat(dfs, axis=0)
        data.index = pd.to_datetime(data.index)
        return data


def from_prices_to_returns(data, lags: list):
    return_dfs = []
    for lag in lags:
        groups = data.groupby("Ticker")[["Close"]].apply(
            lambda x: x.pct_change(lag)
            .add(1)
            .pow(1/lag)
            .sub(1)
            )
        groups = groups.rename({"Close": f"Return_{lag}"}, axis=1)
        return_dfs.append(groups)

    returns = pd.concat(return_dfs, axis=1).reset_index()
    return returns


def transform_sp500_data(input_path, output_path):
    data = read_sp500_data(input_path)
    data.index = pd.to_datetime(data.index)
    data = data.stack().reset_index().set_index("Date")
    logger.info(f"Saving processed S&P500 data to {output_path}")
    save_daily_data(data, output_path, "sp500")


def calculate_daily_returns(input_path, output_path):
    data = read_sp500_data(
        path=None,
        fullpath=f"{input_path}/*/sp500_2*.parquet"
    )
    returns = from_prices_to_returns(data, lags=[1, 5, 10, 21, 42, 63])
    joined = returns.merge(data.reset_index(), on=["Date", "Ticker"])\
        .set_index('Date')
    save_daily_data(joined, output_path, "daily_sp500_returns")


def join_fama_french_data(sp500_input_path, factor_input_path, output_path):
    sp500_data = glob.glob(f"{sp500_input_path}/*/daily*.parquet")
    sp500_data = pd.concat([pd.read_parquet(file) for file in sp500_data])

    factor_data = glob.glob(
        f"{factor_input_path}/*/fama-french-factors_*.parquet"
    )
    factor_data = pd.concat(
        [pd.read_parquet(file) for file in factor_data]
    )
    factor_data = factor_data.div(100)
    factor_data.index.name = 'Date'

    joined = sp500_data.join(factor_data)

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
