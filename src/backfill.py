from datetime import datetime, timedelta, date
from argparse import ArgumentParser
from typing import List

import pytz
import requests
import pandas as pd
from prefect import task, flow
from prefect.task_runners import SequentialTaskRunner

from src.feature_store_api import save_data_to_offline_feature_group
from src import config
from src.logger import get_console_logger

logger = get_console_logger()

PAIR = 'xbtusd'
OHLC_FREQUENCY_IN_MINUTES = 1

@task(retries=3, retry_delay_seconds=60)
def fetch_data_from_kraken_api(
    product_id: str,
    since_nano_seconds: int
) -> List[List]:
    """
    Fetches data from Kraken API for the given `pair` from `since_nano_seconds`

    Args:
        pair (str): currency pair we fetch data for
        last_ts (int): timestamp in seconds

    Returns:
        dict: response from Kraken API
    """
    MAP_PRODUCT_ID_TO_HISTORICAL_API_PAIR_NAME = {
        'XBT/USD': 'xbtusd',
    }
    pair = MAP_PRODUCT_ID_TO_HISTORICAL_API_PAIR_NAME[product_id]

    # build URL we need to fetch data from
    url = f"https://api.kraken.com/0/public/Trades?pair={pair}&since={since_nano_seconds:.0f}"

    # fetch data
    response = requests.get(url)

    # extract data from response
    MAP_PRODUCT_ID_TO_HISTORIAL_API_RESPONSE_KEY = {
        'XBT/USD': 'XXBTZUSD'
    }
    response_key = MAP_PRODUCT_ID_TO_HISTORIAL_API_RESPONSE_KEY[product_id]
    trade_params = response.json()["result"][response_key]

    return trade_params

@flow
def fetch_historical_data_one_day(day: datetime, product_id: str) -> pd.DataFrame:
    """"""
    # time range we want to fetch data for
    from_ts = day.timestamp()
    to_ts = (day + timedelta(days=1)).timestamp()
    # to_ts = (day + timedelta(hours=1)).timestamp()

    trades = []
    last_ts = from_ts
    while last_ts < to_ts:  
        
        # fetch data from Kraken API
        trade_params = fetch_data_from_kraken_api(
            product_id,
            since_nano_seconds=last_ts*1000000000
        )

        # breakpoint()

        # create a list of Dict with the results
        trades_in_batch = [
            {
                'price': params[0],
                'volume': params[1],
                'timestamp': params[2],
                'product_id': product_id,
            }
            for params in trade_params
        ]

        # breakpoint()

        # # checking timestamps
        from_date = datetime.utcfromtimestamp(trades_in_batch[0]['timestamp'])
        to_date = datetime.utcfromtimestamp(trades_in_batch[-1]['timestamp'])
        logger.info(f'trades_in_batch from {from_date} to {to_date}')

        # add trades to list of trades
        trades += trades_in_batch

        # update last_ts for the next iteration
        last_ts = trades[-1]['timestamp']

        # TODO: remove this break. It is here to speed up the debugging process
        # break

    # drop trades that might fall outside of the time window
    trades = [t for t in trades if t['timestamp'] >= from_ts and t['timestamp'] <= to_ts]
    # logger.info(f'Fetched trade data from {trades[0]["timestamp"]} to {trades[-1]["timestamp"]}')
    logger.info(f'{len(trades)=}')

    # # convert trades to pandas dataframe
    trades = pd.DataFrame(trades)

    # # set correct dtypes
    trades['price'] = trades['price'].astype(float)
    trades['volume'] = trades['volume'].astype(float)
    trades['timestamp'] = trades['timestamp'].astype(int)
    trades['product_id'] = trades['product_id'].astype(str)

    return trades

def save_data_to_csv_file(data: pd.DataFrame) -> str:
    """Saves data to a temporary csv file"""
    import time
    tmp_file_path = f'/tmp/{int(time.time())}.csv'
    data.to_csv(tmp_file_path)
    return tmp_file_path

@task
def transform_trades_to_ohlc(trades: pd.DataFrame) -> pd.DataFrame:
    """Transforms raw trades to OHLC data"""
    
    # convert ts column to pd.Timestamp
    # trades.index = pd.to_datetime(trades['ts'], unit='s')

    # logger.info('Saving trades to temporary file')
    file_path = save_data_to_csv_file(trades)
    logger.info(f'Saved trades to temporary file {file_path}')

    # breakpoint()

    logger.info('Creating dataflow for backfilling and running it')
    from src.dataflow import run_dataflow_in_backfill_mode
    ohlc = run_dataflow_in_backfill_mode(input_file=file_path)

    # breakpoint()

    # remove temporary file
    logger.info(f'Removing temporary file {file_path}')
    import os
    os.remove(file_path)

    return ohlc

@task(retries=3, retry_delay_seconds=60)
def save_ohlc_to_feature_store(ohlc: pd.DataFrame) -> None:
    """Saves OHLC data to the feature store"""
    # save OHLC data to offline feature store
    logger.info('Saving OHLC data to offline feature store')
    save_data_to_offline_feature_group(
        data=ohlc,
        feature_group_config=config.FEATURE_GROUP_METADATA
    )

@flow(task_runner=SequentialTaskRunner())
def backfill_one_day(day: datetime, product_id: str):
    """Backfills OHLC data in the feature store for the given `day`"""
    
    # fetch trade data from external API
    trades: pd.DataFrame = fetch_historical_data_one_day(day, product_id)
    
    # transform trade data to OHLC data
    ohlc: pd.DataFrame = transform_trades_to_ohlc(trades)

    # push OHLC data to the feature store
    logger.info(f'Pushing OHLC data to offline feature group, {day=} {product_id=}')
    save_ohlc_to_feature_store(ohlc)

@flow
def backfill_range_dates(from_day: datetime, to_day: datetime, product_id: str):
    """
    Backfills OHLC data in the feature store for the ranges of days
    between `from_day` and `to_day`
    """
    days = pd.date_range(from_day, to_day, freq='D')
    for day in days:
        backfill_one_day(day, product_id=product_id)


if __name__ == "__main__":
    
    parser = ArgumentParser()
    parser.add_argument(
        '--from_day',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    )
    parser.add_argument(
        '--to_day',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=pytz.UTC),
        default=datetime.utcnow().replace(tzinfo=pytz.UTC)
    )
    parser.add_argument(
        '--day',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    )

    parser.add_argument(
        '--product_id', type=str,
        help="For example: XBT/USD",
    )

    args = parser.parse_args()

    if args.day:
        logger.info(f'Starting backfilling process for {args.day}')
        backfill_one_day(day=args.day, product_id=args.product_id)
    
    else:
        assert args.from_day <= args.to_day, "from_day has to be smaller or equal than to_day"
        logger.info(f'Starting backfilling process from {args.from_day} to {args.to_day}')
        backfill_range_dates(from_day=args.from_day, to_day=args.to_day, product_id=args.product_id)