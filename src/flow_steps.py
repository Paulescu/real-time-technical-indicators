from typing import Tuple, Dict, List, Generator, Any, Optional, Callable
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from websocket import create_connection
from bytewax.inputs import DynamicInput, StatelessSource
from bytewax.connectors.files import FileInput, CSVInput
from bytewax.dataflow import Dataflow
from bytewax.window import EventClockConfig, TumblingWindow #Config
from bytewax.outputs import StatelessSink, DynamicOutput

from src.config import PRODUCT_IDS, WINDOW_SECONDS
from src.feature_store_api import FeatureGroupConfig, get_feature_group
from src.date_utils import epoch2datetime
from src.logger import get_console_logger

logger = get_console_logger()

def connect_to_input_csv_file(flow: Dataflow, input_file: Path) -> Dataflow:

    # connect to input file
    flow.input("input", CSVInput(input_file))

    # extract product_id as key and trades as list of dicts
    def extract_key_and_trades(data: Dict) -> Tuple[str, List[Dict]]:
        """"""
        product_id = data['product_id']
        trades = [
            {
                'price': float(data['price']),
                'volume': float(data['volume']),
                'timestamp': float(data['timestamp']),
            }
        ]
        return (product_id, trades)
    
    flow.map(extract_key_and_trades)

    return flow

def connect_to_kraken_websocket(flow: Dataflow) -> Dataflow:
    """"""
    connect_to_input_socket(flow)
    format_websocket_event(flow)

    return flow

def connect_to_input_socket(flow: Dataflow) -> Dataflow:
    """Connects the given dataflow to the Coinbase websocket
    
    Args:
        flow (Dataflow): _description_
    """
    class KrakenSource(StatelessSource):
        def __init__(self, product_ids):

            self.product_ids = product_ids

            self.ws = create_connection("wss://ws.kraken.com/")
            self.ws.send(
                json.dumps(
                    {
                        "event": "subscribe",
                        "subscription": {"name":"trade"},
                        "pair": product_ids,
                    }
                )
            )
            # The first msg is just a confirmation that we have subscribed.
            logger.info(f'First message from websocket: {self.ws.recv()}')

        def next(self) -> Optional[Any]:

            event = self.ws.recv()

            if 'heartbeat' in event:
                # logger.info(f'Heartbeat event: {event}')
                return None

            if 'channelID' in event:
                # logger.info(f'Subscription event: {event}')
                return None

            # logger.info(f'{event=}')
            return event

    class KrakenInput(DynamicInput):

        def build(self, worker_index, worker_count):
            prods_per_worker = int(len(PRODUCT_IDS) / worker_count)
            product_ids = PRODUCT_IDS[
                int(worker_index * prods_per_worker) : int(
                    worker_index * prods_per_worker + prods_per_worker
                )
            ]
            return KrakenSource(product_ids)
        
    flow.input("input", KrakenInput())

    return flow

def format_websocket_event(flow: Dataflow) -> Dataflow:

    # string to json
    flow.map(json.loads)

    # extract product_id as key and trades as list of dicts
    def extract_key_and_trades(data: Dict) -> Tuple[str, List[Dict]]:
        """"""
        product_id = data[3]
        trades = [
            {
                'price': float(t[0]),
                'volume': float(t[1]),
                'timestamp': float(t[2]),
            }
            for t in data[1]
        ]
        return (product_id, trades)

    flow.map(extract_key_and_trades)
    
    return flow

def add_tumbling_window(flow: Dataflow, window_seconds: int) -> Dataflow:

    def get_event_time(trades: List[Dict]) -> datetime:
        """
        This function instructs the event clock on how to retrieve the
        event's datetime from the input.
        """
        # use timestamp from the first trade in the list
        return epoch2datetime(trades[0]['timestamp'])

    def build_array() -> np.array:
        """_summary_

        Returns:
            np.array: _description_
        """
        return np.empty((0,3))

    def acc_values(previous_data: np.array, trades: List[Dict]) -> np.array:
        """
        This is the accumulator function, and outputs a numpy array of time and price
        """
        # TODO: fix this to add all trades, not just the first in the event
        return np.insert(previous_data, 0,
                        np.array((trades[0]['timestamp'], trades[0]['price'], trades[0]['volume'])), 0)


    # Configure the `fold_window` operator to use the event time
    clock_config = EventClockConfig(
        get_event_time,
        wait_for_system_duration=timedelta(seconds=0)
    )

    window_config = TumblingWindow(length=timedelta(seconds=window_seconds),
                                   align_to=datetime(2023, 4, 3, 0, 0, 0, tzinfo=timezone.utc))

    flow.fold_window(f"{window_seconds}_sec",
                     clock_config,
                     window_config,
                     build_array,
                     acc_values)
    return flow

def aggregate_raw_trades_as_ohlc(flow: Dataflow) -> Dataflow:

    # compute OHLC for the window
    def calculate_features(ticker_data: Tuple[str, np.array]) -> Tuple[str, Dict]:
        """Aggregate trade data in window

        Args:
            ticker__data (Tuple[str, np.array]): product_id, data

        Returns:
            Tuple[str, Dict]: product_id, Dict with keys
                - time
                - open
                - high
                - low
                - close
                - volume
        """
        def round_timestamp(timestamp_seconds: int) -> int:
            import math
            return int(math.floor(timestamp_seconds / WINDOW_SECONDS * 1.0)) * WINDOW_SECONDS
        
        ticker, data = ticker_data
        ohlc = {
            "time": round_timestamp(data[-1][0]),
            # "time": data[-1][0],
            "open": data[:,1][-1],
            "high": np.amax(data[:,1]),
            "low": np.amin(data[:,1]),
            "close": data[:,1][0],  
            "volume": np.sum(data[:,2])
        }
        return (ticker, ohlc)

    flow.map(calculate_features)
    return flow

def tuple_to_dict(flow: Dataflow) -> Dataflow:

    def _tuple_to_dict(key__dict: Tuple[str, Dict]) -> Dict:
        key, dict = key__dict
        dict['product_id'] = key

        # TODO: fix this upstream
        dict['time'] = int(dict['time'])

        return dict
     
    flow.map(_tuple_to_dict)
    return flow

def save_output_to_list(flow: Dataflow, output: List[Any]) -> Dataflow:
    """Saves output to a list"""
    from bytewax.testing import TestingOutput
    flow.output("output", TestingOutput(output))
    return flow

def save_output_to_online_feature_store(
    flow: Dataflow,
    feature_group_metadata: FeatureGroupConfig
) -> Dataflow:
    class HopsworksSink(StatelessSink):
        def __init__(self, feature_group):
            self.feature_group = feature_group

        def write(self, item):
            df = pd.DataFrame.from_records([item])
            logger.info(f'Saving {item} to online feature store...')
            self.feature_group.insert(df, write_options={"start_offline_backfill": False})

    class HopsworksOutput(DynamicOutput):
        def __init__(self, feature_group_metadata):
            self.feature_group = get_feature_group(feature_group_metadata)

        def build(self, worker_index, worker_count):
            return HopsworksSink(self.feature_group)

    flow.output("hopsworks", HopsworksOutput(feature_group_metadata))
    return flow