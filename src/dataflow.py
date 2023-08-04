from pathlib import Path
from typing import Optional, List, Any

import pandas as pd
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutput
from bytewax.testing import run_main

from src import config
from src.flow_steps import (
    connect_to_kraken_websocket,
    connect_to_input_csv_file,
    add_tumbling_window,
    aggregate_raw_trades_as_ohlc,
    tuple_to_dict,
    save_output_to_online_feature_store,
    save_output_to_list,
)
from src.logger import get_console_logger

logger = get_console_logger()


def run_dataflow_in_backfill_mode(input_file: str) -> pd.DataFrame:
    """"""
    logger.info('Building dataflow in BACKFILL mode')
    ohlc = []

    flow = get_dataflow(
        execution_mode='BACKFILL',
        backfill_input_file=input_file,
        backfill_output=ohlc
    )

    logger.info('Running dataflow in BACKFILL mode')
    run_main(flow)

    return pd.DataFrame(ohlc)


def get_dataflow(
    execution_mode: Optional[str] = 'LIVE',
    backfill_input_file: Optional[Path] = None,
    backfill_output: List[Any] = None,
) -> Dataflow:
    """Constructs and returns a ByteWax Dataflow

    Args:
        window_seconds (int)

    Returns:
        Dataflow:
    """
    window_seconds = config.WINDOW_SECONDS
    assert execution_mode in ['LIVE', 'BACKFILL', 'DEBUG']

    flow = Dataflow()

    # connect dataflow to an input, either websocket or local file
    if execution_mode == 'BACKFILL':
        logger.info('BACKFILL MODE ON!')
        logger.info('Adding connector to local file to backfill')

        if not backfill_input_file:
            raise Exception('You need to provide `backfill_input_file` when your execution_mode is BACKFILL')

        connect_to_input_csv_file(flow, backfill_input_file)
    else:
        logger.info('Adding connector to input socket')
        connect_to_kraken_websocket(flow)

    # add tumbling window
    add_tumbling_window(flow, window_seconds)
    
    # reduce multiple trades into a single OHLC
    aggregate_raw_trades_as_ohlc(flow)
    
    tuple_to_dict(flow)
    
    # send output to stdout, online feature store or in-memory list
    if execution_mode == 'DEBUG':
        logger.info('Dataflow in debug mode. Output to stdout')
        flow.output("output", StdOutput())
    
    elif execution_mode == 'LIVE':
        from src.config import FEATURE_GROUP_METADATA
        save_output_to_online_feature_store(flow, FEATURE_GROUP_METADATA)
    
    elif execution_mode == 'BACKFILL':
        logger.info('Pushing data to Offline Feature Store')
        save_output_to_list(flow, backfill_output)

    return flow