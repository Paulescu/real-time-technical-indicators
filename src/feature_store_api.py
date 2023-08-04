import os
from typing import List
from dataclasses import dataclass

import hsfs
import hopsworks
import pandas as pd

from src.logger import get_console_logger

logger = get_console_logger()

# load Feature Store credentials from environment variables
HOPSWORKS_PROJECT_NAME = os.environ['HOPSWORKS_PROJECT_NAME']
HOPSWORKS_API_KEY = os.environ['HOPSWORKS_API_KEY']

@dataclass
class FeatureGroupConfig:
    name: str
    version: int
    description: str
    primary_key: List[str]
    event_time: str
    online_enabled: bool

@dataclass
class FeatureViewConfig:
    name: str
    version: int
    feature_group: FeatureGroupConfig


def get_feature_store() -> hsfs.feature_store.FeatureStore:
    """Connects to Hopsworks and returns a pointer to the feature store

    Returns:
        hsfs.feature_store.FeatureStore: pointer to the feature store
    """
    project = hopsworks.login(
        project=HOPSWORKS_PROJECT_NAME,
        api_key_value=HOPSWORKS_API_KEY
    )
    return project.get_feature_store()


def get_feature_group(
    feature_group_metadata: FeatureGroupConfig
) -> hsfs.feature_group.FeatureGroup:
    """Connects to the feature store and returns a pointer to the given
    feature group `name`

    Args:
        name (str): name of the feature group
        version (Optional[int], optional): _description_. Defaults to 1.

    Returns:
        hsfs.feature_group.FeatureGroup: pointer to the feature group
    """
    return get_feature_store().get_or_create_feature_group(
        name=feature_group_metadata.name,
        version=feature_group_metadata.version,
        description=feature_group_metadata.description,
        primary_key=feature_group_metadata.primary_key,
        event_time=feature_group_metadata.event_time,
        online_enabled=feature_group_metadata.online_enabled
    )


def get_or_create_feature_view(
    feature_view_metadata: FeatureViewConfig
) -> hsfs.feature_view.FeatureView:
    """"""

    # get pointer to the feature store
    feature_store = get_feature_store()

    # get pointer to the feature group
    # from src.config import FEATURE_GROUP_METADATA
    feature_group = feature_store.get_feature_group(
        name=feature_view_metadata.feature_group.name,
        version=feature_view_metadata.feature_group.version
    )

    # create feature view if it doesn't exist
    try:
        feature_store.create_feature_view(
            name=feature_view_metadata.name,
            version=feature_view_metadata.version,
            query=feature_group.select_all()
        )
    except:
        # logger.info("Feature view already exists, skipping creation.")
        logger.info("Feature view already exists, skipping creation.")
    
    # get feature view
    feature_store = get_feature_store()
    feature_view = feature_store.get_feature_view(
        name=feature_view_metadata.name,
        version=feature_view_metadata.version,
    )

    return feature_view
    
# from prefect import task
# @task(retries=3, retry_delay_seconds=60)
def save_data_to_offline_feature_group(
    data: pd.DataFrame,
    feature_group_config: FeatureGroupConfig
) -> None:
    """"""
    feature_group = get_feature_group(feature_group_config)
    feature_group.insert(data, write_options={"wait_for_job": False})