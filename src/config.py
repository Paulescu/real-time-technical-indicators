from src.feature_store_api import FeatureGroupConfig, FeatureViewConfig

WINDOW_SECONDS = 60
PRODUCT_IDS = [
    "BTC/USD",
    # "ETH/USD",
]

FEATURE_GROUP_METADATA = FeatureGroupConfig(
    name=f'ohlc_{WINDOW_SECONDS}_sec',
    version=1,
    description=f"OHLC data with technical indicators every {WINDOW_SECONDS} seconds",
    primary_key=[ 'product_id', 'time'],
    event_time='time',
    online_enabled=True,
)

FEATURE_VIEW_CONFIG = FeatureViewConfig(
    name=f'ohlc_{WINDOW_SECONDS}_sec_view',
    version=1,
    feature_group=FEATURE_GROUP_METADATA,
)
