# Import modules
from datetime import datetime
from dags.utils.config import Config
from dags.utils.aws import S3, S3Metadata
from dags.extraction.eia_api import EIA

def natural_gas_spot_prices_extraction(**context):
    ''' Performs data extraction from EIA api for natural gas spot prices '''
    # Timestamp of DAG execution
    ts_nodash = context["ts_nodash"]

    # Instantiate classes for Config, S3, S3Metadata and EIA
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)
    eia = EIA(config=config, s3=s3, s3_metadata=s3_metadata)

    # Extract natural gas spot prices data from EIA API
    headers = {
    'api_key': eia.eia_api_key,
    'frequency': 'daily',
    'data': ['value'],
    'facets': {
        'series': ['RNGWHHD',]
    },
    'sort': [{
        'column': 'period',
        'direction': 'asc'
    }],
    'length': 5000
    }

    eia.extract(endpoint='natural-gas/pri/fut/data/', headers=headers, put_object_s3_key=f'full_program/extraction/natural_gas_spot_prices/natural_gas_spot_prices_{ts_nodash}.json',
    metadata_s3_key='full_program/metadata/metadata.json', dataset_key='natural_gas_spot_prices', extract_timestamp=ts_nodash, is_monthly=False, start_date_if_none='1999-01-04')