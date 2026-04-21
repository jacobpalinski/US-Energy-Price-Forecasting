''' Import modules '''
from datetime import datetime
from dags.extraction.eia_api import *

def heating_oil_spot_prices_extraction(**context):
    ''' Performs data extraction from EIA api for heating oil spot prices '''
    ts_nodash = context["ts_nodash"]

    # Instantiate classes for Config, S3, S3Metadata and EIA
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)
    eia = EIA(config=config, s3=s3, s3_metadata=s3_metadata)

    # Extract heating oil spot prices data from EIA API
    headers = {
    'api_key': eia.eia_api_key,
    'frequency': 'daily',
    'data': ['value'],
    'facets': {
        'series': ['EER_EPD2F_PF4_Y35NY_DPG',]
    },
    'sort': [{
        'column': 'period',
        'direction': 'asc'
    }],
    'length': 5000
    }

    eia.extract(endpoint='petroleum/pri/spt/data/', headers=headers, put_object_s3_key=f'full_program/extraction/heating_oil_spot_prices/heating_oil_spot_prices_{ts_nodash}.json',
    metadata_s3_key='full_program/metadata/metadata.json', dataset_key='heating_oil_spot_prices', extract_timestamp=ts_nodash, is_monthly=False, start_date_if_none='1999-01-04')