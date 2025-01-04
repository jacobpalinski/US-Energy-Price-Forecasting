''' Import modules '''
from datetime import datetime
from dags.extraction.eia_api import *

def heating_oil_spot_prices_extraction():
    ''' Performs data extraction from EIA api for heating oil spot prices '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

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

    eia.extract(endpoint='petroleum/pri/spt/data/', headers=headers, folder='full_program/extraction/heating_oil_spot_prices/',
    object_key=f'heating_oil_spot_prices_{formatted_date}', metadata_folder='full_program/metadata/', metadata_object_key='metadata', 
    metadata_dataset_key='heating_oil_spot_prices', is_monthly=False, start_date_if_none='1999-01-04')