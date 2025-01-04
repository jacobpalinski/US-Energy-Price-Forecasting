''' Import modules '''
from datetime import datetime
from dags.utils.config import *
from dags.extraction.eia_api import *

def natural_gas_monthly_variables_extraction():
    ''' Performs data extraction from EIA api for monthly natural gas variables '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3, S3Metadata and EIA
    config = Config()
    s3 = S3(config=config)
    s3_metadata = S3Metadata(config=config)
    eia = EIA(config=config, s3=s3, s3_metadata=s3_metadata)

    # Extract natural gas monthly variables from EIA API
    headers = {
    'api_key': eia.eia_api_key,
    'frequency': 'monthly',
    'data': ['value'],
    'facets': {
        'duoarea': [
            'NUS',
            'NUS-Z00'
        ],
        'series': ['N3010US2',
            'N3020US2',
            'N5030US2',
            'N9100US2',
            'N9103US2']
    },
    'sort': [{
        'column': 'period',
        'direction': 'asc'
    }],
    'length': 5000
    }

    eia.extract(endpoint='natural-gas/sum/lsum/data/', headers=headers, folder='full_program/extraction/natural_gas_monthly_variables/',
    object_key=f'natural_gas_monthly_variables_{formatted_date}', metadata_folder='full_program/metadata/', metadata_object_key='metadata', 
    metadata_dataset_key='natural_gas_monthly_variables', is_monthly=True, start_date_if_none='1999-01-04')