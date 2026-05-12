# Import modules
from datetime import datetime
from dags.utils.config import Config
from dags.utils.aws import S3, S3Metadata
from dags.extraction.eia_api import EIA

def natural_gas_monthly_variables_extraction(**context):
    ''' Performs data extraction from EIA api for monthly natural gas variables '''
    # Timestamp of DAG execution
    ts_nodash = context["ts_nodash"]

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

    eia.extract(endpoint='natural-gas/sum/lsum/data/', headers=headers, put_object_s3_key=f'full_program/extraction/natural_gas_monthly_variables/natural_gas_monthly_variables_{ts_nodash}.json', 
    metadata_s3_key='full_program/metadata/metadata.json', dataset_key='natural_gas_monthly_variables', extract_timestamp=ts_nodash, is_monthly=True, start_date_if_none='1999-01-04')