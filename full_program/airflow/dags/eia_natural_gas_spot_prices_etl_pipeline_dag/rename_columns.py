''' Import modules '''
from datetime import datetime
from dags.extraction.eia_api import *
from dags.transformation.etl_transforms import EtlTransforms

def rename_columns():
    ''' Rename columns from extracted natural gas spot prices '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    natural_gas_spot_prices_json = s3.get_data(folder='full_program/transformation/natural_gas_spot_prices/', object_key=f'natural_gas_spot_prices_{formatted_date}')
    natural_gas_spot_prices_df = EtlTransforms.json_to_df(data=natural_gas_spot_prices_json, date_as_index=False)

    # Rename value and period columns from natural_gas_spot_prices_df
    natural_gas_spot_prices_df = EtlTransforms.rename_columns(df=natural_gas_spot_prices_df, renamed_columns={'value': 'price ($/MMBTU)', 'period': 'date'})
    
    # Put data in S3
    s3.put_data(data=natural_gas_spot_prices_df, folder='full_program/transformation/natural_gas_spot_prices/', object_key=f'natural_gas_spot_prices_{formatted_date}')