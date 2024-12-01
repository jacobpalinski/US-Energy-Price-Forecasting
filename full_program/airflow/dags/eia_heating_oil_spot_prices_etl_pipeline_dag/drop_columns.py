''' Import modules '''
from datetime import datetime
from extraction.eia_api import *
from transformation.etl_transforms import EtlTransforms

def drop_columns():
    ''' Drop irrelevant columns from extracted heating oil spot prices '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    heating_oil_spot_prices_json = s3.get_data(folder='full_program/extraction/heating_oil_spot_prices/', object_key=f'heating_oil_spot_prices_{formatted_date}')
    heating_oil_spot_prices_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_json, date_as_index=False)

    # Drop irrelevant columns from heating_oil_spot_prices_df
    heating_oil_spot_prices_df = EtlTransforms.drop_columns(df=heating_oil_spot_prices_df, columns=['duoarea', 'area-name', 'product', 'product-name', 'process',
    'process-name', 'series', 'series-description', 'units'])
    
    # Put data in S3
    s3.put_data(data=heating_oil_spot_prices_df, folder='full_program/transformation/heating_oil_spot_prices/', object_key=f'heating_oil_spot_prices_{formatted_date}')