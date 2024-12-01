''' Import modules '''
from datetime import datetime
from utils.config import *
from utils.aws import S3
from transformation.etl_transforms import EtlTransforms

def drop_columns():
    ''' Drop irrelevant columns from extracted natural gas monthly variables '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    natural_gas_monthly_variables_json = s3.get_data(folder='full_program/extraction/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=False)

    # Drop irrelevant columns from natural_gas_monthly_variables_df
    natural_gas_monthly_variables_df = EtlTransforms.drop_columns(df=natural_gas_monthly_variables_df, columns=['duoarea', 'area-name', 'product', 'product-name', 'process',
    'series', 'series-description', 'units'])
    
    # Put data in S3
    s3.put_data(data=natural_gas_monthly_variables_df, folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')