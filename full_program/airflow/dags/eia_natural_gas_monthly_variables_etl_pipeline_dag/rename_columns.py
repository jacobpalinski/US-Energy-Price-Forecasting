''' Import modules '''
from datetime import datetime
from dags.utils.config import *
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms

def rename_columns():
    ''' Rename columns from extracted natural gas monthly variables '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    natural_gas_monthly_variables_json = s3.get_data(folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')
    natural_gas_monthly_variables_df = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json, date_as_index=False)

    # Rename pivoted columns
    natural_gas_monthly_variables_df = EtlTransforms.rename_columns(df=natural_gas_monthly_variables_df, renamed_columns={'Commercial Consumption': 'commercial_consumption', 
    'Imports': 'imports', 'Liquefied Natural Gas Imports': 'lng_imports', 'Residential Consumption': 'residential_consumption', 'Total Underground Storage': 'total_underground_storage',  
    'period': 'date'})
    
    # Put data in S3
    s3.put_data(data=natural_gas_monthly_variables_df, folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')