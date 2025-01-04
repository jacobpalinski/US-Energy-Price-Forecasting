''' Import modules '''
from datetime import datetime
from dags.utils.config import *
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms

def pivot_data():
    ''' Pivot data from extracted natural gas rigs in operation '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    natural_gas_rigs_in_operation_json = s3.get_data(folder='full_program/transformation/natural_gas_rigs_in_operation/', object_key=f'natural_gas_rigs_in_operation_{formatted_date}')
    natural_gas_rigs_in_operation_df = EtlTransforms.json_to_df(data=natural_gas_rigs_in_operation_json, date_as_index=False)

    # Pivot columns
    natural_gas_rigs_in_operation_df = EtlTransforms.pivot_columns(df=natural_gas_rigs_in_operation_df, index=['period'],
    column='process-name', value='value')
    
    # Put data in S3
    s3.put_data(data=natural_gas_rigs_in_operation_df, folder='full_program/transformation/natural_gas_rigs_in_operation/', object_key=f'natural_gas_rigs_in_operation_{formatted_date}')