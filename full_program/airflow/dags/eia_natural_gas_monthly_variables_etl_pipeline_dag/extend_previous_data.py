''' Import modules '''
from datetime import datetime, timedelta
import pandas as pd
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def extend_previous_data():
    ''' Function that concatenates previous natural gas monthly variables transformed dataset with current dataset '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Previous natural gas monthly
    previous = datetime.now() - timedelta(days=7)
    formatted_previous_date = previous.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Import previous and current datasets
    natural_gas_monthly_variables_json_current = s3.get_data(folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')
    natural_gas_monthly_variables_json_previous = s3.get_data(folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_previous_date}')
    natural_gas_monthly_variables_df_current = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json_current, date_as_index=True)
    natural_gas_monthly_variables_df_previous = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json_previous, date_as_index=True)

    # Combine previous and current datasets
    natural_gas_monthly_variables_df = pd.concat([natural_gas_monthly_variables_df_previous, natural_gas_monthly_variables_df_current])

    # Reset index
    natural_gas_monthly_variables_df = natural_gas_monthly_variables_df.reset_index()

    # Convert date from timestamp to string
    natural_gas_monthly_variables_df['date'] = natural_gas_monthly_variables_df['date'].dt.strftime('%Y-%m-%d')

    # Put data in S3
    s3.put_data(data=natural_gas_monthly_variables_df, folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')

