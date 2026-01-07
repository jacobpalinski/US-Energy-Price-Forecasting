''' Import modules '''
from datetime import datetime, timedelta
import pandas as pd
import pandera as pa
from pandera import Column, Check
from dags.utils.aws import S3
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def data_quality_checks():
    ''' Function that performs data quality checks on transformed natural gas monthly variables dataset '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Previous natural gas monthly variables date
    previous = datetime.now() - timedelta(days=7)
    formatted_previous_date = previous.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Import current and previous datasets
    natural_gas_monthly_variables_json_current = s3.get_data(folder='full_program/transformation/natural_gas_monthly_variables/', object_key=f'natural_gas_monthly_variables_{formatted_date}')
    natural_gas_monthly_variables_df_current = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json_current, date_as_index=True)
    
    natural_gas_monthly_variables_json_previous = s3.get_data(folder='full_program/transformation/heating_oil_spot_prices/', object_key=f'natural_gas_monthly_variables_{formatted_previous_date}')

    # Retrieve start and end dates for data quality checks
    if natural_gas_monthly_variables_json_previous:
        natural_gas_monthly_variables_df_previous = EtlTransforms.json_to_df(data=natural_gas_monthly_variables_json_previous, date_as_index=True)
        start_date = natural_gas_monthly_variables_df_previous['date'].iloc[0]
    else:
        start_date = natural_gas_monthly_variables_df_current['date'].iloc[0]

    end_date = natural_gas_monthly_variables_df_current['date'].iloc[-1]

    # Pandera schema for data quality checks
    schema = pa.DataFrameSchema(
    columns={
        "date": Column(str, nullable=False, checks=[Check(lambda s: pd.to_datetime(s).ge(start_date).all(), element_wise=False),
        Check(lambda s: pd.to_datetime(s).le(end_date).all(), element_wise=False),]),
        "commercial_consumption": Column(int, nullable=False, checks=Check.ge(0)),
        "residential_consumption": Column(int, nullable=False, checks=Check.ge(0)),
        "imports": Column(int, nullable=False, checks=Check.ge(0)),
        "lng_imports": Column(int, nullable=False, checks=Check.ge(0)),
        "total_underground_storage": Column(int, nullable=False, checks=Check.ge(0))
    },
    strict=True)

    # Validate schema
    schema.validate(natural_gas_monthly_variables_df_current)