''' Import modules '''
from datetime import datetime, timedelta
import pandas as pd
import pandera as pa
from pandera import Column, Check
from dags.utils.aws import S3, S3Metadata
from dags.utils.config import Config
from dags.transformation.etl_transforms import EtlTransforms

def data_quality_checks():
    ''' Function that performs data quality checks on transformed heating oil spot prices dataset '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest and previous transformed data from S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('heating_oil_spot_prices', {}).get('latest_transformed_file_path')
    heating_oil_spot_prices_transformed_json = s3.get_data(s3_key=latest_transformed_file_path)
    heating_oil_spot_prices_transformed_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_transformed_json, date_as_index=False)
    
    previous_transformed_file_path = metadata.get('heating_oil_spot_prices', {}).get('previous_transformed_file_path')
    if previous_transformed_file_path is not None:
        heating_oil_spot_prices_previous_transformed_json = s3.get_data(s3_key=previous_transformed_file_path)
        heating_oil_spot_prices_previous_transformed_df = EtlTransforms.json_to_df(data=heating_oil_spot_prices_previous_transformed_json, date_as_index=False)
    else:
        heating_oil_spot_prices_previous_transformed_df = None

    # Retrieve start and end dates for data quality checks
    if heating_oil_spot_prices_previous_transformed_df is not None:
        start_date = heating_oil_spot_prices_previous_transformed_df['date'].iloc[0]
    else:
        start_date = heating_oil_spot_prices_transformed_df['date'].iloc[0]

    end_date = heating_oil_spot_prices_transformed_df['date'].iloc[-1]

    # Pandera schema for data quality checks
    schema = pa.DataFrameSchema(
    columns={
        "date": Column(str, nullable=False, checks=[Check(lambda s: pd.to_datetime(s).ge(start_date).all(), element_wise=False),
        Check(lambda s: pd.to_datetime(s).le(end_date).all(), element_wise=False),]),
        "price_heating_oil ($/GAL)": Column(float, nullable=False, checks=Check.ge(0)),
    },
    strict=True,
    unique=["date", "price_heating_oil ($/GAL)"])

    # Validate schema
    schema.validate(heating_oil_spot_prices_transformed_df)