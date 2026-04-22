''' Import modules '''
from datetime import datetime
import pandas as pd
import pandera as pa
from pandera import Column, Check
from datetime import datetime
from dags.utils.config import *
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms

def data_quality_checks():
    ''' Function that performs data quality checks on transformed NOOA weather dataset '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest transformed filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('daily_weather', {}).get('latest_transformed_file_path')

    # Retrieve transformed data and imputed data from S3 folder
    daily_weather_json = s3.get_data(s3_key=latest_transformed_file_path)
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)

    # Retrieve start and end dates for data quality checks
    start_date = daily_weather_df['date'].iloc[0]
    end_date = daily_weather_df['date'].iloc[-1]

    # Required city values
    required_city_values = {
        "Orlando",
        "Miami",
        "Tampa",
        "New Orleans",
        "San Antonio",
        "Houston",
        "Philadelphia",
        "Jacksonville",
        "Austin",
        "Shreveport",
        "Dallas",
        "Baton Rouge",
        "St Louis",
        "Buffalo",
        "New York",
        "Cleveland",
        "Columbus",
        "Los Angeles",
        "San Diego",
        "Sacramento",
        "San Francisco",
        "Cincinnati",
        "Pittsburgh",
        "Chicago",
        "Detroit",
        "Grand Rapids"
    }

    required_state_values = {
        "Florida",
        "Louisiana",
        "Texas",
        "Pennsylvania",
        "Illinois",
        "New York",
        "Ohio",
        "California",
        "Michigan"
    }

    # Pandera schema for data quality checks
    schema = pa.DataFrameSchema(
    columns={
        "date": Column(str, nullable=False, checks=[Check(lambda s: pd.to_datetime(s).ge(start_date).all(), element_wise=False),
        Check(lambda s: pd.to_datetime(s).le(end_date).all(), element_wise=False),]),
        "city": Column(object, nullable=False),
        "state": Column(object, nullable=False), 
        "quarter": Column(int, nullable=False, checks=[Check.ge(1), Check.le(4)]),
        "awnd": Column(float, nullable=False, checks=Check.ge(0)),
        "snow": Column(float, nullable=False, checks=Check.ge(0)),
        "tavg": Column(float, nullable=False),
        "tmax": Column(float, nullable=False),
        "tmin": Column(float, nullable=False),
    },
    strict=True,
    unique=True)

    # Validate schema
    schema.validate(daily_weather_df)