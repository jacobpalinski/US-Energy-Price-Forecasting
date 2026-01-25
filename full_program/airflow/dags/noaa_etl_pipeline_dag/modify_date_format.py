''' Import modules '''
from datetime import datetime
import pandera as pa
from pandera import Column, Check
import pandas as pd
from dags.extraction.noaa_api import *
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.noaa_api_transformation import NoaaTransformation
from dags.utils.data_quality_check_functions import DataQualityChecks

def modify_date_format():
    ''' Modify date format of extracted NOAA weather data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    daily_weather_json = s3.get_data(folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)

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

    # Perform data quality checks on key columns 
    schema = pa.DataFrameSchema(
    columns = {
        "value": Column(
            object,
            checks=Check(DataQualityChecks.is_numeric_or_null, element_wise=True),
            nullable=True,
        ),
        "date": Column(
            datetime,
            checks=[Check(lambda s: s.str.slice(0,10).str.match(r"\d{4}-\d{2}-\d{2}"), element_wise=False),
            Check(lambda s: pd.to_datetime(s, errors="coerce").notna(), element_wise=False)],
            nullable=False,
        ),
        "city": Column(
            object,
            checks=Check(lambda s: required_city_values.issubset(set(s.dropna())), element_wise=False, error=f"city column must contain the following values {required_city_values}"),
            nullable=False,
        ),
        "state": Column(
            object,
            checks=Check(lambda s: required_state_values.issubset(set(s.dropna())), element_wise=False, error=f"state column must contain the following values {required_state_values}"),
            nullable=False
        ),
        "datatype": Column(
            object,
            checks=Check(lambda s: set(s.dropna()).issubset({"TMIN", "TMAX", "TAVG", "SNOW", "AWND"}), element_wise=False, error="datatype column must only contain TMIN, TMAX, TAVG, SNOW, AWND"),
            nullable=False,
        ),
    }
    )
    schema.validate(daily_weather_df)

    # Convert date column to datetime and create a quarter column
    daily_weather_df = NoaaTransformation.modify_date(df=daily_weather_df)

    # Convert date column to string
    daily_weather_df['date'] = daily_weather_df['date'].dt.strftime('%Y-%m-%d')
    
    # Put data in S3
    s3.put_data(data=daily_weather_df, folder='full_program/transformation/daily_weather/', object_key=f'daily_weather_{formatted_date}')







