''' Import modules '''
from datetime import datetime
from dags.extraction.noaa_api import *
from dags.transformation.etl_transforms import EtlTransforms

def pivot_data():
    ''' Pivot data from extracted NOAA weather data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data from S3 folder
    daily_weather_json = s3.get_data(folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)

    # Pivot columns
    daily_weather_df = EtlTransforms.pivot_columns(df=daily_weather_df, index=['date', 'station', 'city', 'state', 'quarter'],
    column='datatype', value='value')
    
    # Put data in S3
    s3.put_data(data=daily_weather_df, folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')