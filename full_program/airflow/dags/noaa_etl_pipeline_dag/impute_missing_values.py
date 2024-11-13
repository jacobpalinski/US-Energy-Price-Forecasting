''' Import modules '''
from datetime import datetime
from extraction.noaa_api import *
from transformation.etl_transforms import EtlTransforms
from transformation.noaa_api_transformation import NoaaTransformation

def impute_missing_values():
    ''' Impute missing values in extracted NOAA weather data '''
    # Todays date
    today = datetime.now()
    formatted_date = today.strftime('%Y%m%d')

    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve extracted data and imputed data from S3 folder
    daily_weather_json = s3.get_data(folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)
    imputed_json = s3.get_data(folder='full_program/extraction/imputation/', object_key='imputation_base_20241108')
    imputed_df = EtlTransforms.json_to_df(data=imputed_json, date_as_index=False)

    # Impute missing weather variables
    daily_weather_df = NoaaTransformation.impute_missing_weather_variables(df=daily_weather_df, imputation_df=imputed_df)
    
    # Put data in S3
    s3.put_data(data=daily_weather_df, folder='full_program/extraction/daily_weather/', object_key=f'daily_weather_{formatted_date}')