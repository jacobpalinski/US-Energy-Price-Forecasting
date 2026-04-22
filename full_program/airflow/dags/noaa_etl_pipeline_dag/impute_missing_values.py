''' Import modules '''
from datetime import datetime
from dags.extraction.noaa_api import *
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.noaa_api_transformation import NoaaTransformation

def impute_missing_values():
    ''' Impute missing values in extracted NOAA weather data '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest transformed filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('daily_weather', {}).get('latest_transformed_file_path')

    # Retrieve transformed data and imputed data from S3 folder
    daily_weather_json = s3.get_data(s3_key=latest_transformed_file_path)
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)
    imputed_json = s3.get_data(s3_key='full_program/transformation/imputation/daily_weather_etl_imputation_base_20241108')
    imputed_df = EtlTransforms.json_to_df(data=imputed_json, date_as_index=False)

    # Impute missing weather variables
    daily_weather_df = NoaaTransformation.impute_missing_weather_variables(df=daily_weather_df, imputation_df=imputed_df)
    
    # Put data in S3
    s3.put_data(data=daily_weather_df, s3_key=latest_transformed_file_path)