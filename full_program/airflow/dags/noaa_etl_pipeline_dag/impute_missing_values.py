# Import modules
from datetime import datetime
from dags.extraction.noaa_api import *
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.noaa_api_transformation import NoaaTransformation
import logging

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def impute_missing_values():
    ''' Impute missing values in extracted NOAA weather data '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest transformed and imputed weather datasets from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path_daily_weather = metadata.get('daily_weather', {}).get('latest_transformed_file_path')
    daily_weather_json = s3.get_data(s3_key=latest_transformed_file_path_daily_weather)
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)
    latest_transformed_file_path_daily_weather_imputation_base = metadata.get('daily_weather_imputation_base', {}).get('latest_transformed_file_path')
    daily_weather_imputation_base_json = s3.get_data(s3_key=latest_transformed_file_path_daily_weather_imputation_base)
    daily_weather_imputation_base_df = EtlTransforms.json_to_df(data=daily_weather_imputation_base_json, date_as_index=False)

    # Impute missing weather variables for each day where data is missing
    daily_weather_df = NoaaTransformation.impute_missing_weather_variables(df=daily_weather_df, imputation_df=daily_weather_imputation_base_df)

    # Log imputation for missing weather variables has been successful
    logger.log('Successfully imputed missing weather variables')
    
    # Put data in S3
    s3.put_data(data=daily_weather_df, s3_key=latest_transformed_file_path_daily_weather)