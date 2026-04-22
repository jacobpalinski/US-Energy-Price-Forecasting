''' Import modules '''
from datetime import datetime
from dags.utils.config import *
from dags.utils.aws import S3
from dags.transformation.etl_transforms import EtlTransforms

def rename_columns():
    ''' Rename columns from extracted natural gas monthly variables '''
    # Instantiate classes for Config, S3
    config = Config()
    s3 = S3(config=config)

    # Retrieve latest transformed filepath from metadata in S3
    metadata = s3.get_data(s3_key='full_program/metadata/metadata.json')
    latest_transformed_file_path = metadata.get('daily_weather', {}).get('latest_transformed_file_path')

    # Retrieve transformed data and imputed data from S3 folder
    daily_weather_json = s3.get_data(s3_key=latest_transformed_file_path)
    daily_weather_df = EtlTransforms.json_to_df(data=daily_weather_json, date_as_index=False)

    # Rename pivoted columns
    daily_weather_df = EtlTransforms.rename_columns(df=daily_weather_df, renamed_columns={'AWND': 'awnd', 'TMIN': 'tmin', 'TMAX': 'tmax', 
    'TAVG': 'tavg', 'SNOW':'snow'})
    
    # Put data in S3
    s3.put_data(data=daily_weather_df, s3_key=latest_transformed_file_path)