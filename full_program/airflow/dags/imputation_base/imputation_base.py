# Import modules
from datetime import datetime
from dags.utils.config import Config
from dags.utils.aws import S3, S3Metadata
from dags.extraction.noaa_api import NOAA
from dags.transformation.etl_transforms import EtlTransforms
from dags.transformation.noaa_api_transformation import NoaaTransformation

# Todays timestamp
today = datetime.now()
timestamp_str = today.strftime('%Y%m%d%H%M%S')

# Instantiate classes for Config, S3, S3Metadata and NOAA
config = Config()
s3 = S3(config=config)
s3_metadata = S3Metadata(config=config)
noaa = NOAA(config=config, s3=s3, s3_metadata=s3_metadata)

# Extract weather data from NOAA API
parameters = {'datasetid': 'GHCND',
'datatypeid': ['TMIN', 'TMAX','TAVG', 'SNOW', 'AWND'],
'stationid': ['GHCND:USW00023174', 'GHCND:USW00023188',
'GHCND:USW00023234', 'GHCND:USW00023232', 'GHCND:USW00012839',
'GHCND:USW00012842', 'GHCND:USW00012815', 'GHCND:USW00013889',
'GHCND:USW00094846', 'GHCND:USW00013994', 'GHCND:USW00012916',
'GHCND:USW00013970', 'GHCND:USW00013957', 'GHCND:USW00094847',
'GHCND:USW00094860', 'GHCND:USW00014734', 'GHCND:USW00014733',
'GHCND:USW00014820', 'GHCND:USW00014821', 'GHCND:USW00093814',
'GHCND:USW00013739', 'GHCND:USW00094823', 'GHCND:USW00012960',
'GHCND:USW00013960', 'GHCND:USW00012921', 'GHCND:USW00013904'],
'units': 'metric',
'limit': 1000}

noaa.extract(parameters=parameters, put_object_s3_key=f'full_program/extraction/imputation/daily_weather_imputation_base_{timestamp_str}.json',
metadata_s3_key='full_program/metadata/metadata.json', dataset_key='daily_weather_imputation_base', 
start_date_if_none='1999-01-04', extract_timestamp=timestamp_str)

# Create dataframe to be used for imputation of missing weather variables as part of ETL process
daily_weather_imputation_json = s3.get_data(s3_key=f'full_program/extraction/imputation/daily_weather_imputation_base_{timestamp_str}.json')
daily_weather_imputation_df = EtlTransforms.json_to_df(data=daily_weather_imputation_json, date_as_index=False)
daily_weather_imputation_df = NoaaTransformation.modify_date(df=daily_weather_imputation_df)
daily_weather_imputation_df = NoaaTransformation.imputation_df(df=daily_weather_imputation_df)

# Put data in S3 and update metadata
s3.put_data(data=daily_weather_imputation_df, s3_key=f'full_program/transformation/daily_weather_imputation_base_{timestamp_str}.json')
s3_metadata.update_metadata(s3_key='full_program/metadata/metadata.json', dataset_key='daily_weather_imputation_base', latest_transformed_file_path=f'full_program/transformation/daily_weather_imputation_base_{timestamp_str}.json')









